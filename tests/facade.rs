/*
Tests for the hand-written facade.

The generated core is covered by codegen; these exercise only the ergonomics
injected on top of it: DAG validation that must fail before any round-trip,
the create-then-fetch pairing, bearer auth, and the polling loop that turns a
sequence of responses into a terminal record or a timeout. The transport is a
local wiremock server, so nothing here needs a live QueueFlow.
*/

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use queueflow::{FacadeError, QueueFlow, WorkflowBuilder};
use serde_json::{json, Value};
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

fn job_json(id: &str, status: &str) -> Value {
    json!({
        "id": id,
        "task_name": "echo",
        "queue_name": "default",
        "status": status,
        "retry_count": 0,
        "created_at": "2026-01-01T00:00:00Z",
        "scheduled_at": "2026-01-01T00:00:00Z",
        "config": {
            "max_retries": 3,
            "priority": 0,
            "retry_delay_secs": 1,
            "retry_max_delay_secs": 60,
            "timeout_secs": 30
        }
    })
}

fn workflow_json(id: &str, status: &str) -> Value {
    json!({
        "id": id,
        "name": "etl",
        "status": status,
        "steps": [],
        "created_at": "2026-01-01T00:00:00Z"
    })
}

/// Responds with `before` for the first `flips_after` requests, then `after`.
struct JobStatusSequence {
    calls: AtomicUsize,
    flips_after: usize,
    before: &'static str,
    after: &'static str,
}

impl Respond for JobStatusSequence {
    fn respond(&self, _req: &Request) -> ResponseTemplate {
        let n = self.calls.fetch_add(1, Ordering::SeqCst);
        let status = if n < self.flips_after {
            self.before
        } else {
            self.after
        };
        ResponseTemplate::new(200).set_body_json(job_json("j1", status))
    }
}

#[test]
fn workflow_builder_rejects_invalid_dags_before_any_round_trip() {
    let err = |b: &WorkflowBuilder| match b.build() {
        Err(FacadeError::InvalidWorkflow(msg)) => msg,
        other => panic!("expected InvalidWorkflow, got {:?}", other.map(|_| "Ok")),
    };

    assert!(err(&WorkflowBuilder::new("")).contains("name is required"));
    assert!(err(&WorkflowBuilder::new("empty")).contains("has no steps"));

    let dup = WorkflowBuilder::new("dup")
        .step("a", "t", &[])
        .step("a", "t", &[]);
    assert!(err(&dup).contains("duplicate step name"));

    let dangling = WorkflowBuilder::new("dangling").step("a", "t", &["ghost"]);
    assert!(err(&dangling).contains("unknown step"));

    let cycle = WorkflowBuilder::new("cycle")
        .step("a", "t", &["b"])
        .step("b", "t", &["a"]);
    assert!(err(&cycle).contains("dependency cycle"));
}

#[test]
fn workflow_builder_accepts_a_diamond_dag() {
    let body = WorkflowBuilder::new("diamond")
        .step("root", "t", &[])
        .step("left", "t", &["root"])
        .step("right", "t", &["root"])
        .step("join", "t", &["left", "right"])
        .build()
        .expect("diamond DAG should validate");
    assert_eq!(body.name, "diamond");
    assert_eq!(body.steps.len(), 4);
    assert_eq!(
        body.steps[3].depends_on.as_deref(),
        Some(["left".to_owned(), "right".to_owned()].as_slice())
    );
}

#[tokio::test]
async fn create_job_enqueues_then_fetches_with_bearer_auth() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/jobs"))
        .and(header("authorization", "Bearer test-token"))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({"job_id": "j1"})))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v1/jobs/j1"))
        .and(header("authorization", "Bearer test-token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(job_json("j1", "pending")))
        .expect(1)
        .mount(&server)
        .await;

    // The trailing slash must be stripped, or paths would double up.
    let qf = QueueFlow::new(&format!("{}/", server.uri()), "test-token");
    let job = qf.create_job("echo", None).await.expect("create_job");
    assert_eq!(job.id, "j1");
    assert_eq!(job.task_name, "echo");
}

#[tokio::test]
async fn wait_for_job_polls_until_a_terminal_status() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/jobs/j1"))
        .respond_with(JobStatusSequence {
            calls: AtomicUsize::new(0),
            flips_after: 2,
            before: "running",
            after: "completed",
        })
        .mount(&server)
        .await;

    let qf = QueueFlow::new(&server.uri(), "test-token");
    let job = qf
        .wait_for_job("j1", Duration::from_secs(5), Duration::from_millis(10))
        .await
        .expect("wait_for_job");
    assert_eq!(job.status, queueflow::models::JobStatus::Completed);
}

#[tokio::test]
async fn wait_for_job_returns_failed_rather_than_waiting_it_out() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/jobs/j1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(job_json("j1", "failed")))
        .expect(1)
        .mount(&server)
        .await;

    let qf = QueueFlow::new(&server.uri(), "test-token");
    let job = qf
        .wait_for_job("j1", Duration::from_secs(5), Duration::from_secs(5))
        .await
        .expect("failed is terminal");
    assert_eq!(job.status, queueflow::models::JobStatus::Failed);
}

#[tokio::test]
async fn wait_for_job_times_out_while_still_running() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/jobs/j1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(job_json("j1", "running")))
        .mount(&server)
        .await;

    let qf = QueueFlow::new(&server.uri(), "test-token");
    let err = qf
        .wait_for_job("j1", Duration::from_millis(50), Duration::from_millis(10))
        .await
        .expect_err("must time out");
    assert!(matches!(err, FacadeError::Timeout(_)), "got {:?}", err);
}

#[tokio::test]
async fn create_workflow_validates_locally_then_creates_and_fetches() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/workflows"))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({"workflow_id": "w1"})))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v1/workflows/w1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(workflow_json("w1", "created")))
        .expect(1)
        .mount(&server)
        .await;

    let qf = QueueFlow::new(&server.uri(), "test-token");
    let ok = WorkflowBuilder::new("etl")
        .step("extract", "t", &[])
        .step("load", "t", &["extract"]);
    let wf = qf.create_workflow(&ok).await.expect("create_workflow");
    assert_eq!(wf.id, "w1");

    // An invalid builder must fail locally: the server saw exactly one POST.
    let bad = WorkflowBuilder::new("bad").step("a", "t", &["a"]);
    let err = qf.create_workflow(&bad).await.expect_err("cycle must fail");
    assert!(matches!(err, FacadeError::InvalidWorkflow(_)), "got {:?}", err);
}

#[tokio::test]
async fn wait_for_workflow_treats_partially_failed_as_terminal() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/workflows/w1"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(workflow_json("w1", "partially_failed")),
        )
        .expect(1)
        .mount(&server)
        .await;

    let qf = QueueFlow::new(&server.uri(), "test-token");
    let wf = qf
        .wait_for_workflow("w1", Duration::from_secs(5), Duration::from_secs(5))
        .await
        .expect("partially_failed is terminal");
    assert_eq!(
        wf.status,
        queueflow::models::WorkflowStatus::PartiallyFailed
    );
}

#[tokio::test]
async fn create_cron_sends_name_and_expression_in_the_right_fields() {
    // Regression: name and cron_expr are both strings and were once swapped
    // by the positional constructor, sending the human name as the
    // expression (which the server rejects as invalid cron syntax).
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/cron"))
        .and(wiremock::matchers::body_partial_json(json!({
            "name": "nightly-report",
            "cron_expr": "0 3 * * *",
            "task_name": "report"
        })))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({ "cron_id": "c1" })))
        .expect(1)
        .mount(&server)
        .await;

    let qf = QueueFlow::new(&server.uri(), "tok");
    let id = qf
        .create_cron("nightly-report", "0 3 * * *", "report")
        .await
        .expect("create_cron should succeed with correctly-mapped fields");
    assert_eq!(id, "c1");
}
