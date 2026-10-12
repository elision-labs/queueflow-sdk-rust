/*
Tests for the hand-written facade.

The generated core is covered by codegen; these exercise only the ergonomics
injected on top of it: DAG validation that must fail before any round-trip,
request shaping (options, headers, filters), bearer auth per credential, the
polling loop that turns a sequence of responses into a terminal record or a
timeout, typed errors, the timeout split between regular and lease calls, and
the worker runtime (lease, heartbeat, complete, fail, lost lease, stop and
drain). The transport is a local wiremock server, so nothing here needs a
live QueueFlow.
*/

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use queueflow_sdk::models::{
    BackoffStrategy, ConditionOp, FanOut, JobConfig, JobStatus, OnFailure, OnSuccess,
    StepCondition, StepStatus, WorkflowStatus,
};
use queueflow_sdk::{
    CancelJobsFilter, CreateCronOptions, CreateJobOptions, FacadeError, HandlerError, Handlers,
    Json, LeaseOptions, ListOptions, QueueFlow, QueueFlowOptions, ReplayDeadLettersFilter,
    StepOptions, WaitOptions, WorkerOptions, WorkflowBuilder,
};
use serde_json::{json, Value};
use wiremock::matchers::{header, method, path, path_regex};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

fn job_json(id: &str, status: &str) -> Value {
    job_json_task(id, "echo", status)
}

fn job_json_task(id: &str, task: &str, status: &str) -> Value {
    json!({
        "id": id,
        "task_name": task,
        "queue_name": "default",
        "status": status,
        "payload": {"n": 1},
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

fn obj(v: Value) -> Json {
    v.as_object()
        .expect("object")
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
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

/// Requests the server saw whose path ends with `suffix`, in arrival order.
async fn requests_to(server: &MockServer, suffix: &str) -> Vec<Request> {
    server
        .received_requests()
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(|r| r.url.path().ends_with(suffix))
        .collect()
}

fn body_json(req: &Request) -> Value {
    serde_json::from_slice(&req.body).unwrap_or(Value::Null)
}

// --- WorkflowBuilder -------------------------------------------------------

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
    // A step without dependencies must not carry an empty list.
    assert_eq!(body.steps[0].depends_on, None);
}

#[test]
fn workflow_builder_step_options_and_context_reach_the_wire_shape() {
    let body = WorkflowBuilder::new("etl")
        .step_with_options(
            "extract",
            "fetch",
            StepOptions {
                payload: Some(obj(json!({"n": 1}))),
                ..StepOptions::default()
            },
        )
        .step_with_options(
            "load",
            "upsert",
            StepOptions {
                after: vec!["extract".to_owned()],
                config: Some(JobConfig {
                    max_retries: Some(5),
                    ..JobConfig::new()
                }),
                on_failure: Some(OnFailure::Continue),
                on_success: Some(OnSuccess::Continue),
                metadata: Some(obj(json!({"owner": "data"}))),
                ..StepOptions::default()
            },
        )
        .context(obj(json!({"a": 1})))
        .context(obj(json!({"b": 2})))
        .metadata(obj(json!({"source": "test"})))
        .build()
        .expect("build");
    let wire = serde_json::to_value(&body).unwrap();
    let steps = wire["steps"].as_array().unwrap();
    assert_eq!(steps[0]["payload"]["n"], 1);
    assert!(steps[0].get("on_failure").is_none(), "unset policy must be omitted");
    assert_eq!(steps[1]["on_failure"], "continue");
    assert_eq!(steps[1]["on_success"], "continue");
    assert_eq!(steps[1]["config"]["max_retries"], 5);
    assert_eq!(steps[1]["metadata"]["owner"], "data");
    assert_eq!(steps[1]["depends_on"], json!(["extract"]));
    assert_eq!(wire["context"], json!({"a": 1, "b": 2}));
    assert_eq!(wire["metadata"], json!({"source": "test"}));
}


/// A leaf condition in the generated model's shape.
fn leaf(path: &str, op: Option<ConditionOp>, value: Option<Value>) -> StepCondition {
    StepCondition {
        path: Some(Some(path.to_owned())),
        op: op.map(Some),
        value: value.map(Some),
        ..StepCondition::new()
    }
}

#[test]
fn workflow_builder_v2_step_fields_reach_the_wire_shape() {
    let child = WorkflowBuilder::new("fulfilment")
        .step("pick", "pick_items", &[])
        .step("pack", "pack_parcel", &["pick"])
        .timeout_secs(1800)
        .build()
        .expect("child build");
    let body = WorkflowBuilder::new("order")
        .step_with_options(
            "validate",
            "validate_order",
            StepOptions {
                queue: Some("orders".to_owned()),
                ..StepOptions::default()
            },
        )
        .step_with_options(
            "review",
            "queue_for_review",
            StepOptions {
                after: vec!["validate".to_owned()],
                when: Some(StepCondition {
                    any: Some(Some(vec![
                        leaf("validate.score", Some(ConditionOp::Gt), Some(json!(0.8))),
                        StepCondition {
                            not: Some(Some(Box::new(leaf(
                                "validate.trusted",
                                None,
                                Some(json!(true)),
                            )))),
                            ..StepCondition::new()
                        },
                    ])),
                    ..StepCondition::new()
                }),
                ..StepOptions::default()
            },
        )
        .step_with_options(
            "resize",
            "resize_image",
            StepOptions {
                after: vec!["validate".to_owned()],
                fan_out: Some(FanOut {
                    item_key: Some(Some("image".to_owned())),
                    max_items: Some(Some(50)),
                    path: "validate.images".to_owned(),
                }),
                on_failure: Some(OnFailure::Continue),
                ..StepOptions::default()
            },
        )
        .sub_workflow(
            "fulfil",
            child,
            StepOptions {
                after: vec!["review".to_owned(), "resize".to_owned()],
                payload: Some(obj(json!({"warehouse": "eu-1"}))),
                ..StepOptions::default()
            },
        )
        .timeout_secs(3600)
        .build()
        .expect("build");
    let wire = serde_json::to_value(&body).unwrap();
    assert_eq!(wire["timeout_secs"], 3600);
    let steps = wire["steps"].as_array().unwrap();
    assert_eq!(steps.len(), 4);

    assert_eq!(steps[0]["task_name"], "validate_order");
    assert_eq!(steps[0]["queue"], "orders");
    for key in ["when", "fan_out", "workflow"] {
        assert!(steps[0].get(key).is_none(), "unset {} must be omitted", key);
    }
    assert!(steps[1].get("queue").is_none(), "unset queue must be omitted");

    assert_eq!(
        steps[1]["when"],
        json!({"any": [
            {"path": "validate.score", "op": "gt", "value": 0.8},
            {"not": {"path": "validate.trusted", "value": true}}
        ]})
    );

    assert_eq!(
        steps[2]["fan_out"],
        json!({"path": "validate.images", "item_key": "image", "max_items": 50})
    );
    assert_eq!(steps[2]["on_failure"], "continue");

    assert!(
        steps[3].get("task_name").is_none(),
        "a sub-workflow step carries no task_name"
    );
    assert_eq!(steps[3]["depends_on"], json!(["review", "resize"]));
    assert_eq!(steps[3]["payload"], json!({"warehouse": "eu-1"}));
    assert_eq!(steps[3]["workflow"]["name"], "fulfilment");
    assert_eq!(steps[3]["workflow"]["timeout_secs"], 1800);
    assert_eq!(steps[3]["workflow"]["steps"][1]["depends_on"], json!(["pick"]));
}

#[test]
fn workflow_builder_rejects_invalid_v2_definitions_before_any_round_trip() {
    let err = |b: &WorkflowBuilder| match b.build() {
        Err(FacadeError::InvalidWorkflow(msg)) => msg,
        other => panic!("expected InvalidWorkflow, got {:?}", other.map(|_| "Ok")),
    };
    let gated = |cond: StepCondition| {
        WorkflowBuilder::new("w").step_with_options(
            "a",
            "t",
            StepOptions {
                when: Some(cond),
                ..StepOptions::default()
            },
        )
    };

    assert!(err(&WorkflowBuilder::new("w").step("a", "t", &[]).timeout_secs(0))
        .contains("timeout_secs must be within 1..=2592000"));
    assert!(
        err(&WorkflowBuilder::new("w").step("a", "t", &[]).timeout_secs(2_592_001))
            .contains("timeout_secs"),
    );

    // Conditions: shape and operator/value consistency.
    let msg = err(&gated(leaf("x", Some(ConditionOp::Gt), None)));
    assert!(msg.contains("step \"a\": when: op gt needs a value"), "{}", msg);
    assert!(err(&gated(leaf("x", Some(ConditionOp::Exists), Some(json!(1)))))
        .contains("op exists does not take a value"));
    assert!(err(&gated(leaf("x", Some(ConditionOp::In), Some(json!("not-an-array")))))
        .contains("op in needs an array value"));
    assert!(err(&gated(leaf("", None, Some(json!(1))))).contains("path must not be empty"));
    assert!(err(&gated(StepCondition::new())).contains("exactly one of path, all, any or not"));
    assert!(err(&gated(StepCondition {
        all: Some(Some(vec![])),
        ..StepCondition::new()
    }))
    .contains("at least one member"));
    assert!(err(&gated(StepCondition {
        all: Some(Some(vec![leaf("x", None, None)])),
        op: Some(Some(ConditionOp::Eq)),
        ..StepCondition::new()
    }))
    .contains("op and value are only valid with path"));
    // A nested member is validated too.
    assert!(err(&gated(StepCondition {
        not: Some(Some(Box::new(leaf("x", Some(ConditionOp::Lt), None)))),
        ..StepCondition::new()
    }))
    .contains("op lt needs a value"));
    // Defaults: a value alone means eq, no value means exists.
    gated(leaf("x", None, Some(json!(1)))).build().expect("eq by default");
    gated(leaf("x", None, None)).build().expect("exists by default");

    // Fan-out.
    let fan = |f: FanOut| {
        WorkflowBuilder::new("w").step_with_options(
            "a",
            "t",
            StepOptions {
                fan_out: Some(f),
                ..StepOptions::default()
            },
        )
    };
    assert!(err(&fan(FanOut::new(" ".to_owned()))).contains("fan_out.path is required"));
    assert!(err(&fan(FanOut {
        max_items: Some(Some(0)),
        ..FanOut::new("items".to_owned())
    }))
    .contains("fan_out.max_items must be within 1..=10000"));
    assert!(err(&fan(FanOut {
        item_key: Some(Some("_context".to_owned())),
        ..FanOut::new("items".to_owned())
    }))
    .contains("fan_out.item_key"));

    // Sub-workflows: not combinable with a fan-out, validated recursively,
    // and bounded in depth.
    let child = WorkflowBuilder::new("c").step("x", "t", &[]).build().unwrap();
    let both = WorkflowBuilder::new("w").sub_workflow(
        "s",
        child.clone(),
        StepOptions {
            fan_out: Some(FanOut::new("items".to_owned())),
            ..StepOptions::default()
        },
    );
    assert!(err(&both).contains("either a sub-workflow or a fan-out"));
    let mut bad_child = child.clone();
    bad_child.steps[0].depends_on = Some(vec!["x".to_owned()]);
    let msg = err(&WorkflowBuilder::new("w").sub_workflow("s", bad_child, StepOptions::default()));
    assert!(msg.contains("step \"s\": sub-workflow: dependency cycle"), "{}", msg);
    let mut nested = child;
    // Eight levels below the root are allowed; the ninth is not.
    for level in 0..8 {
        nested = WorkflowBuilder::new(&format!("level{}", level))
            .sub_workflow("s", nested, StepOptions::default())
            .build()
            .expect("intermediate levels build on their own");
    }
    let too_deep = WorkflowBuilder::new("root").sub_workflow("s", nested, StepOptions::default());
    assert!(err(&too_deep).contains("nest deeper than 8"));
}

// --- Client construction ---------------------------------------------------

#[test]
fn with_options_validates_and_applies_defaults() {
    assert!(matches!(
        QueueFlow::with_options(QueueFlowOptions::new("", "t")),
        Err(FacadeError::InvalidOptions(_))
    ));
    assert!(matches!(
        QueueFlow::with_options(QueueFlowOptions::new("http://x", "")),
        Err(FacadeError::InvalidOptions(_))
    ));
    let qf = QueueFlow::with_options(
        QueueFlowOptions::new("http://x/", "t").timeout(Duration::from_secs(5)),
    )
    .unwrap();
    assert_eq!(qf.timeout(), Duration::from_secs(5));
    assert_eq!(qf.config.base_path, "http://x");
    assert_eq!(
        QueueFlow::new("http://x", "t").timeout(),
        queueflow_sdk::DEFAULT_TIMEOUT
    );
}

#[tokio::test]
async fn worker_routes_use_the_worker_token() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/jobs/j1"))
        .and(header("authorization", "Bearer tenant"))
        .respond_with(ResponseTemplate::new(200).set_body_json(job_json("j1", "pending")))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v1/queues/q/lease"))
        .and(header("authorization", "Bearer worker"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"jobs": []})))
        .expect(1)
        .mount(&server)
        .await;

    let qf = QueueFlow::with_options(
        QueueFlowOptions::new(&format!("{}/", server.uri()), "tenant").worker_token("worker"),
    )
    .unwrap();
    qf.get_job("j1").await.expect("get_job");
    let leased = qf
        .lease_jobs("q", LeaseOptions::default())
        .await
        .expect("lease_jobs");
    assert!(leased.is_empty());
}

// --- Jobs ------------------------------------------------------------------

#[tokio::test]
async fn create_job_posts_with_bearer_auth_and_returns_the_id() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/jobs"))
        .and(header("authorization", "Bearer test-token"))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({"job_id": "j1"})))
        .expect(1)
        .mount(&server)
        .await;

    // The trailing slash must be stripped, or paths would double up.
    let qf = QueueFlow::new(&format!("{}/", server.uri()), "test-token");
    let id = qf
        .create_job("echo", Some(obj(json!({"hi": 1}))))
        .await
        .expect("create_job");
    assert_eq!(id, "j1");

    let posted = body_json(&requests_to(&server, "/api/v1/jobs").await[0]);
    assert_eq!(posted["task_name"], "echo");
    assert_eq!(posted["payload"]["hi"], 1);
    assert!(posted.get("config").is_none(), "config must be omitted when unset");
    assert!(posted.get("run_at").is_none(), "run_at must be omitted when unset");
}

#[tokio::test]
async fn create_job_with_options_sends_every_field_and_the_idempotency_header() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/jobs"))
        .and(header("Idempotency-Key", "order-1"))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({"job_id": "j1"})))
        .expect(1)
        .mount(&server)
        .await;

    let qf = QueueFlow::new(&server.uri(), "t");
    qf.create_job_with_options(
        "echo",
        CreateJobOptions {
            payload: Some(obj(json!({"hi": 1}))),
            queue: Some("billing".to_owned()),
            priority: Some(7),
            max_retries: Some(2),
            timeout_secs: Some(45),
            retry_backoff: Some(BackoffStrategy::Linear),
            retry_delay_secs: Some(3),
            retry_max_delay_secs: Some(90),
            jitter_factor: Some(0.25),
            idempotency_key: Some("order-1".to_owned()),
            run_at: Some("2030-01-02T03:04:05Z".to_owned()),
        },
    )
    .await
    .expect("create_job_with_options");

    let posted = body_json(&requests_to(&server, "/api/v1/jobs").await[0]);
    assert_eq!(
        posted["config"],
        json!({
            "queue": "billing", "priority": 7, "max_retries": 2, "timeout": 45,
            "retry_backoff": "linear", "retry_delay_secs": 3, "retry_max_delay_secs": 90,
            "jitter_factor": 0.25
        })
    );
    assert_eq!(posted["run_at"], "2030-01-02T03:04:05Z");
}

#[tokio::test]
async fn non_2xx_responses_become_http_errors_with_status_and_body() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/jobs/missing"))
        .respond_with(ResponseTemplate::new(404).set_body_json(json!({"error": "no such job"})))
        .mount(&server)
        .await;

    let qf = QueueFlow::new(&server.uri(), "t");
    let err = qf.get_job("missing").await.expect_err("404 must fail");
    assert_eq!(err.status(), Some(404));
    match &err {
        FacadeError::Http { op, status, body } => {
            assert_eq!(*op, "get_job");
            assert_eq!(*status, 404);
            assert!(body.contains("no such job"), "body = {:?}", body);
        }
        other => panic!("expected Http, got {:?}", other),
    }
    let text = err.to_string();
    assert!(text.contains("404") && text.contains("no such job"), "{}", text);
}

#[tokio::test]
async fn lease_call_outlives_the_request_timeout() {
    let server = MockServer::start().await;
    let slow = Duration::from_millis(150);
    Mock::given(method("GET"))
        .and(path("/api/v1/jobs/j1"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(job_json("j1", "pending"))
                .set_delay(slow),
        )
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v1/queues/q/lease"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"jobs": []}))
                .set_delay(slow),
        )
        .mount(&server)
        .await;

    let qf = QueueFlow::with_options(
        QueueFlowOptions::new(&server.uri(), "t").timeout(Duration::from_millis(50)),
    )
    .unwrap();
    let err = qf.get_job("j1").await.expect_err("50ms timeout must hit");
    assert!(matches!(err, FacadeError::Transport { .. }), "got {:?}", err);
    assert_eq!(err.status(), None);
    qf.lease_jobs(
        "q",
        LeaseOptions {
            wait_secs: Some(1),
            ..LeaseOptions::default()
        },
    )
    .await
    .expect("the lease call is bound by wait_secs + 35s, not the request timeout");
}

#[tokio::test]
async fn list_jobs_forwards_set_filters_and_omits_unset_ones() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/jobs"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobs": [], "limit": 50, "offset": 0, "has_more": false
        })))
        .mount(&server)
        .await;

    let qf = QueueFlow::new(&server.uri(), "t");
    qf.list_jobs(&ListOptions {
        status: Some("failed".to_owned()),
        queue: Some("billing".to_owned()),
        limit: Some(50),
        cursor: Some("abc".to_owned()),
        created_after: Some("2026-01-01T00:00:00Z".to_owned()),
        ..ListOptions::default()
    })
    .await
    .expect("list_jobs");

    let req = &requests_to(&server, "/api/v1/jobs").await[0];
    let query: HashMap<String, String> = req
        .url
        .query_pairs()
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    assert_eq!(query.get("status").map(String::as_str), Some("failed"));
    assert_eq!(query.get("queue").map(String::as_str), Some("billing"));
    assert_eq!(query.get("limit").map(String::as_str), Some("50"));
    assert_eq!(query.get("cursor").map(String::as_str), Some("abc"));
    assert_eq!(
        query.get("created_after").map(String::as_str),
        Some("2026-01-01T00:00:00Z")
    );
    for absent in ["offset", "order_by", "include_total", "created_before", "task_name", "replayed"] {
        assert!(!query.contains_key(absent), "{} must be omitted", absent);
    }
}

fn query_of(req: &Request) -> HashMap<String, String> {
    req.url
        .query_pairs()
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect()
}

#[tokio::test]
async fn list_filters_forward_task_name_and_replayed() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/jobs"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobs": [], "limit": 50, "offset": 0, "has_more": false
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v1/dlq"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "dead_letters": [], "limit": 50, "offset": 0, "has_more": false
        })))
        .mount(&server)
        .await;

    let qf = QueueFlow::new(&server.uri(), "t");
    qf.list_jobs(&ListOptions {
        task_name: Some("send-email".to_owned()),
        ..ListOptions::default()
    })
    .await
    .expect("list_jobs");
    let jobs_query = query_of(&requests_to(&server, "/api/v1/jobs").await[0]);
    assert_eq!(jobs_query.get("task_name").map(String::as_str), Some("send-email"));
    assert!(!jobs_query.contains_key("replayed"));

    qf.list_dead_letters(&ListOptions {
        queue: Some("billing".to_owned()),
        task_name: Some("charge".to_owned()),
        replayed: Some(false),
        ..ListOptions::default()
    })
    .await
    .expect("list_dead_letters");
    let dlq_query = query_of(&requests_to(&server, "/api/v1/dlq").await[0]);
    assert_eq!(dlq_query.get("queue").map(String::as_str), Some("billing"));
    assert_eq!(dlq_query.get("task_name").map(String::as_str), Some("charge"));
    assert_eq!(dlq_query.get("replayed").map(String::as_str), Some("false"));
}

#[tokio::test]
async fn cancel_jobs_posts_the_filter_and_returns_the_count() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/jobs/cancel"))
        .and(header("authorization", "Bearer t"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"cancelled": 2})))
        .expect(2)
        .mount(&server)
        .await;

    let qf = QueueFlow::new(&server.uri(), "t");
    let cancelled = qf
        .cancel_jobs(&CancelJobsFilter {
            queue: Some("billing".to_owned()),
            status: Some("pending".to_owned()),
            limit: Some(2),
            created_before: Some("2026-02-01T00:00:00Z".to_owned()),
            ..CancelJobsFilter::default()
        })
        .await
        .expect("cancel_jobs");
    assert_eq!(cancelled, 2);
    // An empty filter is an empty JSON object: unset fields are not sent as
    // nulls, so the server defaults apply.
    qf.cancel_jobs(&CancelJobsFilter::default())
        .await
        .expect("cancel_jobs (empty filter)");

    let posts = requests_to(&server, "/api/v1/jobs/cancel").await;
    assert_eq!(
        body_json(&posts[0]),
        json!({
            "queue": "billing", "status": "pending", "limit": 2,
            "created_before": "2026-02-01T00:00:00Z"
        })
    );
    assert_eq!(body_json(&posts[1]), json!({}));
}

#[tokio::test]
async fn cancel_jobs_with_a_terminal_status_is_a_400() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/jobs/cancel"))
        .respond_with(
            ResponseTemplate::new(400)
                .set_body_json(json!({"error": "status completed is terminal"})),
        )
        .mount(&server)
        .await;

    let qf = QueueFlow::new(&server.uri(), "t");
    let err = qf
        .cancel_jobs(&CancelJobsFilter {
            status: Some("completed".to_owned()),
            ..CancelJobsFilter::default()
        })
        .await
        .expect_err("400 must fail");
    match err {
        FacadeError::Http { op, status, body } => {
            assert_eq!(op, "cancel_jobs");
            assert_eq!(status, 400);
            assert!(body.contains("terminal"), "body = {:?}", body);
        }
        other => panic!("expected Http, got {:?}", other),
    }
}

#[tokio::test]
async fn queues_and_tasks_unwrap_their_envelopes() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/queues"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"queues": [
            {"queue": "default", "pending": 2, "scheduled": 0, "running": 1, "oldest_pending_age_secs": 12, "paused": true},
            {"queue": "legacy", "pending": 0, "scheduled": 1, "running": 0}
        ]})))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v1/tasks"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"tasks": ["echo", "sleep"]})))
        .mount(&server)
        .await;

    let qf = QueueFlow::new(&server.uri(), "t");
    let queues = qf.queues().await.expect("queues");
    assert_eq!(queues.len(), 2);
    assert_eq!(queues[0].queue, "default");
    assert_eq!(queues[0].pending, 2);
    assert_eq!(queues[0].oldest_pending_age_secs, Some(Some(12)));
    assert_eq!(queues[0].paused, Some(true));
    // A pre-0.5 server omits `paused`; the field must stay optional.
    assert_eq!(queues[1].paused, None);
    assert_eq!(qf.tasks().await.expect("tasks"), vec!["echo", "sleep"]);
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
        .wait_for_job(
            "j1",
            WaitOptions {
                timeout: Duration::from_secs(5),
                interval: Duration::from_millis(10),
            },
        )
        .await
        .expect("wait_for_job");
    assert_eq!(job.status, JobStatus::Completed);
    assert_eq!(requests_to(&server, "/api/v1/jobs/j1").await.len(), 3);
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
        .wait_for_job("j1", WaitOptions::default())
        .await
        .expect("failed is terminal");
    assert_eq!(job.status, JobStatus::Failed);
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
    let started = Instant::now();
    let err = qf
        .wait_for_job(
            "j1",
            WaitOptions {
                timeout: Duration::from_millis(50),
                interval: Duration::from_millis(10),
            },
        )
        .await
        .expect_err("must time out");
    assert!(matches!(err, FacadeError::Timeout(_)), "got {:?}", err);
    assert!(started.elapsed() < Duration::from_secs(2), "gave up late");
}

// --- Workflows -------------------------------------------------------------

#[tokio::test]
async fn create_workflow_validates_locally_then_creates() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/workflows"))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({"workflow_id": "w1"})))
        .expect(1)
        .mount(&server)
        .await;

    let qf = QueueFlow::new(&server.uri(), "test-token");
    let ok = WorkflowBuilder::new("etl")
        .step("extract", "t", &[])
        .step("load", "t", &["extract"]);
    assert_eq!(qf.create_workflow(&ok).await.expect("create_workflow"), "w1");

    // An invalid builder must fail locally: the server saw exactly one POST.
    let bad = WorkflowBuilder::new("bad").step("a", "t", &["a"]);
    let err = qf.create_workflow(&bad).await.expect_err("cycle must fail");
    assert!(matches!(err, FacadeError::InvalidWorkflow(_)), "got {:?}", err);
}

#[tokio::test]
async fn workflow_steps_unwraps_the_envelope() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/workflows/w1/steps"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"steps": [
            {"name": "a", "status": "completed", "job_id": "j1"},
            {"name": "b", "status": "pending"}
        ]})))
        .mount(&server)
        .await;
    let qf = QueueFlow::new(&server.uri(), "t");
    let steps = qf.workflow_steps("w1").await.expect("workflow_steps");
    assert_eq!(steps.len(), 2);
    assert_eq!(steps[0].status, StepStatus::Completed);
    assert_eq!(steps[1].name, "b");
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
        .wait_for_workflow("w1", WaitOptions::default())
        .await
        .expect("partially_failed is terminal");
    assert_eq!(
        wf.status,
        WorkflowStatus::PartiallyFailed
    );
}


#[tokio::test]
async fn retry_workflow_posts_to_the_retry_route_and_surfaces_409() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/workflows/w1/retry"))
        .and(header("authorization", "Bearer test-token"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v1/workflows/w2/retry"))
        .respond_with(
            ResponseTemplate::new(409)
                .set_body_json(json!({"error": "workflow w2 is running; only failed, partially_failed or cancelled workflows can be retried"})),
        )
        .expect(1)
        .mount(&server)
        .await;

    let qf = QueueFlow::new(&server.uri(), "test-token");
    qf.retry_workflow("w1").await.expect("retry_workflow");
    let err = qf.retry_workflow("w2").await.expect_err("running is a conflict");
    match err {
        FacadeError::Http { op, status, body } => {
            assert_eq!(op, "retry_workflow");
            assert_eq!(status, 409);
            assert!(body.contains("only failed"), "{}", body);
        }
        other => panic!("want Http, got {:?}", other),
    }
    assert!(requests_to(&server, "/w1/retry").await[0].body.is_empty());
}

#[tokio::test]
async fn workflow_steps_expose_errors_timings_children_and_fan_out_counts() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/workflows/w1/steps"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"steps": [
            {"name": "gate", "status": "skipped", "error": "condition not met",
             "started_at": "2026-01-01T00:00:01Z", "completed_at": "2026-01-01T00:00:01Z"},
            {"name": "resize", "status": "running", "job_id": "j1",
             "started_at": "2026-01-01T00:00:02Z",
             "fan_out": {"total": 3, "completed": 1, "failed": 0}},
            {"name": "fulfil", "status": "completed", "child_workflow_id": "w2",
             "started_at": "2026-01-01T00:00:03Z", "completed_at": "2026-01-01T00:00:09Z"},
            {"name": "late", "status": "pending"}
        ]})))
        .mount(&server)
        .await;
    let qf = QueueFlow::new(&server.uri(), "t");
    let steps = qf.workflow_steps("w1").await.expect("workflow_steps");
    assert_eq!(steps.len(), 4);

    assert_eq!(steps[0].status, StepStatus::Skipped);
    assert_eq!(steps[0].error, Some(Some("condition not met".to_owned())));
    assert_eq!(steps[0].completed_at, Some(Some("2026-01-01T00:00:01Z".to_owned())));
    assert_eq!(steps[0].job_id, None);

    let progress = steps[1].fan_out.clone().flatten().expect("fan_out progress");
    assert_eq!((progress.total, progress.completed, progress.failed), (3, 1, 0));
    assert_eq!(steps[1].job_id, Some(Some("j1".to_owned())));
    assert_eq!(steps[1].completed_at, None);

    assert_eq!(steps[2].child_workflow_id, Some(Some("w2".to_owned())));
    assert_eq!(steps[2].fan_out, None);

    assert_eq!(steps[3].started_at, None);
    assert_eq!(steps[3].error, None);
}

#[tokio::test]
async fn get_workflow_decodes_the_deadline_retry_and_parent_fields() {
    let server = MockServer::start().await;
    let mut record = workflow_json("w2", "failed");
    for (k, v) in [
        ("error_message", json!("step 'b': boom")),
        ("timeout_secs", json!(3600)),
        ("deadline_at", json!("2026-01-01T01:00:00Z")),
        ("retry_count", json!(2)),
        ("parent_workflow_id", json!("w1")),
        ("parent_step_name", json!("fulfil")),
    ] {
        record[k] = v;
    }
    Mock::given(method("GET"))
        .and(path("/api/v1/workflows/w2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(record))
        .mount(&server)
        .await;
    let qf = QueueFlow::new(&server.uri(), "t");
    let wf = qf.get_workflow("w2").await.expect("get_workflow");
    assert_eq!(wf.status, WorkflowStatus::Failed);
    assert_eq!(wf.error_message, Some(Some("step 'b': boom".to_owned())));
    assert_eq!(wf.timeout_secs, Some(Some(3600)));
    assert_eq!(wf.deadline_at, Some(Some("2026-01-01T01:00:00Z".to_owned())));
    assert_eq!(wf.retry_count, Some(2));
    assert_eq!(wf.parent_workflow_id, Some(Some("w1".to_owned())));
    assert_eq!(wf.parent_step_name, Some(Some("fulfil".to_owned())));

    // A record without them decodes too (the pre-0.6 shape).
    let bare: queueflow_sdk::models::Workflow =
        serde_json::from_value(workflow_json("w1", "running")).unwrap();
    assert_eq!(bare.retry_count, None);
    assert_eq!(bare.error_message, None);
}

// --- Cron ------------------------------------------------------------------

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

#[tokio::test]
async fn create_cron_with_options_sends_queue_and_payload() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/cron"))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({ "cron_id": "c1" })))
        .expect(1)
        .mount(&server)
        .await;

    let qf = QueueFlow::new(&server.uri(), "tok");
    qf.create_cron_with_options(
        "nightly",
        "0 3 * * *",
        "report",
        CreateCronOptions {
            payload: Some(obj(json!({"from": "cron"}))),
            queue: Some("reports".to_owned()),
            config: None,
        },
    )
    .await
    .expect("create_cron_with_options");
    let posted = body_json(&requests_to(&server, "/api/v1/cron").await[0]);
    assert_eq!(posted["queue"], "reports");
    assert_eq!(posted["payload"]["from"], "cron");
    assert!(posted.get("config").is_none());
}

// --- Dead letters ----------------------------------------------------------

#[tokio::test]
async fn replay_dead_letters_posts_the_filter_and_unwraps_results() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/dlq/replay"))
        .and(header("authorization", "Bearer t"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "replayed": 1,
            "results": [
                {"dead_letter_id": 7, "job_id": "j7"},
                {"dead_letter_id": 8, "error": "original job purged by retention"}
            ]
        })))
        .expect(1)
        .mount(&server)
        .await;

    let qf = QueueFlow::new(&server.uri(), "t");
    let results = qf
        .replay_dead_letters(&ReplayDeadLettersFilter {
            queue: Some("billing".to_owned()),
            task_name: Some("charge".to_owned()),
            limit: Some(10),
            ..ReplayDeadLettersFilter::default()
        })
        .await
        .expect("replay_dead_letters");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].dead_letter_id, 7);
    assert_eq!(results[0].job_id, Some(Some("j7".to_owned())));
    assert_eq!(results[0].error, None);
    assert_eq!(results[1].dead_letter_id, 8);
    assert_eq!(results[1].job_id, None);
    assert_eq!(
        results[1].error,
        Some(Some("original job purged by retention".to_owned()))
    );

    let posted = body_json(&requests_to(&server, "/api/v1/dlq/replay").await[0]);
    assert_eq!(
        posted,
        json!({"queue": "billing", "task_name": "charge", "limit": 10})
    );
}

// --- System ----------------------------------------------------------------

#[tokio::test]
async fn pause_and_resume_queue_post_to_the_queue_control_routes() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v1/queues/billing/pause"))
        .and(header("authorization", "Bearer t"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "queue": "billing", "paused": true, "running": 3, "pending": 12
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/v1/queues/billing/resume"))
        .and(header("authorization", "Bearer t"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "queue": "billing", "paused": false, "running": 0, "pending": 12
        })))
        .expect(1)
        .mount(&server)
        .await;

    let qf = QueueFlow::new(&server.uri(), "t");
    let paused = qf.pause_queue("billing").await.expect("pause_queue");
    assert_eq!(paused.queue, "billing");
    assert!(paused.paused);
    assert_eq!(paused.running, 3);
    assert_eq!(paused.pending, 12);

    let resumed = qf.resume_queue("billing").await.expect("resume_queue");
    assert!(!resumed.paused);
    assert_eq!(resumed.running, 0);

    let err = qf.pause_queue("other").await.expect_err("unmocked queue");
    assert_eq!(err.status(), Some(404));
    assert!(matches!(err, FacadeError::Http { op: "pause_queue", .. }), "got {:?}", err);
}

#[tokio::test]
async fn whoami_returns_the_tenant_and_access_of_the_token() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/v1/whoami"))
        .and(header("authorization", "Bearer acme:ro"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "tenant": "acme", "access": "read_only"
        })))
        .expect(1)
        .mount(&server)
        .await;

    let qf = QueueFlow::new(&server.uri(), "acme:ro");
    let me = qf.whoami().await.expect("whoami");
    assert_eq!(me.tenant, "acme");
    assert_eq!(me.access, "read_only");
}

// --- Worker runtime --------------------------------------------------------

/// A scripted worker-protocol server: hands out the queued leases (honouring
/// max_jobs), then empty responses; heartbeats answer per the configured
/// mode; complete/fail are 204 and inspected via `received_requests`.
#[derive(Clone)]
struct Broker {
    state: Arc<Mutex<BrokerState>>,
}

struct BrokerState {
    pending: Vec<Value>,
    lease_fail_n: u32,
    lease_status: Option<u16>,
    heartbeat: &'static str,
    heartbeat_409: bool,
}

impl Broker {
    fn new() -> Self {
        Broker {
            state: Arc::new(Mutex::new(BrokerState {
                pending: Vec::new(),
                lease_fail_n: 0,
                lease_status: None,
                heartbeat: "running",
                heartbeat_409: false,
            })),
        }
    }

    fn enqueue(&self, id: &str, task: &str) {
        self.state
            .lock()
            .unwrap()
            .pending
            .push(job_json_task(id, task, "running"));
    }

    async fn mount(&self, server: &MockServer) {
        Mock::given(method("POST"))
            .and(path_regex(r"^/api/v1/queues/[^/]+/lease$"))
            .respond_with(LeaseResponder(self.clone()))
            .mount(server)
            .await;
        Mock::given(method("POST"))
            .and(path_regex(r"^/api/v1/jobs/[^/]+/heartbeat$"))
            .respond_with(HeartbeatResponder(self.clone()))
            .mount(server)
            .await;
        Mock::given(method("POST"))
            .and(path_regex(r"^/api/v1/jobs/[^/]+/(complete|fail)$"))
            .respond_with(ResponseTemplate::new(204))
            .mount(server)
            .await;
    }
}

struct LeaseResponder(Broker);

impl Respond for LeaseResponder {
    fn respond(&self, req: &Request) -> ResponseTemplate {
        let mut st = self.0.state.lock().unwrap();
        if let Some(status) = st.lease_status {
            return ResponseTemplate::new(status).set_body_json(json!({"error": "nope"}));
        }
        if st.lease_fail_n > 0 {
            st.lease_fail_n -= 1;
            return ResponseTemplate::new(500).set_body_json(json!({"error": "flaky"}));
        }
        let max = body_json(req)["max_jobs"].as_u64().unwrap_or(1) as usize;
        let take = max.min(st.pending.len());
        let jobs: Vec<Value> = st
            .pending
            .drain(..take)
            .map(|job| json!({"job": job, "lease_token": "tok"}))
            .collect();
        let mut resp = ResponseTemplate::new(200).set_body_json(json!({"jobs": jobs}));
        if jobs.is_empty() {
            // Stand in for the server's long poll so an idle worker does not
            // spin against the mock.
            resp = resp.set_delay(Duration::from_millis(20));
        }
        resp
    }
}

struct HeartbeatResponder(Broker);

impl Respond for HeartbeatResponder {
    fn respond(&self, _req: &Request) -> ResponseTemplate {
        let st = self.0.state.lock().unwrap();
        if st.heartbeat_409 {
            return ResponseTemplate::new(409).set_body_json(json!({"error": "lease lost"}));
        }
        ResponseTemplate::new(200).set_body_json(json!({"status": st.heartbeat}))
    }
}

fn job_id(req: &Request) -> String {
    let segs: Vec<&str> = req.url.path().split('/').collect();
    segs[segs.len() - 2].to_owned()
}

/// Polls `cond` for up to five seconds.
async fn wait_until<F, Fut>(what: &str, mut cond: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if cond().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("timed out waiting for {}", what);
}

async fn worker_client(server: &MockServer) -> QueueFlow {
    QueueFlow::with_options(
        QueueFlowOptions::new(&server.uri(), "tenant").worker_token("worker-token"),
    )
    .unwrap()
}

fn quick_opts() -> WorkerOptions {
    WorkerOptions {
        lease_secs: 10,
        wait_secs: 1,
        ..WorkerOptions::default()
    }
}

#[tokio::test]
async fn run_worker_leases_runs_and_completes() {
    let server = MockServer::start().await;
    let broker = Broker::new();
    broker.enqueue("job-1", "send-email");
    broker.mount(&server).await;

    let seen = Arc::new(Mutex::new(Vec::<String>::new()));
    let handlers = Handlers::new().register("send-email", {
        let seen = seen.clone();
        move |ctx| {
            let seen = seen.clone();
            async move {
                seen.lock().unwrap().push(ctx.job.id.clone());
                let mut out = ctx.job.payload.unwrap_or_default();
                out.insert("sent".to_owned(), json!(true));
                Ok(out)
            }
        }
    });
    let opts = quick_opts();
    let stop = opts.stop.clone();
    let qf = worker_client(&server).await;
    let worker = tokio::spawn({
        let qf = qf.clone();
        async move { qf.run_worker("q", handlers, opts).await }
    });
    wait_until("completion", || async {
        !requests_to(&server, "/complete").await.is_empty()
    })
    .await;
    stop.cancel();
    worker.await.unwrap().expect("run_worker returns Ok after stop");

    let completes = requests_to(&server, "/complete").await;
    assert_eq!(completes.len(), 1);
    assert_eq!(job_id(&completes[0]), "job-1");
    let body = body_json(&completes[0]);
    assert_eq!(body["lease_token"], "tok");
    assert_eq!(body["result"], json!({"n": 1, "sent": true}));
    assert!(requests_to(&server, "/fail").await.is_empty());

    let leases = requests_to(&server, "/lease").await;
    let first = body_json(&leases[0]);
    assert_eq!(first["max_jobs"], 1);
    assert_eq!(first["lease_secs"], 10);
    assert_eq!(first["wait_secs"], 1);
    for req in server.received_requests().await.unwrap() {
        assert_eq!(
            req.headers.get("authorization").map(|v| v.to_str().unwrap()),
            Some("Bearer worker-token"),
            "worker routes must use the worker token: {}",
            req.url.path()
        );
    }
    assert_eq!(*seen.lock().unwrap(), vec!["job-1".to_owned()]);
}

#[tokio::test]
async fn run_worker_reports_failures() {
    let server = MockServer::start().await;
    let broker = Broker::new();
    broker.enqueue("job-retry", "flaky");
    broker.enqueue("job-perm", "bad-input");
    broker.enqueue("job-question", "parse");
    broker.enqueue("job-ghost", "ghost");
    broker.mount(&server).await;

    let handlers = Handlers::new()
        .register("flaky", |_ctx| async {
            Err(HandlerError::retryable("upstream 503"))
        })
        .register("bad-input", |_ctx| async {
            Err(HandlerError::non_retryable("malformed order"))
        })
        .register("parse", |_ctx| async {
            // `?` on any std error converts into a retryable failure.
            let n: i32 = "not a number".parse()?;
            Ok(obj(json!({"n": n})))
        });
    let opts = quick_opts();
    let stop = opts.stop.clone();
    let qf = worker_client(&server).await;
    let worker = tokio::spawn({
        let qf = qf.clone();
        async move { qf.run_worker("q", handlers, opts).await }
    });
    wait_until("four fail reports", || async {
        requests_to(&server, "/fail").await.len() == 4
    })
    .await;
    stop.cancel();
    worker.await.unwrap().unwrap();

    assert!(requests_to(&server, "/complete").await.is_empty());
    let fails: HashMap<String, Value> = requests_to(&server, "/fail")
        .await
        .iter()
        .map(|r| (job_id(r), body_json(r)))
        .collect();
    let check = |id: &str, retryable: bool, msg: &str| {
        let f = &fails[id];
        assert_eq!(f["lease_token"], "tok", "{}", id);
        assert_eq!(f["retryable"], retryable, "{}: {}", id, f);
        assert!(
            f["error"].as_str().unwrap().contains(msg),
            "{}: error = {}, want {:?}",
            id,
            f["error"],
            msg
        );
    };
    check("job-retry", true, "upstream 503");
    check("job-perm", false, "malformed order");
    check("job-question", true, "invalid digit");
    check("job-ghost", false, "no handler for task");
}

#[tokio::test]
async fn run_worker_heartbeats_and_abandons_lost_leases() {
    for mode in ["409", "cancelled"] {
        let server = MockServer::start().await;
        let broker = Broker::new();
        {
            let mut st = broker.state.lock().unwrap();
            if mode == "409" {
                st.heartbeat_409 = true;
            } else {
                st.heartbeat = "cancelled";
            }
        }
        broker.enqueue("job-1", "slow");
        broker.mount(&server).await;

        let cancelled = Arc::new(tokio::sync::Notify::new());
        let handlers = Handlers::new().register("slow", {
            let cancelled = cancelled.clone();
            move |ctx| {
                let cancelled = cancelled.clone();
                async move {
                    // The handler future itself is dropped when the lease is
                    // lost; work it spawned elsewhere observes the token.
                    let token = ctx.cancel.clone();
                    tokio::spawn(async move {
                        token.cancelled().await;
                        cancelled.notify_one();
                    });
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    Ok(Json::new())
                }
            }
        });
        // lease_secs 2 => a heartbeat every second.
        let opts = WorkerOptions {
            lease_secs: 2,
            wait_secs: 1,
            ..WorkerOptions::default()
        };
        let stop = opts.stop.clone();
        let qf = worker_client(&server).await;
        let worker = tokio::spawn({
            let qf = qf.clone();
            async move { qf.run_worker("q", handlers, opts).await }
        });
        tokio::time::timeout(Duration::from_secs(5), cancelled.notified())
            .await
            .unwrap_or_else(|_| panic!("[{}] job token was not cancelled after the lost-lease heartbeat", mode));
        stop.cancel();
        let started = Instant::now();
        worker.await.unwrap().unwrap();
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "[{}] the abandoned handler must not hold up the stop",
            mode
        );

        let heartbeats = requests_to(&server, "/heartbeat").await;
        assert!(!heartbeats.is_empty(), "[{}] no heartbeat recorded", mode);
        let hb = body_json(&heartbeats[0]);
        assert_eq!(hb["lease_token"], "tok");
        assert_eq!(hb["extend_secs"], 2);
        assert!(
            requests_to(&server, "/complete").await.is_empty()
                && requests_to(&server, "/fail").await.is_empty(),
            "[{}] nothing may be reported after a lost lease",
            mode
        );
    }
}

#[tokio::test]
async fn run_worker_stop_drains_in_flight_jobs() {
    let server = MockServer::start().await;
    let broker = Broker::new();
    broker.enqueue("job-1", "slow");
    broker.mount(&server).await;

    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let handlers = Handlers::new().register("slow", {
        let started = started.clone();
        let release = release.clone();
        move |_ctx| {
            let started = started.clone();
            let release = release.clone();
            async move {
                started.notify_one();
                release.notified().await;
                Ok(obj(json!({"drained": true})))
            }
        }
    });
    let opts = WorkerOptions {
        drain_timeout: Duration::from_secs(5),
        ..quick_opts()
    };
    let stop = opts.stop.clone();
    let qf = worker_client(&server).await;
    let mut worker = tokio::spawn({
        let qf = qf.clone();
        async move { qf.run_worker("q", handlers, opts).await }
    });
    started.notified().await;
    stop.cancel();
    tokio::select! {
        r = &mut worker => panic!("run_worker returned {:?} before the in-flight handler finished", r),
        _ = tokio::time::sleep(Duration::from_millis(200)) => {}
    }
    release.notify_one();
    worker.await.unwrap().unwrap();

    let completes = requests_to(&server, "/complete").await;
    assert_eq!(completes.len(), 1, "the drained job must still be reported");
    assert_eq!(body_json(&completes[0])["result"]["drained"], true);
}

#[tokio::test]
async fn run_worker_drain_timeout_drops_stuck_handlers() {
    let server = MockServer::start().await;
    let broker = Broker::new();
    broker.enqueue("job-1", "stuck");
    broker.mount(&server).await;

    let started = Arc::new(tokio::sync::Notify::new());
    let cancelled = Arc::new(tokio::sync::Notify::new());
    let handlers = Handlers::new().register("stuck", {
        let started = started.clone();
        let cancelled = cancelled.clone();
        move |ctx| {
            let started = started.clone();
            let cancelled = cancelled.clone();
            async move {
                started.notify_one();
                let token = ctx.cancel.clone();
                tokio::spawn(async move {
                    token.cancelled().await;
                    cancelled.notify_one();
                });
                tokio::time::sleep(Duration::from_secs(30)).await;
                Ok(Json::new())
            }
        }
    });
    let opts = WorkerOptions {
        drain_timeout: Duration::from_millis(100),
        ..quick_opts()
    };
    let stop = opts.stop.clone();
    let qf = worker_client(&server).await;
    let worker = tokio::spawn({
        let qf = qf.clone();
        async move { qf.run_worker("q", handlers, opts).await }
    });
    started.notified().await;
    stop.cancel();
    let begin = Instant::now();
    worker.await.unwrap().unwrap();
    assert!(begin.elapsed() < Duration::from_secs(3), "gave up late");
    tokio::time::timeout(Duration::from_secs(1), cancelled.notified())
        .await
        .expect("job token must be cancelled at the drain timeout");
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        requests_to(&server, "/complete").await.is_empty()
            && requests_to(&server, "/fail").await.is_empty(),
        "a drain-cancelled job must not be reported"
    );
}

#[tokio::test]
async fn run_worker_returns_on_auth_failure() {
    for status in [401u16, 403] {
        let server = MockServer::start().await;
        let broker = Broker::new();
        broker.state.lock().unwrap().lease_status = Some(status);
        broker.mount(&server).await;

        let qf = QueueFlow::new(&server.uri(), "t");
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            qf.run_worker("q", Handlers::new(), WorkerOptions::default()),
        )
        .await
        .expect("run_worker kept retrying a fatal auth error");
        let err = result.expect_err("auth failure must surface");
        assert_eq!(err.status(), Some(status));
    }
}

#[tokio::test]
async fn run_worker_backs_off_on_lease_errors() {
    let server = MockServer::start().await;
    let broker = Broker::new();
    broker.state.lock().unwrap().lease_fail_n = 2;
    broker.enqueue("job-1", "echo");
    broker.mount(&server).await;

    let reported = Arc::new(AtomicUsize::new(0));
    let handlers = Handlers::new().register("echo", |_ctx| async { Ok(Json::new()) });
    let opts = WorkerOptions {
        on_error: Some(Arc::new({
            let reported = reported.clone();
            move |e: &FacadeError| {
                assert_eq!(e.status(), Some(500));
                reported.fetch_add(1, Ordering::SeqCst);
            }
        })),
        ..quick_opts()
    };
    let stop = opts.stop.clone();
    let qf = worker_client(&server).await;
    let begin = Instant::now();
    let worker = tokio::spawn({
        let qf = qf.clone();
        async move { qf.run_worker("q", handlers, opts).await }
    });
    wait_until("completion after backoff", || async {
        !requests_to(&server, "/complete").await.is_empty()
    })
    .await;
    stop.cancel();
    worker.await.unwrap().unwrap();
    assert_eq!(reported.load(Ordering::SeqCst), 2);
    assert!(
        begin.elapsed() >= Duration::from_secs(2),
        "two failures should have cost two 1s backoffs, took {:?}",
        begin.elapsed()
    );
}

#[tokio::test]
async fn run_worker_leases_and_runs_concurrently() {
    let server = MockServer::start().await;
    let broker = Broker::new();
    for i in 1..=3 {
        broker.enqueue(&format!("job-{}", i), "barrier");
    }
    broker.mount(&server).await;

    // Only returns once every job is in flight at the same time.
    let barrier = Arc::new(tokio::sync::Barrier::new(3));
    let handlers = Handlers::new().register("barrier", {
        let barrier = barrier.clone();
        move |_ctx| {
            let barrier = barrier.clone();
            async move {
                match tokio::time::timeout(Duration::from_secs(3), barrier.wait()).await {
                    Ok(_) => Ok(Json::new()),
                    Err(_) => Err(HandlerError::retryable(
                        "the other handlers never started: not concurrent",
                    )),
                }
            }
        }
    });
    let opts = WorkerOptions {
        concurrency: 3,
        ..quick_opts()
    };
    let stop = opts.stop.clone();
    let qf = worker_client(&server).await;
    let worker = tokio::spawn({
        let qf = qf.clone();
        async move { qf.run_worker("q", handlers, opts).await }
    });
    wait_until("three completions", || async {
        requests_to(&server, "/complete").await.len() == 3
    })
    .await;
    stop.cancel();
    worker.await.unwrap().unwrap();
    assert!(requests_to(&server, "/fail").await.is_empty());
    let leases = requests_to(&server, "/lease").await;
    assert_eq!(body_json(&leases[0])["max_jobs"], 3, "all slots free on the first lease");
}
