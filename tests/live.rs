/*
Live conformance test: runs the facade against a REAL QueueFlow server.

Skipped unless QUEUEFLOW_URL is set. The server must run in `--mode all` with
the built-in `echo` handler registered (the default `queueflow serve`).

    QUEUEFLOW_URL           base URL, e.g. http://localhost:8000
    QUEUEFLOW_TOKEN         tenant bearer token (default "dev")
    QUEUEFLOW_WORKER_TOKEN  worker-protocol token (read for parity with the other
                            SDKs; this crate ships no worker runtime, so it is unused)

Run with `cargo test --test live`. Plain `cargo test` collects it too, but it
skips itself, so CI stays offline.
*/

use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use queueflow_sdk::apis::{
    cron_api, dlq_api, health_api, jobs_api, system_api, workflows_api, Error,
};
use queueflow_sdk::models::{CreateJobRequest, JobStatus, StepStatus, WorkflowStatus};
use queueflow_sdk::{QueueFlow, WorkflowBuilder};
use serde_json::{json, Value};

const WAIT: Duration = Duration::from_secs(60);
const POLL: Duration = Duration::from_millis(250);

/// The live client, or `None` (after printing why) when QUEUEFLOW_URL is unset.
fn live_client() -> Option<QueueFlow> {
    let url = match std::env::var("QUEUEFLOW_URL") {
        Ok(u) if !u.is_empty() => u,
        _ => {
            eprintln!("skipping live test: QUEUEFLOW_URL is not set");
            return None;
        }
    };
    let token = std::env::var("QUEUEFLOW_TOKEN").unwrap_or_else(|_| "dev".to_owned());
    Some(QueueFlow::new(&url, &token))
}

fn suffix() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch");
    format!("{:x}-{:x}", now.as_secs(), now.subsec_nanos())
}

fn payload_map(v: Value) -> HashMap<String, Value> {
    v.as_object()
        .expect("payload must be a JSON object")
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

fn http_status<T>(err: &Error<T>) -> Option<u16> {
    match err {
        Error::ResponseError(content) => Some(content.status.as_u16()),
        _ => None,
    }
}

#[tokio::test]
async fn live_server_reachable_and_echo_registered() {
    let Some(qf) = live_client() else { return };

    let health = health_api::get_health(&qf.config)
        .await
        .expect("get_health");
    assert!(!health.status.is_empty(), "health status is empty");

    let tasks = system_api::list_tasks(&qf.config)
        .await
        .expect("list_tasks")
        .tasks;
    assert!(
        tasks.iter().any(|t| t == "echo"),
        "echo handler not registered: {:?}",
        tasks
    );
}

#[tokio::test]
async fn live_echo_job_completes_and_echoes_payload() {
    let Some(qf) = live_client() else { return };
    let payload = json!({"hello": "world", "n": 42, "nested": {"ok": true}});
    let key = format!("sdk-live-{}", suffix());

    // The facade's create_job has no idempotency-key parameter; use the
    // generated client for the create and the facade for everything else.
    let mut req = CreateJobRequest::new("echo".to_owned());
    req.payload = Some(payload_map(payload.clone()));
    let created = jobs_api::create_job(&qf.config, req.clone(), Some(&key))
        .await
        .expect("create_job");

    let fetched = jobs_api::get_job(&qf.config, &created.job_id)
        .await
        .expect("get_job");
    assert_eq!(fetched.task_name, "echo");
    assert_eq!(fetched.idempotency_key, Some(Some(key.clone())));

    let done = qf
        .wait_for_job(&created.job_id, WAIT, POLL)
        .await
        .expect("wait_for_job");
    assert_eq!(
        done.status,
        JobStatus::Completed,
        "error: {:?}",
        done.error_message
    );
    // The built-in echo handler returns the payload plus `echoed: true`.
    let mut want = payload_map(payload);
    want.insert("echoed".to_owned(), json!(true));
    assert_eq!(done.result, Some(want));

    // Re-submitting the same idempotency key returns the original job.
    let again = jobs_api::create_job(&qf.config, req, Some(&key))
        .await
        .expect("create_job (replay)");
    assert_eq!(again.job_id, created.job_id);
}

#[tokio::test]
async fn live_two_step_echo_workflow_completes() {
    let Some(qf) = live_client() else { return };

    let dag = WorkflowBuilder::new(&format!("sdk-live-wf-{}", suffix()))
        .step("first", "echo", &[])
        .step("second", "echo", &["first"]);
    let wf = qf.create_workflow(&dag).await.expect("create_workflow");
    assert_eq!(wf.steps.len(), 2);

    let finished = qf
        .wait_for_workflow(&wf.id, WAIT, POLL)
        .await
        .expect("wait_for_workflow");
    assert_eq!(finished.status, WorkflowStatus::Completed);

    let states = workflows_api::get_workflow_step_states(&qf.config, &wf.id)
        .await
        .expect("get_workflow_step_states")
        .steps;
    assert_eq!(states.len(), 2);
    for step in &states {
        assert_eq!(step.status, StepStatus::Completed, "step {}", step.name);
    }
}

#[tokio::test]
async fn live_cron_create_list_pause_resume_delete() {
    let Some(qf) = live_client() else { return };
    let name = format!("sdk-live-cron-{}", suffix());

    // Fires once a year; the schedule never triggers during the test.
    let id = qf
        .create_cron(&name, "0 0 1 1 *", "echo")
        .await
        .expect("create_cron");

    let cron = cron_api::get_cron(&qf.config, &id).await.expect("get_cron");
    assert_eq!(cron.name, name);
    assert!(cron.enabled);

    let listed = cron_api::list_crons(
        &qf.config,
        None,
        None,
        Some(100),
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .await
    .expect("list_crons");
    assert!(
        listed.crons.iter().any(|c| c.id == id),
        "new cron missing from list"
    );

    cron_api::pause_cron(&qf.config, &id)
        .await
        .expect("pause_cron");
    let paused = cron_api::get_cron(&qf.config, &id)
        .await
        .expect("get_cron after pause");
    assert!(!paused.enabled, "after pause: cron still enabled");

    cron_api::resume_cron(&qf.config, &id)
        .await
        .expect("resume_cron");
    let resumed = cron_api::get_cron(&qf.config, &id)
        .await
        .expect("get_cron after resume");
    assert!(resumed.enabled, "after resume: cron still disabled");

    cron_api::delete_cron(&qf.config, &id)
        .await
        .expect("delete_cron");
    match cron_api::get_cron(&qf.config, &id).await {
        Err(err) if http_status(&err) == Some(404) => {}
        other => panic!("get_cron after delete: {:?}, want 404", other.map(|c| c.id)),
    }
}

#[tokio::test]
async fn live_dead_letter_list_and_stats() {
    let Some(qf) = live_client() else { return };

    let dlq = dlq_api::list_dead_letters(
        &qf.config,
        None,
        None,
        Some(10),
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .await
    .expect("list_dead_letters");
    assert!(dlq.limit >= 1);
    let _entries: &Vec<_> = &dlq.dead_letters;

    let stats = system_api::get_stats(&qf.config).await.expect("get_stats");
    assert!(
        stats.jobs_created >= 1,
        "jobs_created = {}, want >= 1 (this suite created jobs)",
        stats.jobs_created
    );
}
