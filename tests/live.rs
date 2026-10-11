/*
Live conformance test: runs the facade against a REAL QueueFlow server.

Skipped unless QUEUEFLOW_URL is set. The server must run in `--mode all` with
the built-in `echo` handler registered (the default `queueflow serve`).

    QUEUEFLOW_URL           base URL, e.g. http://localhost:8000
    QUEUEFLOW_TOKEN         tenant bearer token (default "dev")
    QUEUEFLOW_WORKER_TOKEN  worker-protocol token (defaults to QUEUEFLOW_TOKEN,
                            which only works when the server has no --worker-token)

Run with `cargo test --test live`. Plain `cargo test` collects it too, but it
skips itself, so CI stays offline.
*/

use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use queueflow_sdk::models::{JobStatus, StepStatus, WorkflowStatus};
use queueflow_sdk::{
    CancelJobsFilter, CreateJobOptions, HandlerError, Handlers, Json, LeaseOptions, ListOptions,
    QueueFlow, QueueFlowOptions, ReplayDeadLettersFilter, StepOptions, WaitOptions,
    WorkerOptions, WorkflowBuilder,
};
use serde_json::{json, Value};

const WAIT: WaitOptions = WaitOptions {
    timeout: Duration::from_secs(60),
    interval: Duration::from_millis(250),
};

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
    let mut opts = QueueFlowOptions::new(&url, &token);
    if let Ok(worker) = std::env::var("QUEUEFLOW_WORKER_TOKEN") {
        if !worker.is_empty() {
            opts = opts.worker_token(&worker);
        }
    }
    Some(QueueFlow::with_options(opts).expect("live client"))
}

fn suffix() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch");
    format!("{:x}-{:x}", now.as_secs(), now.subsec_nanos())
}

fn obj(v: Value) -> Json {
    v.as_object()
        .expect("payload must be a JSON object")
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

/// Runs `body` while a worker for `queue` serves `handlers`, then stops the
/// worker and returns the body's output.
async fn with_worker<T, F>(qf: &QueueFlow, queue: &str, handlers: Handlers, body: F) -> T
where
    F: std::future::Future<Output = T>,
{
    let opts = WorkerOptions {
        lease_secs: 10,
        wait_secs: 2,
        ..WorkerOptions::default()
    };
    let stop = opts.stop.clone();
    let worker = tokio::spawn({
        let qf = qf.clone();
        let queue = queue.to_owned();
        async move { qf.run_worker(&queue, handlers, opts).await }
    });
    let out = body.await;
    stop.cancel();
    worker
        .await
        .expect("worker task panicked")
        .expect("run_worker");
    out
}

#[tokio::test]
async fn live_server_reachable_and_echo_registered() {
    let Some(qf) = live_client() else { return };

    let health = qf.health().await.expect("health");
    assert!(!health.status.is_empty(), "health status is empty");
    qf.ready().await.expect("ready");

    let tasks = qf.tasks().await.expect("tasks");
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
    let opts = || CreateJobOptions {
        payload: Some(obj(payload.clone())),
        idempotency_key: Some(key.clone()),
        ..CreateJobOptions::default()
    };

    let id = qf
        .create_job_with_options("echo", opts())
        .await
        .expect("create_job_with_options");

    let fetched = qf.get_job(&id).await.expect("get_job");
    assert_eq!(fetched.task_name, "echo");
    assert_eq!(fetched.idempotency_key, Some(Some(key.clone())));

    let done = qf.wait_for_job(&id, WAIT).await.expect("wait_for_job");
    assert_eq!(
        done.status,
        JobStatus::Completed,
        "error: {:?}",
        done.error_message
    );
    // The built-in echo handler returns the payload plus `echoed: true`.
    let mut want = obj(payload.clone());
    want.insert("echoed".to_owned(), json!(true));
    assert_eq!(done.result, Some(want));

    // Re-submitting the same idempotency key returns the original job.
    let again = qf
        .create_job_with_options("echo", opts())
        .await
        .expect("create_job_with_options (replay)");
    assert_eq!(again, id);

    // The job shows up in a filtered list.
    let listed = qf
        .list_jobs(&ListOptions {
            status: Some("completed".to_owned()),
            limit: Some(100),
            order_by: Some("created_at DESC".to_owned()),
            ..ListOptions::default()
        })
        .await
        .expect("list_jobs");
    assert!(
        listed.jobs.iter().any(|j| j.id == id),
        "job {} missing from the completed list",
        id
    );
}

#[tokio::test]
async fn live_two_step_echo_workflow_completes() {
    let Some(qf) = live_client() else { return };

    let dag = WorkflowBuilder::new(&format!("sdk-live-wf-{}", suffix()))
        .step_with_options(
            "first",
            "echo",
            StepOptions {
                payload: Some(obj(json!({"step": 1}))),
                ..StepOptions::default()
            },
        )
        .step_with_options(
            "second",
            "echo",
            StepOptions {
                after: vec!["first".to_owned()],
                payload: Some(obj(json!({"step": 2}))),
                ..StepOptions::default()
            },
        );
    let id = qf.create_workflow(&dag).await.expect("create_workflow");
    let wf = qf.get_workflow(&id).await.expect("get_workflow");
    assert_eq!(wf.steps.len(), 2);

    let finished = qf
        .wait_for_workflow(&id, WAIT)
        .await
        .expect("wait_for_workflow");
    assert_eq!(finished.status, WorkflowStatus::Completed);

    let states = qf.workflow_steps(&id).await.expect("workflow_steps");
    assert_eq!(states.len(), 2);
    for step in &states {
        assert_eq!(step.status, StepStatus::Completed, "step {}", step.name);
    }

    let diagram = qf.workflow_diagram(&id).await.expect("workflow_diagram");
    assert!(!diagram.diagram.is_empty(), "empty diagram");
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

    let cron = qf.get_cron(&id).await.expect("get_cron");
    assert_eq!(cron.name, name);
    assert!(cron.enabled);

    let listed = qf
        .list_crons(&ListOptions {
            limit: Some(100),
            ..ListOptions::default()
        })
        .await
        .expect("list_crons");
    assert!(
        listed.crons.iter().any(|c| c.id == id),
        "new cron missing from list"
    );

    qf.pause_cron(&id).await.expect("pause_cron");
    assert!(
        !qf.get_cron(&id).await.expect("get_cron after pause").enabled,
        "after pause: cron still enabled"
    );

    qf.resume_cron(&id).await.expect("resume_cron");
    assert!(
        qf.get_cron(&id).await.expect("get_cron after resume").enabled,
        "after resume: cron still disabled"
    );

    qf.delete_cron(&id).await.expect("delete_cron");
    match qf.get_cron(&id).await {
        Err(err) if err.status() == Some(404) => {}
        other => panic!("get_cron after delete: {:?}, want 404", other.map(|c| c.id)),
    }
}

#[tokio::test]
async fn live_dead_letter_list_queues_and_stats() {
    let Some(qf) = live_client() else { return };

    let dlq = qf
        .list_dead_letters(&ListOptions {
            limit: Some(10),
            ..ListOptions::default()
        })
        .await
        .expect("list_dead_letters");
    assert!(dlq.limit >= 1);

    let stats = qf.stats().await.expect("stats");
    assert!(
        stats.jobs_created >= 1,
        "jobs_created = {}, want >= 1 (this suite created jobs)",
        stats.jobs_created
    );

    // A job on a queue no in-process worker polls stays pending, so its
    // queue shows up in the backlog view while we look.
    let queue = format!("sdk-live-backlog-{}", suffix());
    let id = qf
        .create_job_with_options(
            "echo",
            CreateJobOptions {
                queue: Some(queue.clone()),
                ..CreateJobOptions::default()
            },
        )
        .await
        .expect("create_job_with_options");
    let queues = qf.queues().await;
    let _ = qf.cancel_job(&id).await;
    let queues = queues.expect("queues");
    let entry = queues
        .iter()
        .find(|q| q.queue == queue)
        .unwrap_or_else(|| panic!("queue {} missing from queues(): {:?}", queue, queues));
    assert_eq!(entry.pending, 1);
}

#[tokio::test]
async fn live_whoami_resolves_the_tenant_token() {
    let Some(qf) = live_client() else { return };
    let me = qf.whoami().await.expect("whoami");
    assert!(!me.tenant.is_empty(), "empty tenant");
    assert!(
        me.access == "read_write" || me.access == "read_only",
        "access = {:?}",
        me.access
    );
    // The suite creates jobs, so the configured token must be read-write.
    assert_eq!(me.access, "read_write");
}

/// Bulk cancel, scoped to a queue only this run uses (the server is shared):
/// bounded by `limit`, narrowed by `status`, and a terminal status is
/// rejected.
#[tokio::test]
async fn live_cancel_jobs_cancels_matching_jobs_up_to_limit() {
    let Some(qf) = live_client() else { return };
    let suffix = suffix();
    let queue = format!("rust-live-cancel-{}", suffix);
    let task = format!("rust-live-cancel-task-{}", suffix);

    let mut ids = Vec::new();
    for n in 0..3 {
        let id = qf
            .create_job_with_options(
                &task,
                CreateJobOptions {
                    payload: Some(obj(json!({"n": n}))),
                    queue: Some(queue.clone()),
                    ..CreateJobOptions::default()
                },
            )
            .await
            .expect("create_job_with_options");
        ids.push(id);
    }

    // task_name narrows the job list to exactly this run's jobs.
    let listed = qf
        .list_jobs(&ListOptions {
            queue: Some(queue.clone()),
            task_name: Some(task.clone()),
            limit: Some(100),
            ..ListOptions::default()
        })
        .await
        .expect("list_jobs");
    assert_eq!(listed.jobs.len(), 3, "{:?}", listed.jobs);

    let cancelled = qf
        .cancel_jobs(&CancelJobsFilter {
            queue: Some(queue.clone()),
            limit: Some(2),
            ..CancelJobsFilter::default()
        })
        .await
        .expect("cancel_jobs (limit 2)");
    assert_eq!(cancelled, 2);
    let mut statuses = Vec::new();
    for id in &ids {
        statuses.push(qf.get_job(id).await.expect("get_job").status);
    }
    assert_eq!(
        statuses.iter().filter(|s| **s == JobStatus::Cancelled).count(),
        2,
        "{:?}",
        statuses
    );
    assert_eq!(
        statuses.iter().filter(|s| **s == JobStatus::Pending).count(),
        1,
        "{:?}",
        statuses
    );

    let cancelled = qf
        .cancel_jobs(&CancelJobsFilter {
            queue: Some(queue.clone()),
            task_name: Some(task.clone()),
            status: Some("pending".to_owned()),
            ..CancelJobsFilter::default()
        })
        .await
        .expect("cancel_jobs (pending)");
    assert_eq!(cancelled, 1);

    let cancelled = qf
        .cancel_jobs(&CancelJobsFilter {
            queue: Some(queue.clone()),
            ..CancelJobsFilter::default()
        })
        .await
        .expect("cancel_jobs (exhausted)");
    assert_eq!(cancelled, 0, "nothing left to cancel");

    match qf
        .cancel_jobs(&CancelJobsFilter {
            queue: Some(queue.clone()),
            status: Some("completed".to_owned()),
            ..CancelJobsFilter::default()
        })
        .await
    {
        Err(err) if err.status() == Some(400) => {}
        other => panic!("cancel_jobs with a terminal status: {:?}, want 400", other),
    }
}

/// Bulk replay, scoped to a queue only this run uses: two dead letters are
/// replayed in one call, the `replayed` list filter flips from unreplayed to
/// replayed, and the fresh jobs are cancelled by queue afterwards.
#[tokio::test]
async fn live_replay_dead_letters_replays_unreplayed_entries_once() {
    let Some(qf) = live_client() else { return };
    let suffix = suffix();
    let queue = format!("rust-live-replay-{}", suffix);
    let task = format!("rust-live-replay-task-{}", suffix);

    let mut ids = Vec::new();
    for n in 0..2 {
        let id = qf
            .create_job_with_options(
                &task,
                CreateJobOptions {
                    payload: Some(obj(json!({"n": n}))),
                    queue: Some(queue.clone()),
                    ..CreateJobOptions::default()
                },
            )
            .await
            .expect("create_job_with_options");
        ids.push(id);
    }
    let handlers = Handlers::new().register(&task, |ctx| async move {
        Err(HandlerError::non_retryable(format!("poison {}", ctx.job.id)))
    });
    let done = with_worker(&qf, &queue, handlers, async {
        let mut done = Vec::new();
        for id in &ids {
            done.push(qf.wait_for_job(id, WAIT).await.expect("wait_for_job"));
        }
        done
    })
    .await;
    for job in &done {
        assert_eq!(job.status, JobStatus::Failed, "job {}", job.id);
    }

    let unreplayed = qf
        .list_dead_letters(&ListOptions {
            queue: Some(queue.clone()),
            replayed: Some(false),
            limit: Some(10),
            ..ListOptions::default()
        })
        .await
        .expect("list_dead_letters (unreplayed)");
    assert_eq!(unreplayed.dead_letters.len(), 2, "{:?}", unreplayed.dead_letters);

    let results = qf
        .replay_dead_letters(&ReplayDeadLettersFilter {
            queue: Some(queue.clone()),
            task_name: Some(task.clone()),
            ..ReplayDeadLettersFilter::default()
        })
        .await
        .expect("replay_dead_letters");
    assert_eq!(results.len(), 2, "{:?}", results);
    let mut replayed_ids: Vec<i64> = results.iter().map(|r| r.dead_letter_id).collect();
    replayed_ids.sort_unstable();
    let mut want_ids: Vec<i64> = unreplayed.dead_letters.iter().map(|d| d.id).collect();
    want_ids.sort_unstable();
    assert_eq!(replayed_ids, want_ids);
    let fresh: Vec<String> = results
        .iter()
        .map(|r| {
            let job_id = r.job_id.clone().flatten();
            assert!(job_id.is_some(), "no job_id: {:?}", r);
            job_id.unwrap()
        })
        .collect();
    for id in &fresh {
        assert!(!ids.contains(id), "replay reused the original id {}", id);
    }

    // A second bulk replay finds nothing: entries replay at most once.
    let again = qf
        .replay_dead_letters(&ReplayDeadLettersFilter {
            queue: Some(queue.clone()),
            ..ReplayDeadLettersFilter::default()
        })
        .await
        .expect("replay_dead_letters (again)");
    assert!(again.is_empty(), "{:?}", again);

    let still_unreplayed = qf
        .list_dead_letters(&ListOptions {
            queue: Some(queue.clone()),
            replayed: Some(false),
            limit: Some(10),
            ..ListOptions::default()
        })
        .await
        .expect("list_dead_letters (unreplayed, after)");
    assert!(still_unreplayed.dead_letters.is_empty());
    let replayed = qf
        .list_dead_letters(&ListOptions {
            queue: Some(queue.clone()),
            task_name: Some(task.clone()),
            replayed: Some(true),
            limit: Some(10),
            ..ListOptions::default()
        })
        .await
        .expect("list_dead_letters (replayed)");
    assert_eq!(replayed.dead_letters.len(), 2);
    for entry in &replayed.dead_letters {
        let replay_job = entry.replay_job_id.clone().flatten();
        assert!(
            replay_job.as_ref().is_some_and(|j| fresh.contains(j)),
            "replay_job_id {:?} not among {:?}",
            replay_job,
            fresh
        );
    }

    // The fresh jobs wait on this run's queue; cancel them by queue.
    let cancelled = qf
        .cancel_jobs(&CancelJobsFilter {
            queue: Some(queue.clone()),
            ..CancelJobsFilter::default()
        })
        .await
        .expect("cancel_jobs");
    assert_eq!(cancelled, 2);
}

/// Pausing a queue only this run uses stops leases of its jobs until it is
/// resumed; both calls are idempotent and `queues()` reflects the flag.
#[tokio::test]
async fn live_pause_queue_blocks_leases_until_resume() {
    let Some(qf) = live_client() else { return };
    let suffix = suffix();
    let queue = format!("rust-live-pause-{}", suffix);
    let task = format!("rust-live-pause-task-{}", suffix);

    let id = qf
        .create_job_with_options(
            &task,
            CreateJobOptions {
                queue: Some(queue.clone()),
                ..CreateJobOptions::default()
            },
        )
        .await
        .expect("create_job_with_options");

    let paused = qf.pause_queue(&queue).await.expect("pause_queue");
    assert_eq!(paused.queue, queue);
    assert!(paused.paused);
    assert_eq!(paused.pending, 1);
    assert_eq!(paused.running, 0);
    let again = qf.pause_queue(&queue).await.expect("pause_queue (again)");
    assert!(again.paused, "pause must be idempotent");

    let queues = qf.queues().await.expect("queues");
    let entry = queues
        .iter()
        .find(|q| q.queue == queue)
        .unwrap_or_else(|| panic!("queue {} missing from queues(): {:?}", queue, queues));
    assert_eq!(entry.paused, Some(true));

    let leased = qf
        .lease_jobs(
            &queue,
            LeaseOptions {
                wait_secs: Some(1),
                ..LeaseOptions::default()
            },
        )
        .await
        .expect("lease_jobs (paused)");
    assert!(leased.is_empty(), "leased from a paused queue: {:?}", leased);
    assert_eq!(
        qf.get_job(&id).await.expect("get_job").status,
        JobStatus::Pending
    );

    let resumed = qf.resume_queue(&queue).await.expect("resume_queue");
    assert!(!resumed.paused);
    assert_eq!(resumed.pending, 1);
    let again = qf.resume_queue(&queue).await.expect("resume_queue (again)");
    assert!(!again.paused, "resume must be idempotent");

    let leased = qf
        .lease_jobs(
            &queue,
            LeaseOptions {
                lease_secs: Some(30),
                wait_secs: Some(5),
                ..LeaseOptions::default()
            },
        )
        .await
        .expect("lease_jobs (resumed)");
    assert_eq!(leased.len(), 1, "{:?}", leased);
    assert_eq!(leased[0].job.id, id);
    qf.complete_job(&leased[0], obj(json!({"paused_then_resumed": true})))
        .await
        .expect("complete_job");
    let done = qf.wait_for_job(&id, WAIT).await.expect("wait_for_job");
    assert_eq!(done.status, JobStatus::Completed);
}

/// The worker runtime: enqueue on a queue the server's in-process workers
/// never poll, with a task name only this test's worker knows, so the job can
/// only be completed by `run_worker`.
#[tokio::test]
async fn live_worker_runtime_leases_and_completes_a_job() {
    let Some(qf) = live_client() else { return };
    let suffix = suffix();
    let queue = format!("sdk-live-q-{}", suffix);
    let task = format!("sdk-live-task-{}", suffix);

    let id = qf
        .create_job_with_options(
            &task,
            CreateJobOptions {
                payload: Some(obj(json!({"order": 7}))),
                queue: Some(queue.clone()),
                ..CreateJobOptions::default()
            },
        )
        .await
        .expect("create_job_with_options");
    let job = qf.get_job(&id).await.expect("get_job");
    assert_eq!(job.queue_name, queue);
    assert_eq!(job.status, JobStatus::Pending);

    let handled = Arc::new(Mutex::new(Vec::<String>::new()));
    let handlers = Handlers::new().register(&task, {
        let handled = handled.clone();
        move |ctx| {
            let handled = handled.clone();
            async move {
                handled.lock().unwrap().push(ctx.job.id.clone());
                let mut out = ctx.job.payload.unwrap_or_default();
                out.insert("handled".to_owned(), json!(true));
                Ok(out)
            }
        }
    });
    let done = with_worker(&qf, &queue, handlers, qf.wait_for_job(&id, WAIT))
        .await
        .expect("wait_for_job");

    assert_eq!(*handled.lock().unwrap(), vec![id.clone()]);
    assert_eq!(
        done.status,
        JobStatus::Completed,
        "error: {:?}",
        done.error_message
    );
    assert_eq!(done.result, Some(obj(json!({"order": 7, "handled": true}))));
}

/// A non-retryable error dead-letters immediately; the entry can be read and
/// replayed exactly once.
#[tokio::test]
async fn live_worker_non_retryable_failure_dead_letters() {
    let Some(qf) = live_client() else { return };
    let suffix = suffix();
    let queue = format!("sdk-live-dlq-q-{}", suffix);
    let task = format!("sdk-live-dlq-task-{}", suffix);

    let id = qf
        .create_job_with_options(
            &task,
            CreateJobOptions {
                queue: Some(queue.clone()),
                max_retries: Some(5),
                ..CreateJobOptions::default()
            },
        )
        .await
        .expect("create_job_with_options");

    let handlers = Handlers::new().register(&task, |ctx| async move {
        Err(HandlerError::non_retryable(format!(
            "bad input for {}",
            ctx.job.id
        )))
    });
    let done = with_worker(&qf, &queue, handlers, qf.wait_for_job(&id, WAIT))
        .await
        .expect("wait_for_job");
    assert_eq!(done.status, JobStatus::Failed);
    assert_eq!(done.retry_count, 0, "a non-retryable failure must not retry");

    let dlq = qf
        .list_dead_letters(&ListOptions {
            queue: Some(queue.clone()),
            limit: Some(10),
            ..ListOptions::default()
        })
        .await
        .expect("list_dead_letters");
    assert_eq!(dlq.dead_letters.len(), 1, "{:?}", dlq.dead_letters);
    assert_eq!(dlq.dead_letters[0].job_id, id);
    let entry = qf
        .get_dead_letter(dlq.dead_letters[0].id)
        .await
        .expect("get_dead_letter");

    let replayed = qf
        .replay_dead_letter(entry.id)
        .await
        .expect("replay_dead_letter");
    assert!(!replayed.is_empty() && replayed != id, "replay = {:?}", replayed);
    let second = qf.replay_dead_letter(entry.id).await;
    let _ = qf.cancel_job(&replayed).await;
    match second {
        Err(err) if err.status() == Some(409) => {}
        other => panic!("second replay: {:?}, want 409", other),
    }
}
