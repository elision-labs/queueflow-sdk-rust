# queueflow-sdk

Rust client for [QueueFlow](https://queueflow.dev), a PostgreSQL-native distributed job queue and
workflow engine.

- **Typed end-to-end**: request/response models mirror the server's OpenAPI 3.1 spec.
- **Ergonomic**: one method per endpoint on `QueueFlow`, `wait_for_job`/`wait_for_workflow`
  pollers, and a workflow builder with local DAG validation.
- **Worker runtime**: `run_worker` leases jobs, runs your async handlers concurrently, heartbeats
  them, and reports outcomes, with the server's retry policy and dead-letter queue doing the rest.
- **Typed errors**: every non-2xx response is a `FacadeError::Http` carrying the status and body.
- **Async** (tokio + reqwest).
- **Thin facade over a generated core**: the facade (`src/facade.rs`) adds what codegen cannot;
  the generated `apis::*` functions and `models::*` types stay available for everything else.

The crate is `queueflow-sdk`; the bare `queueflow` crate name belongs to the server and CLI binary.

## Install

```toml
[dependencies]
queueflow-sdk = "0.4"
tokio = { version = "1", features = ["macros", "rt-multi-thread", "signal"] }
serde_json = "1"
```

## Quick start

```rust
use std::collections::HashMap;

use queueflow_sdk::{QueueFlow, WaitOptions, WorkflowBuilder};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let qf = QueueFlow::new("http://localhost:8000", "dev");

    // Enqueue a job and wait for the result.
    let payload = HashMap::from([("hello".to_owned(), serde_json::json!("world"))]);
    let id = qf.create_job("echo", Some(payload)).await?;
    let done = qf.wait_for_job(&id, WaitOptions::default()).await?;
    println!("job {} finished as {} with {:?}", done.id, done.status, done.result);
    // completed, Some({"hello": "world", "echoed": true})

    // Declare and run a DAG workflow. `build()` validates it locally (duplicate
    // names, dangling deps, cycles) before anything goes over the wire.
    let dag = WorkflowBuilder::new("etl")
        .step("extract", "echo", &[])
        .step("transform", "echo", &["extract"])
        .step("load", "echo", &["transform"]);
    let wf_id = qf.create_workflow(&dag).await?;
    let finished = qf.wait_for_workflow(&wf_id, WaitOptions::default()).await?;
    println!("workflow {} finished as {}", finished.id, finished.status);
    Ok(())
}
```

`echo` is a handler built into the server; it returns the payload plus `"echoed": true`.

## API

### Client

```rust
use std::time::Duration;
use queueflow_sdk::{QueueFlow, QueueFlowOptions};

let qf = QueueFlow::new(base_url, token);

// or, with a separate worker token and a custom timeout:
let qf = QueueFlow::with_options(
    QueueFlowOptions::new("http://localhost:8000", "dev") // tenant token (required)
        .worker_token("worker-secret")   // for lease/heartbeat/complete/fail and run_worker
        .timeout(Duration::from_secs(30)), // per-request HTTP timeout (default 30s)
)?;
```

`QueueFlow` is `Clone` (cheap: it wraps two `reqwest::Client`s). The per-request timeout applies
to every call except the lease long-poll, which is bounded by `wait_secs + 35s` instead.

| Field | Description |
| --- | --- |
| `qf.config` | The generated `Configuration` with the tenant token; pass `&qf.config` to any `apis::*` function. |
| `qf.worker_config` | The generated `Configuration` with the worker token and no client-level timeout. |

`Json` is `HashMap<String, serde_json::Value>`, the type of every payload, result, context and
metadata.

### Jobs

| Method | Description |
| --- | --- |
| `create_job(task, payload: Option<Json>) -> String` | Enqueue with the server defaults; returns the job id. |
| `create_job_with_options(task, CreateJobOptions) -> String` | Enqueue with config, idempotency key and `run_at`; returns the job id. |
| `get_job(id) -> models::Job` | Fetch a job. |
| `list_jobs(&ListOptions) -> models::ListJobsResponse` | List jobs (`status`, `queue`, `task_name`, `limit`, `offset`, `order_by`, `include_total`, `cursor`, `created_after`, `created_before`). |
| `cancel_job(id)` | Cancel a job. |
| `cancel_jobs(&CancelJobsFilter) -> u64` | Cancel every non-terminal job matching the filter, up to `limit`; returns how many were cancelled. |
| `wait_for_job(id, WaitOptions) -> models::Job` | Poll until `completed` / `failed` / `cancelled`. |

`CreateJobOptions` is `{payload, queue, priority, max_retries, timeout_secs, retry_backoff,
retry_delay_secs, retry_max_delay_secs, jitter_factor, idempotency_key, run_at}`, all `Option`s
with `Default`; unset fields are not sent, so the server defaults apply. `run_at` is an RFC 3339
string. The same `idempotency_key` returns the original job id.

`WaitOptions` is `{timeout, interval}` with a `Default` of 60s and 500ms
(`WaitOptions::with_timeout(d)` keeps the default interval). Past the deadline the call returns
`FacadeError::Timeout`.

List responses carry `next_cursor` when there are more pages; pass it back as `cursor` for keyset
pagination (cheaper than deep `offset`).

`CancelJobsFilter` is `{queue, task_name, status, created_after, created_before, limit}`, all
`Option`s with `Default`; unset fields are not sent. `status` narrows to `pending`, `retrying` or
`running` (a terminal status is a 400). The server cancels at most `limit` jobs per call (default
100), so a return value below the limit means the matching set is exhausted; jobs that finish
concurrently are left as they ended.

```rust
use queueflow_sdk::CancelJobsFilter;

let n = qf.cancel_jobs(&CancelJobsFilter {
    queue: Some("billing".to_owned()),
    status: Some("pending".to_owned()),
    ..CancelJobsFilter::default()
}).await?;
println!("cancelled {} pending billing jobs", n);
```

### Workflows

| Method | Description |
| --- | --- |
| `create_workflow(&WorkflowBuilder) -> String` | Validate the DAG locally, create it, return the workflow id. |
| `get_workflow(id)` / `list_workflows(&ListOptions)` / `cancel_workflow(id)` | Fetch / list / cancel. |
| `workflow_diagram(id) -> models::WorkflowDiagramResponse` | Mermaid (`graph TD`) diagram of the DAG. |
| `workflow_steps(id) -> Vec<models::WorkflowStepState>` | Runtime status of every step, with the job id executing it. |
| `wait_for_workflow(id, WaitOptions) -> models::Workflow` | Poll until a terminal workflow state. |

### Workflow builder

```rust
use queueflow_sdk::models::{JobConfig, OnFailure};
use queueflow_sdk::{Json, StepOptions, WorkflowBuilder};

let dag = WorkflowBuilder::new("order_123")
    .step("validate", "validate_order", &[])
    .step_with_options("pay", "process_payment", StepOptions {
        after: vec!["validate".to_owned()],
        payload: Some(Json::from([("amount".to_owned(), serde_json::json!(42))])),
        on_failure: Some(OnFailure::Halt),
        config: Some(JobConfig { max_retries: Some(5), ..JobConfig::new() }),
        ..StepOptions::default()
    })
    .step("ship", "create_shipment", &["pay"])
    .context(Json::from([("source".to_owned(), serde_json::json!("web"))]));
let body = dag.build()?; // duplicate names, dangling deps and cycles fail here, before any request
```

`step(name, task, after)` adds a step gated on the named `after` steps; `step_with_options` adds
`payload`, `config`, `on_failure`, `on_success` and `metadata`. `context` and `metadata` seed the
workflow-level fields. `create_workflow` calls `build()` for you.

### Worker

Run task handlers in this process against a remote QueueFlow server:

```rust
use std::time::Duration;
use queueflow_sdk::{HandlerError, Handlers, Json, WorkerOptions};

let handlers = Handlers::new()
    .register("send-email", |ctx| async move {
        // ctx.job is the leased models::Job; ctx.cancel fires if the lease is
        // lost or the drain timeout passes (the handler future is dropped too).
        let payload = ctx.job.payload.unwrap_or_default();
        if payload.get("to").is_none() {
            return Err(HandlerError::non_retryable("no recipient")); // dead-letters immediately
        }
        send_email(&payload).await?; // any std::error::Error converts to a retryable failure
        Ok(Json::from([("sent".to_owned(), serde_json::json!(true))]))
    });

let opts = WorkerOptions {
    concurrency: 4,                          // jobs in flight at once (default 1)
    lease_secs: 30,                          // heartbeat at half this interval (default 30)
    wait_secs: 20,                           // server-side long poll when idle (default 20, max 30)
    drain_timeout: Duration::from_secs(30),  // wait for in-flight handlers after stop (default 30s)
    ..WorkerOptions::default()
};
let stop = opts.stop.clone();                // a CancelToken; keep it to stop the worker
let worker = tokio::spawn({
    let qf = qf.clone();
    async move { qf.run_worker("default", handlers, opts).await }
});

tokio::signal::ctrl_c().await?;
stop.cancel();
worker.await??;
```

`run_worker` leases up to `concurrency` jobs per call (one per free slot), runs each handler on
its own tokio task, heartbeats every job at half the lease interval, and reports `complete` or
`fail`. A heartbeat that shows the lease is lost (HTTP 409, or any status other than `running`)
fires `ctx.cancel`, drops the handler future and suppresses the report; the server already owns
the outcome. Jobs whose task has no handler are failed non-retryably; a panicking handler kills
only its own task (its lease expires and the server redelivers).

When `opts.stop` is cancelled, `run_worker` abandons the in-flight lease long-poll, waits up to
`drain_timeout` for running handlers to finish and report, then returns `Ok(())`. It returns the
`FacadeError::Http` when the lease call is rejected with 401 or 403 (a wrong or missing worker
token cannot heal by retrying); other lease errors back off one second and are printed to
stderr on the first failure and every 30th after that (`on_error` replaces that).

Delivery is at-least-once, so handlers must be idempotent. Servers started with
`--worker-token` require `QueueFlowOptions::worker_token`.

The raw protocol is also exposed: `lease_jobs(queue, LeaseOptions)`,
`heartbeat_job(&lease, extend_secs)`, `complete_job(&lease, result)`,
`fail_job(&lease, message, retryable)`.

### Cron

| Method | Description |
| --- | --- |
| `create_cron(name, schedule, task) -> String` | Register a recurring enqueue (5-field crontab, UTC); returns the schedule id. |
| `create_cron_with_options(name, schedule, task, CreateCronOptions) -> String` | Also set `payload`, `queue` and a `config` override. |
| `get_cron(id)` / `list_crons(&ListOptions)` / `delete_cron(id)` | Fetch / list / delete. |
| `pause_cron(id)` / `resume_cron(id)` | Stop firings / resume at the next future occurrence. |

### Dead letters

| Method | Description |
| --- | --- |
| `list_dead_letters(&ListOptions)` / `get_dead_letter(id: i64)` | Inspect terminally-failed jobs. `ListOptions::task_name` and `ListOptions::replayed` (`Some(false)` = not yet replayed, `Some(true)` = replayed) narrow the list. |
| `replay_dead_letter(id: i64) -> String` | Re-run one as a fresh job; returns the new job id (at most once; a second replay is a 409). |
| `replay_dead_letters(&ReplayDeadLettersFilter) -> Vec<models::ReplayedDeadLetter>` | Re-run every unreplayed entry matching the filter, up to `limit`; each result has the `dead_letter_id` plus the new `job_id` or an `error`. |

`ReplayDeadLettersFilter` is `{queue, task_name, created_after, created_before, limit}`, all
`Option`s with `Default`. Only unreplayed entries are considered, so calling it again replays
only what the previous call did not reach (default `limit` 100).

```rust
use queueflow_sdk::ReplayDeadLettersFilter;

let results = qf.replay_dead_letters(&ReplayDeadLettersFilter {
    queue: Some("billing".to_owned()),
    ..ReplayDeadLettersFilter::default()
}).await?;
for r in &results {
    match (&r.job_id, &r.error) {
        (Some(Some(job)), _) => println!("dead letter {} replayed as job {}", r.dead_letter_id, job),
        (_, Some(Some(err))) => println!("dead letter {} not replayed: {}", r.dead_letter_id, err),
        _ => {}
    }
}
```

### System

| Method | Description |
| --- | --- |
| `stats() -> models::StatsSnapshot` | Engine counters. |
| `queues() -> Vec<models::QueueStats>` | Live per-queue backlog: `pending`, `scheduled`, `running`, `oldest_pending_age_secs`, `paused`. |
| `pause_queue(name)` / `resume_queue(name) -> models::QueueControlResponse` | Stop / allow claims of this tenant's jobs on a queue. Idempotent; the response carries `paused`, `running` and `pending`. |
| `tasks() -> Vec<String>` | Registered task handler names. |
| `whoami() -> models::WhoAmIResponse` | The `tenant` the token is scoped to and its `access` (`read_write` or `read_only`). |
| `health()` / `ready()` | `GET /health` / `GET /ready`. |

Pausing a queue stops workers claiming this tenant's jobs on it; jobs already running finish, so
the queue is drained once `running` reaches zero. Pending jobs accumulate until `resume_queue`.

### Everything else: the generated client

Anything the facade does not wrap (batch enqueue, raw request bodies) is one call away in
`queueflow_sdk::apis`, each function taking `&qf.config`:

```rust
use queueflow_sdk::apis::jobs_api;
use queueflow_sdk::models::CreateBatchJobsRequest;

let batch = jobs_api::create_batch_jobs(&qf.config, CreateBatchJobsRequest::new(reqs)).await?;
```

Per-endpoint and per-model reference for the generated layer lives in [`docs/`](./docs).

## Authentication

Every request carries `Authorization: Bearer <token>`. Two credentials exist:

- The **tenant token** (`QueueFlowOptions::token`) authenticates the job, workflow, cron, DLQ and
  system routes. On a server started without `--api-keys`, any non-empty token is accepted
  (development mode).
- The **worker token** (`QueueFlowOptions::worker_token`) authenticates the worker-protocol
  routes (lease, heartbeat, complete, fail), used by `run_worker`. Configure it on the server
  with `--worker-token` / `QUEUEFLOW_WORKER_TOKEN`. It is a separate secret, never a tenant
  token. It defaults to the tenant token, which only works on a server running without one.

## Error handling

Facade methods return `FacadeError`:

- `Http { op, status, body }`: a non-2xx response; `err.status()` returns `Some(status)`.
- `Transport { op, source }`: no response, or it did not decode (connection refused, timeout,
  TLS, JSON).
- `InvalidWorkflow(String)`: a DAG failed local validation, before any request.
- `InvalidOptions(String)`: `with_options` was given an empty `base_url` or `token`.
- `Timeout(String)`: a `wait_for_*` poller, or a worker-protocol call, ran out of time.

```rust
use queueflow_sdk::FacadeError;

match qf.get_job("missing").await {
    Ok(job) => println!("{}", job.status),
    Err(err) if err.status() == Some(404) => println!("no such job"),
    Err(FacadeError::Http { op, status, body }) => eprintln!("{} failed: HTTP {}: {}", op, status, body),
    Err(other) => return Err(other.into()),
}
```

Worker handlers return `HandlerError`: `Retryable(String)` (the server applies the job's retry
policy) or `NonRetryable(String)` (dead-letters immediately). Any `std::error::Error` converts
into `Retryable` with `?`; `HandlerError` deliberately does not implement `std::error::Error`
itself, which is what makes that blanket conversion possible.

Generated functions return `apis::Error<OperationError>` directly; its `ResponseError` variant
carries the HTTP `status`, raw `content`, and the decoded error `entity`.

## Known limitation: `stream_job_events`

The generated `jobs_api::stream_job_events` cannot consume the server's SSE stream: it buffers the
whole response until the stream closes and then fails to decode it. Use `wait_for_job` (polling)
instead, or a hand-rolled SSE consumer over `GET /api/v1/jobs/{id}/events`.

## Conformance tests

`tests/live.rs` runs the facade against a real QueueFlow server (started with the default
`queueflow serve`, i.e. `--mode all`, so the built-in `echo` handler is registered). It creates
and waits on an `echo` job, checks idempotent re-creation and list filters, runs a two-step
workflow, exercises the cron, dead-letter, queue backlog, stats and `whoami` endpoints, and runs
`run_worker` against a dedicated queue: one scenario completes a job with a handler only the test
knows, another dead-letters a job with `HandlerError::non_retryable` and replays it. Bulk cancel,
bulk replay and pause/resume each run against a queue named for that run alone, so the suite is
safe on a shared server. The tests skip themselves unless `QUEUEFLOW_URL` is set, so plain
`cargo test` and CI stay offline.

```bash
QUEUEFLOW_URL=http://localhost:8000 \
QUEUEFLOW_TOKEN=dev \
QUEUEFLOW_WORKER_TOKEN=worker-secret \
cargo test --test live
```

`QUEUEFLOW_TOKEN` is the tenant token (default `dev`). `QUEUEFLOW_WORKER_TOKEN` is the server's
`--worker-token`; it defaults to the tenant token, which only works when the server runs without
one.

## Architecture

This crate is a thin hand-written **facade** over a **generated core**:

```
queueflow-sdk-rust/
├── src/
│   ├── facade.rs        the hand-written facade (QueueFlow, run_worker, WorkflowBuilder, FacadeError)
│   ├── lib.rs           declares and re-exports the facade
│   ├── apis/            generated per-tag functions (jobs_api, workflows_api, worker_api, ...)
│   └── models/          generated models
├── tests/
│   ├── facade.rs        offline facade tests (wiremock)
│   └── live.rs          live conformance tests (need QUEUEFLOW_URL)
└── docs/                generated per-endpoint and per-model reference
```

The core is generated by openapi-generator from the server's
[OpenAPI spec](https://github.com/elision-labs/queueflow-core/blob/main/spec/openapi.yaml)
(`scripts/generate-sdks.sh rust` in queueflow-core). The facade is injected at generation time
from `sdk-templates/rust/facade.mustache` in that repo, so edit the template, not
`src/facade.rs`. `Cargo.toml` and `src/lib.rs` come from `Cargo.mustache` and `lib.mustache`
in the same directory.

### Development

```bash
cargo clippy --all-targets
cargo test
```

## Links

- Documentation: [docs.queueflow.dev](https://docs.queueflow.dev)
- Server: [github.com/elision-labs/queueflow-core](https://github.com/elision-labs/queueflow-core)
- This SDK: [github.com/elision-labs/queueflow-sdk-rust](https://github.com/elision-labs/queueflow-sdk-rust)

## License

[MIT](./LICENSE)
