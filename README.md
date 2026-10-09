# queueflow-sdk

Rust client for [QueueFlow](https://queueflow.dev), a PostgreSQL-native distributed job queue and
workflow engine.

- **Typed end-to-end**: request/response models mirror the server's OpenAPI 3.1 spec.
- **Ergonomic**: `QueueFlow::new`, `create_job`, `wait_for_job`/`wait_for_workflow` pollers, and a
  workflow builder with local DAG validation.
- **Async** (tokio + reqwest).
- **Thin facade over a generated core**: the facade (`src/facade.rs`) adds what codegen cannot;
  the generated `apis::*` functions and `models::*` types stay available for everything else.
- **No worker runtime**: this crate exposes the worker-protocol endpoints as raw calls only. For a
  Rust worker, use the first-party [`queueflow-client`](https://github.com/elision-labs/queueflow-core/tree/main/crates/queueflow-client)
  crate (see [Worker protocol](#worker-protocol)).

The crate is `queueflow-sdk`; the bare `queueflow` crate name belongs to the server and CLI binary.

## Install

```toml
[dependencies]
queueflow-sdk = "0.2"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
serde_json = "1"
```

## Quick start

```rust
use std::collections::HashMap;
use std::time::Duration;

use queueflow_sdk::{QueueFlow, WorkflowBuilder};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let qf = QueueFlow::new("http://localhost:8000", "dev");

    // Enqueue a job and wait for the result.
    let payload = HashMap::from([("hello".to_owned(), serde_json::json!("world"))]);
    let job = qf.create_job("echo", Some(payload)).await?;
    let done = qf
        .wait_for_job(&job.id, Duration::from_secs(60), Duration::from_millis(500))
        .await?;
    println!("job {} finished as {} with {:?}", done.id, done.status, done.result);
    // completed, Some({"hello": "world", "echoed": true})

    // Declare and run a DAG workflow. `build()` validates it locally (duplicate
    // names, dangling deps, cycles) before anything goes over the wire.
    let dag = WorkflowBuilder::new("etl")
        .step("extract", "echo", &[])
        .step("transform", "echo", &["extract"])
        .step("load", "echo", &["transform"]);
    let wf = qf.create_workflow(&dag).await?;
    let finished = qf
        .wait_for_workflow(&wf.id, Duration::from_secs(300), Duration::from_secs(1))
        .await?;
    println!("workflow {} finished as {}", finished.id, finished.status);
    Ok(())
}
```

`echo` is a handler built into the server; it returns the payload plus `"echoed": true`.

## API

### Client

`QueueFlow::new(base_url: &str, token: &str) -> QueueFlow` configures the generated client with
`Authorization: Bearer <token>` and exposes:

| Method | Description |
| --- | --- |
| `.config` | The generated `apis::configuration::Configuration`; pass `&qf.config` to any `apis::*` function. |
| `create_job(task: &str, payload: Option<HashMap<String, serde_json::Value>>) -> Result<models::Job, FacadeError>` | Enqueue a job and return the created record. |
| `wait_for_job(job_id: &str, timeout: Duration, interval: Duration) -> Result<models::Job, FacadeError>` | Poll until `completed` / `failed` / `cancelled`; `FacadeError::Timeout` past the deadline. |
| `create_workflow(builder: &WorkflowBuilder) -> Result<models::Workflow, FacadeError>` | Validate the DAG locally, create it, return the record. |
| `wait_for_workflow(workflow_id: &str, timeout: Duration, interval: Duration) -> Result<models::Workflow, FacadeError>` | Poll until a terminal workflow state. |
| `create_cron(name: &str, schedule: &str, task: &str) -> Result<String, FacadeError>` | Register a recurring enqueue (5-field crontab, UTC); returns the schedule id. |
| `replay_dead_letter(id: i64) -> Result<models::Job, FacadeError>` | Re-run a dead-lettered job as a fresh job (at most once; a second replay is a 409). |

### Workflow builder

```rust
let dag = WorkflowBuilder::new("order_123")
    .step("validate", "validate_order", &[])
    .step("pay", "process_payment", &["validate"])
    .step("ship", "create_shipment", &["pay"]);
let body = dag.build()?; // duplicate names, dangling deps and cycles fail here, before any request
```

`step(name, task, after)` adds a step gated on the named `after` steps. `create_workflow` calls
`build()` for you.

### Everything else: the generated client

Anything the facade does not wrap is one call away in `queueflow_sdk::apis`, each function taking
`&qf.config`:

```rust
use queueflow_sdk::apis::{cron_api, dlq_api, jobs_api, system_api, workflows_api};
use queueflow_sdk::models::CreateJobRequest;

// Idempotent create: the same key returns the original job instead of a duplicate.
let mut req = CreateJobRequest::new("echo".to_owned());
req.payload = Some(payload);
let created = jobs_api::create_job(&qf.config, req, Some("order-123")).await?;

// Lists take (status, queue, limit, offset, order_by, include_total, cursor, created_after, created_before).
let failed = jobs_api::list_jobs(&qf.config, Some("failed"), None, Some(50), None, None, None, None, None, None).await?;
let crons = cron_api::list_crons(&qf.config, None, None, Some(100), None, None, None, None, None, None).await?;
let dlq = dlq_api::list_dead_letters(&qf.config, None, None, Some(10), None, None, None, None, None, None).await?;

// Cron lifecycle.
cron_api::pause_cron(&qf.config, &cron_id).await?;
cron_api::resume_cron(&qf.config, &cron_id).await?;
cron_api::delete_cron(&qf.config, &cron_id).await?;

// Workflow progress and diagram.
let states = workflows_api::get_workflow_step_states(&qf.config, &wf_id).await?;
let diagram = workflows_api::get_workflow_diagram(&qf.config, &wf_id).await?;

// Engine introspection.
let stats = system_api::get_stats(&qf.config).await?;
let tasks = system_api::list_tasks(&qf.config).await?;
```

List responses carry `next_cursor` when there are more pages; pass it back as `cursor` for keyset
pagination (cheaper than deep `offset`). Per-endpoint and per-model reference for the generated
layer lives in [`docs/`](./docs).

## Authentication

Every request carries `Authorization: Bearer <token>`. Two credentials exist:

- The **tenant token** (the `token` passed to `QueueFlow::new`) authenticates the job, workflow,
  cron, DLQ and system routes. On a server started without `--api-keys`, any non-empty token is
  accepted (development mode).
- The **worker token** authenticates the worker-protocol routes (lease, heartbeat, complete,
  fail). Configure it on the server with `--worker-token` / `QUEUEFLOW_WORKER_TOKEN`. It is a
  separate secret, never a tenant token. Build a second `Configuration` for it:

  ```rust
  let mut worker_config = qf.config.clone();
  worker_config.bearer_access_token = Some(worker_token.to_owned());
  ```

## Worker protocol

This crate ships **no worker runtime**. If you are writing a Rust worker, use the first-party
[`queueflow-client`](https://github.com/elision-labs/queueflow-core/tree/main/crates/queueflow-client)
crate from the queueflow-core workspace: it shares the server's own domain types and ships a worker
runtime with automatic heartbeating, lease-loss detection, and concurrent batch processing.

If you do call `apis::worker_api` (`lease_jobs`, `heartbeat_job`, `complete_job`, `fail_job`)
directly, three rules keep the at-least-once contract honest:

1. Worker routes authenticate with the worker token, not a tenant token (see above).
2. Heartbeat every in-flight job at roughly half its lease interval. A heartbeat whose `status`
   is anything other than `running` (or an HTTP 409) means the server owns the outcome: abandon
   the handler and report nothing.
3. Delivery is at-least-once, so handlers must be idempotent. Report permanent failures with
   `retryable: false` so they dead-letter immediately instead of burning retries.

## Error handling

Facade helpers return `FacadeError`:

- `FacadeError::Api(Box<dyn Error>)`: the generated client failed (transport, decode, or a
  non-2xx response). The source is an `apis::Error<T>`; its `ResponseError` variant carries the
  HTTP `status`, raw `content`, and the decoded error `entity`.
- `FacadeError::InvalidWorkflow(String)`: a DAG failed local validation, before any request.
- `FacadeError::Timeout(String)`: a `wait_for_*` poller ran out of time.

Generated functions return `apis::Error<OperationError>` directly:

```rust
use queueflow_sdk::apis::{jobs_api, Error};

match jobs_api::get_job(&qf.config, "missing").await {
    Ok(job) => println!("{}", job.status),
    Err(Error::ResponseError(resp)) if resp.status.as_u16() == 404 => println!("no such job"),
    Err(Error::ResponseError(resp)) => eprintln!("{}: {}", resp.status, resp.content),
    Err(other) => return Err(other.into()),
}
```

## Known limitation: `stream_job_events`

The generated `jobs_api::stream_job_events` cannot consume the server's SSE stream: it buffers the
whole response until the stream closes and then fails to decode it. Use `wait_for_job` (polling)
instead, or a hand-rolled SSE consumer over `GET /api/v1/jobs/{id}/events`.

## Conformance tests

`tests/live.rs` runs the facade against a real QueueFlow server (started with the default
`queueflow serve`, i.e. `--mode all`, so the built-in `echo` handler is registered). It creates
and waits on an `echo` job, checks idempotent re-creation, runs a two-step workflow, exercises the
cron and dead-letter endpoints, and reads stats. The tests skip themselves unless `QUEUEFLOW_URL`
is set, so plain `cargo test` and CI stay offline.

```bash
QUEUEFLOW_URL=http://localhost:8000 \
QUEUEFLOW_TOKEN=dev \
QUEUEFLOW_WORKER_TOKEN=worker-secret \
cargo test --test live
```

`QUEUEFLOW_TOKEN` is the tenant token (default `dev`). `QUEUEFLOW_WORKER_TOKEN` is read for
parity with the other SDKs but unused here, since this crate has no worker runtime.

## Architecture

This crate is a thin hand-written **facade** over a **generated core**:

```
queueflow-sdk-rust/
├── src/
│   ├── facade.rs        the hand-written facade (QueueFlow, WorkflowBuilder, FacadeError)
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
`src/facade.rs`.

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
