# QueueFlow Rust facade

The facade documentation has moved into [README.md](./README.md), which is the single entry point
for this crate: install, quick start, the `QueueFlow` client, the `WorkflowBuilder`, the generated
client, authentication, the worker runtime (`run_worker`), error handling, and the conformance
tests.

`src/facade.rs` is injected at generation time from the queueflow-core template
(`sdk-templates/rust/facade.mustache`); edit the template, not the file.
