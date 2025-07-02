# QueueFlow Rust SDK

The official Rust SDK for QueueFlow distributed job queue system.

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
queueflow = "1.0"
```

## Quick Start

```rust
use queueflow::{QueueFlowClient, CreateJobRequest, JobConfig, Priority};
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create client
    let client = QueueFlowClient::new("http://localhost:8080", "your-api-key")?;
    
    // Create job payload
    let mut payload = HashMap::new();
    payload.insert("user_id".to_string(), serde_json::Value::Number(123.into()));
    payload.insert("action".to_string(), serde_json::Value::String("send_email".to_string()));
    
    // Create a job
    let job = client.create_job(CreateJobRequest {
        task_name: "process_data".to_string(),
        payload,
        config: Some(JobConfig {
            priority: Some(Priority::High),
            retries: Some(3),
            timeout: Some(300),
            ..Default::default()
        }),
    }).await?;
    
    println!("Job created: {}", job.id);
    
    // Get job status
    let status = client.get_job(&job.id).await?;
    println!("Job status: {:?}", status.status);
    
    Ok(())
}
```

## Features

- ✅ Async/await support with tokio
- ✅ Type-safe API with strong typing
- ✅ Create and manage jobs
- ✅ Batch job operations
- ✅ Job status monitoring
- ✅ Workflow support
- ✅ Error handling with custom error types
- ✅ Configurable HTTP client
- ✅ Retry mechanisms with exponential backoff

## API Reference

### Client Configuration

```rust
use queueflow::{QueueFlowClient, ClientConfig};
use std::time::Duration;

// Basic client
let client = QueueFlowClient::new("http://localhost:8080", "api-key")?;

// Client with custom configuration
let config = ClientConfig {
    timeout: Duration::from_secs(30),
    max_retries: 3,
    retry_delay: Duration::from_millis(1000),
    ..Default::default()
};

let client = QueueFlowClient::with_config("http://localhost:8080", "api-key", config)?;
```

### Jobs

```rust
use queueflow::{CreateJobRequest, JobConfig, Priority, JobStatus};
use std::collections::HashMap;

// Create a job
let mut payload = HashMap::new();
payload.insert("email".to_string(), serde_json::Value::String("user@example.com".to_string()));

let job = client.create_job(CreateJobRequest {
    task_name: "send_email".to_string(),
    payload,
    config: Some(JobConfig {
        priority: Some(Priority::High),
        retries: Some(3),
        timeout: Some(60),
        delay: Some(5),
        queue: Some("emails".to_string()),
        ..Default::default()
    }),
}).await?;

// Get job status
let job = client.get_job(&job_id).await?;

// Cancel job
client.cancel_job(&job_id).await?;

// List jobs
let jobs = client.list_jobs(Some(JobStatus::Pending), Some(50), Some(0)).await?;
```

### Batches

```rust
use queueflow::{CreateBatchRequest, CreateJobRequest};

// Create batch
let jobs = vec![
    CreateJobRequest {
        task_name: "task1".to_string(),
        payload: {
            let mut map = HashMap::new();
            map.insert("data".to_string(), serde_json::Value::String("value1".to_string()));
            map
        },
        config: None,
    },
    CreateJobRequest {
        task_name: "task2".to_string(),
        payload: {
            let mut map = HashMap::new();
            map.insert("data".to_string(), serde_json::Value::String("value2".to_string()));
            map
        },
        config: None,
    },
];

let batch = client.create_batch(CreateBatchRequest { jobs }).await?;

// Get batch status
let batch = client.get_batch(&batch.id).await?;
```

### Workflows

```rust
use queueflow::{CreateWorkflowRequest, WorkflowStep};

// Create workflow
let workflow = client.create_workflow(CreateWorkflowRequest {
    name: "data_pipeline".to_string(),
    steps: vec![
        WorkflowStep {
            name: "extract".to_string(),
            task_name: "extract_data".to_string(),
            payload: {
                let mut map = HashMap::new();
                map.insert("source".to_string(), serde_json::Value::String("database".to_string()));
                map
            },
            depends_on: vec![],
            config: None,
        },
        WorkflowStep {
            name: "transform".to_string(),
            task_name: "transform_data".to_string(),
            payload: {
                let mut map = HashMap::new();
                map.insert("format".to_string(), serde_json::Value::String("json".to_string()));
                map
            },
            depends_on: vec!["extract".to_string()],
            config: None,
        },
    ],
}).await?;

// Get workflow status
let workflow = client.get_workflow(&workflow.id).await?;
```

## Types

### Core Types

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: String,
    pub task_name: String,
    pub payload: HashMap<String, serde_json::Value>,
    pub status: JobStatus,
    pub created_at: String,
    pub updated_at: String,
    pub result: Option<HashMap<String, serde_json::Value>>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JobStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Priority {
    Low,
    Normal,
    High,
    Critical,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct JobConfig {
    pub priority: Option<Priority>,
    pub retries: Option<u32>,
    pub timeout: Option<u64>,
    pub delay: Option<u64>,
    pub queue: Option<String>,
}
```

## Error Handling

```rust
use queueflow::{QueueFlowError, ErrorKind};

match client.create_job(request).await {
    Ok(job) => println!("Job created: {}", job.id),
    Err(QueueFlowError { kind: ErrorKind::NotFound, message }) => {
        println!("Resource not found: {}", message);
    },
    Err(QueueFlowError { kind: ErrorKind::Validation, message }) => {
        println!("Validation error: {}", message);
    },
    Err(QueueFlowError { kind: ErrorKind::Network, message }) => {
        println!("Network error: {}", message);
    },
    Err(e) => {
        println!("Other error: {}", e);
    }
}
```

## Examples

### Polling for Job Completion

```rust
use tokio::time::{sleep, Duration};

async fn wait_for_job(client: &QueueFlowClient, job_id: &str) -> Result<Job, QueueFlowError> {
    loop {
        let job = client.get_job(job_id).await?;
        
        match job.status {
            JobStatus::Completed => return Ok(job),
            JobStatus::Failed => {
                return Err(QueueFlowError {
                    kind: ErrorKind::JobFailed,
                    message: job.error.unwrap_or_else(|| "Job failed".to_string()),
                });
            },
            _ => {
                // Wait 5 seconds before polling again
                sleep(Duration::from_secs(5)).await;
            }
        }
    }
}
```

### Batch Processing with Progress

```rust
use queueflow::{CreateBatchRequest, BatchStatus};

async fn process_batch(
    client: &QueueFlowClient, 
    jobs: Vec<CreateJobRequest>
) -> Result<(), QueueFlowError> {
    let batch = client.create_batch(CreateBatchRequest { jobs }).await?;
    
    println!("Batch created: {}", batch.id);
    
    loop {
        let status = client.get_batch(&batch.id).await?;
        
        println!(
            "Progress: {}/{} completed, {} failed", 
            status.completed_jobs, 
            status.total_jobs, 
            status.failed_jobs
        );
        
        match status.status {
            BatchStatus::Completed => {
                println!("Batch completed successfully!");
                break;
            },
            BatchStatus::Failed => {
                return Err(QueueFlowError {
                    kind: ErrorKind::BatchFailed,
                    message: "Batch processing failed".to_string(),
                });
            },
            _ => {
                sleep(Duration::from_secs(10)).await;
            }
        }
    }
    
    Ok(())
}
```

### Custom HTTP Client

```rust
use reqwest::Client;
use queueflow::QueueFlowClient;

// Create custom HTTP client with specific settings
let http_client = Client::builder()
    .timeout(Duration::from_secs(30))
    .build()?;

let client = QueueFlowClient::with_http_client(
    "http://localhost:8080",
    "api-key",
    http_client
)?;
```

## Development

```bash
# Run tests
cargo test

# Run tests with output
cargo test -- --nocapture

# Check code
cargo check

# Format code
cargo fmt

# Lint code
cargo clippy

# Build documentation
cargo doc --open
```

## Examples

Check out the [examples](./examples/) directory:

- [Basic Usage](./examples/basic_usage.rs)
- [Workflow Example](./examples/workflow_example.rs)
- [Batch Processing](./examples/batch_processing.rs)
- [Error Handling](./examples/error_handling.rs)

## License

MIT License