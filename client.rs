// Optimized QueueFlow Rust SDK with performance improvements and best practices
// src/lib.rs

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use reqwest::{Client as HttpClient, RequestBuilder, Response};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use tokio::sync::{RwLock, Semaphore};
use tokio::time::{sleep, timeout, Instant};
use url::Url;
use uuid::Uuid;
use backoff::{ExponentialBackoff, backoff::Backoff};
use tracing::{debug, error, info, warn, instrument};

/// Re-exports for convenience
pub use chrono::{DateTime, Utc};

/// Result type alias for QueueFlow operations
pub type Result<T> = std::result::Result<T, Error>;

/// Connection pool for reusing clients
type ConnectionPool = Arc<RwLock<HashMap<String, Arc<InnerClient>>>>;

/// Comprehensive error types for QueueFlow operations
#[derive(Error, Debug)]
pub enum Error {
    #[error("Configuration error: {message}")]
    Config { message: String },

    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    #[error("Authentication failed: {message}")]
    Authentication { message: String },

    #[error("Rate limit exceeded, retry after {retry_after} seconds")]
    RateLimit { retry_after: u64 },

    #[error("Validation error: {message}")]
    Validation { message: String },

    #[error("Resource not found: {resource_type} with ID {id}")]
    NotFound { resource_type: String, id: String },

    #[error("Operation timed out after {timeout_seconds} seconds")]
    Timeout { timeout_seconds: u64 },

    #[error("API error ({status}): {message}")]
    Api { status: u16, message: String },

    #[error("JSON serialization/deserialization error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("URL parsing error: {0}")]
    Url(#[from] url::ParseError),

    #[error("UUID parsing error: {0}")]
    Uuid(#[from] uuid::Error),
}

/// Job status enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Retrying,
    Cancelled,
}

impl fmt::Display for JobStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", serde_json::to_value(self).unwrap().as_str().unwrap())
    }
}

/// Workflow status enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowStatus {
    Created,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl fmt::Display for WorkflowStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", serde_json::to_value(self).unwrap().as_str().unwrap())
    }
}

/// Job configuration with builder pattern and validation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobConfig {
    #[serde(default)]
    pub priority: i32,
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    #[serde(default = "default_retry_delay")]
    pub retry_delay: u32,
    #[serde(default = "default_timeout")]
    pub timeout: u32,
    #[serde(default = "default_queue")]
    pub queue: String,
}

fn default_max_retries() -> u32 { 3 }
fn default_retry_delay() -> u32 { 60 }
fn default_timeout() -> u32 { 300 }
fn default_queue() -> String { "default".to_string() }

impl Default for JobConfig {
    fn default() -> Self {
        Self {
            priority: 0,
            max_retries: default_max_retries(),
            retry_delay: default_retry_delay(),
            timeout: default_timeout(),
            queue: default_queue(),
        }
    }
}

impl JobConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn priority(mut self, priority: i32) -> Self {
        self.priority = priority.clamp(-10, 10);
        self
    }

    pub fn max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries.min(10);
        self
    }

    pub fn retry_delay(mut self, retry_delay: u32) -> Self {
        self.retry_delay = retry_delay;
        self
    }

    pub fn timeout(mut self, timeout: u32) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn queue<S: Into<String>>(mut self, queue: S) -> Self {
        self.queue = queue.into();
        self
    }
}

/// Workflow step configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowStep {
    pub name: String,
    pub task_name: String,
    pub payload: Value,
    #[serde(default)]
    pub depends_on: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config: Option<JobConfig>,
}

impl WorkflowStep {
    pub fn new<S1, S2>(name: S1, task_name: S2, payload: Value) -> Self
    where
        S1: Into<String>,
        S2: Into<String>,
    {
        Self {
            name: name.into(),
            task_name: task_name.into(),
            payload,
            depends_on: Vec::new(),
            config: None,
        }
    }

    pub fn depends_on(mut self, depends_on: Vec<String>) -> Self {
        self.depends_on = depends_on;
        self
    }

    pub fn config(mut self, config: JobConfig) -> Self {
        self.config = Some(config);
        self
    }
}

/// Job information with optimized serialization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: String,
    pub queue_name: String,
    pub task_name: String,
    pub status: JobStatus,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub error_message: Option<String>,
    pub retry_count: u32,
    pub workflow_id: Option<String>,
    pub result: Option<Value>,
}

impl Job {
    /// Check if job completed successfully
    #[inline]
    pub fn is_successful(&self) -> bool {
        self.status == JobStatus::Completed
    }

    /// Check if job is in a terminal state
    #[inline]
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.status,
            JobStatus::Completed | JobStatus::Failed | JobStatus::Cancelled
        )
    }

    /// Get job duration if completed
    pub fn duration(&self) -> Option<chrono::Duration> {
        match (self.started_at, self.completed_at) {
            (Some(start), Some(end)) => Some(end - start),
            _ => None,
        }
    }
}

/// Workflow information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Workflow {
    pub id: String,
    pub name: String,
    pub status: WorkflowStatus,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub steps: Vec<WorkflowStepInfo>,
    pub context: HashMap<String, Value>,
}

impl Workflow {
    /// Check if workflow completed successfully
    #[inline]
    pub fn is_successful(&self) -> bool {
        self.status == WorkflowStatus::Completed
    }

    /// Check if workflow is in a terminal state
    #[inline]
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.status,
            WorkflowStatus::Completed | WorkflowStatus::Failed | WorkflowStatus::Cancelled
        )
    }

    /// Get workflow duration if completed
    pub fn duration(&self) -> Option<chrono::Duration> {
        match (self.started_at, self.completed_at) {
            (Some(start), Some(end)) => Some(end - start),
            _ => None,
        }
    }

    /// Get failed steps
    pub fn failed_steps(&self) -> Vec<&WorkflowStepInfo> {
        self.steps.iter().filter(|step| step.status == "failed").collect()
    }
}

/// Workflow step information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowStepInfo {
    pub name: String,
    pub task_name: String,
    pub status: String,
    pub job_id: Option<String>,
    pub error_message: Option<String>,
}

/// System statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemStats {
    pub jobs: JobStats,
    pub workflows: WorkflowStats,
    pub workers: WorkerStats,
    pub queues: Vec<QueueInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobStats {
    pub pending: u64,
    pub running: u64,
    pub completed: u64,
    pub failed: u64,
    pub total: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowStats {
    pub running: u64,
    pub completed: u64,
    pub failed: u64,
    pub total: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerStats {
    pub active: u64,
    pub total: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueInfo {
    pub name: String,
    pub size: u64,
    pub processing: u64,
}

/// Health status response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthStatus {
    pub status: String,
    pub database: String,
    pub active_workers: u64,
    pub timestamp: DateTime<Utc>,
}

/// Batch job request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchJobRequest {
    pub task_name: String,
    pub payload: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config: Option<JobConfig>,
}

impl BatchJobRequest {
    pub fn new<S: Into<String>>(task_name: S, payload: Value) -> Self {
        Self {
            task_name: task_name.into(),
            payload,
            config: None,
        }
    }

    pub fn config(mut self, config: JobConfig) -> Self {
        self.config = Some(config);
        self
    }
}

/// Paginated list response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListResponse<T> {
    pub items: Vec<T>,
    pub total: u64,
    pub limit: u64,
    pub offset: u64,
    pub has_more: bool,
}

/// Client configuration with performance optimizations
#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub timeout: Duration,
    pub max_retries: u32,
    pub retry_delay: Duration,
    pub default_queue: String,
    pub user_agent: String,
    pub max_connections: usize,
    pub idle_timeout: Duration,
    pub enable_compression: bool,
    pub enable_http2: bool,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(30),
            max_retries: 3,
            retry_delay: Duration::from_millis(1000),
            default_queue: "default".to_string(),
            user_agent: format!("QueueFlow-Rust-SDK/{}", env!("CARGO_PKG_VERSION")),
            max_connections: 100,
            idle_timeout: Duration::from_secs(90),
            enable_compression: true,
            enable_http2: true,
        }
    }
}

impl ClientConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    pub fn retry_delay(mut self, retry_delay: Duration) -> Self {
        self.retry_delay = retry_delay;
        self
    }

    pub fn default_queue<S: Into<String>>(mut self, queue: S) -> Self {
        self.default_queue = queue.into();
        self
    }

    pub fn user_agent<S: Into<String>>(mut self, user_agent: S) -> Self {
        self.user_agent = user_agent.into();
        self
    }

    pub fn max_connections(mut self, max_connections: usize) -> Self {
        self.max_connections = max_connections;
        self
    }
}

/// Internal client implementation for better resource management
struct InnerClient {
    base_url: Url,
    api_key: String,
    http_client: HttpClient,
    config: ClientConfig,
    request_semaphore: Arc<Semaphore>,
}

/// Main QueueFlow client with connection pooling
#[derive(Clone)]
pub struct Client {
    inner: Arc<InnerClient>,
}

impl Client {
    /// Create a new client with default configuration
    pub fn new<S1, S2>(base_url: S1, api_key: S2) -> Result<Self>
    where
        S1: AsRef<str>,
        S2: Into<String>,
    {
        Self::with_config(base_url, api_key, ClientConfig::default())
    }

    /// Create a new client with custom configuration
    pub fn with_config<S1, S2>(base_url: S1, api_key: S2, config: ClientConfig) -> Result<Self>
    where
        S1: AsRef<str>,
        S2: Into<String>,
    {
        let api_key = api_key.into();
        if api_key.is_empty() {
            return Err(Error::Config {
                message: "API key cannot be empty".to_string(),
            });
        }

        let mut base_url = Url::parse(base_url.as_ref()).map_err(|e| Error::Config {
            message: format!("Invalid base URL: {}", e),
        })?;
        
        // Add API version to path
        base_url.set_path(&format!("{}/api/v1", base_url.path().trim_end_matches('/')));

        let mut http_client_builder = HttpClient::builder()
            .timeout(config.timeout)
            .user_agent(&config.user_agent)
            .pool_max_idle_per_host(config.max_connections)
            .pool_idle_timeout(config.idle_timeout);

        if config.enable_compression {
            http_client_builder = http_client_builder.gzip(true).brotli(true);
        }

        if config.enable_http2 {
            http_client_builder = http_client_builder.http2_prior_knowledge();
        }

        let http_client = http_client_builder.build()?;

        // Create semaphore for rate limiting concurrent requests
        let request_semaphore = Arc::new(Semaphore::new(config.max_connections));

        Ok(Self {
            inner: Arc::new(InnerClient {
                base_url,
                api_key,
                http_client,
                config,
                request_semaphore,
            }),
        })
    }

    /// Build HTTP request with authentication and error handling
    fn request(&self, method: reqwest::Method, path: &str) -> RequestBuilder {
        let url = self.inner.base_url.join(path).expect("Valid URL");
        
        self.inner.http_client
            .request(method, url)
            .header("Authorization", format!("Bearer {}", self.inner.api_key))
            .header("Content-Type", "application/json")
    }

    /// Execute HTTP request with retry logic and instrumentation
    #[instrument(skip(self, request), fields(method = %request.method()))]
    async fn execute_request<T>(&self, request: RequestBuilder) -> Result<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        let mut backoff = ExponentialBackoff::default();
        backoff.max_elapsed_time = Some(Duration::from_secs(60));
        
        loop {
            // Acquire semaphore permit for rate limiting
            let _permit = self.inner.request_semaphore.acquire().await.unwrap();
            
            let request_clone = request.try_clone().ok_or_else(|| Error::Config {
                message: "Failed to clone request".to_string(),
            })?;

            match self.send_request(request_clone).await {
                Ok(response) => return self.handle_response(response).await,
                Err(e) => {
                    // Check if error is retryable
                    let is_retryable = match &e {
                        Error::Api { status, .. } => {
                            // Retry on 429 (rate limit) and 5xx errors
                            *status == 429 || *status >= 500
                        }
                        Error::Http(_) | Error::Timeout { .. } => true,
                        _ => false,
                    };

                    if !is_retryable {
                        return Err(e);
                    }

                    // Check if we should retry
                    match backoff.next_backoff() {
                        Some(duration) => {
                            warn!("Request failed, retrying in {:?}: {}", duration, e);
                            sleep(duration).await;
                        }
                        None => {
                            error!("Request failed after all retries: {}", e);
                            return Err(e);
                        }
                    }
                }
            }
        }
    }

    async fn send_request(&self, request: RequestBuilder) -> Result<Response> {
        let response = timeout(self.inner.config.timeout, request.send()).await
            .map_err(|_| Error::Timeout {
                timeout_seconds: self.inner.config.timeout.as_secs(),
            })?
            .map_err(Error::Http)?;

        Ok(response)
    }

    async fn handle_response<T>(&self, response: Response) -> Result<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        let status = response.status();
        
        if status == 401 {
            return Err(Error::Authentication {
                message: "Invalid API key or authentication failed".to_string(),
            });
        }

        if status == 429 {
            let retry_after = response
                .headers()
                .get("Retry-After")
                .and_then(|h| h.to_str().ok())
                .and_then(|s| s.parse().ok())
                .unwrap_or(60);
            
            return Err(Error::RateLimit { retry_after });
        }

        if !status.is_success() {
            let error_text = response.text().await.unwrap_or_default();
            let message = if error_text.is_empty() {
                format!("HTTP {}", status)
            } else {
                error_text
            };

            return Err(Error::Api {
                status: status.as_u16(),
                message,
            });
        }

        let body = response.json().await?;
        Ok(body)
    }

    /// Create a new job
    #[instrument(skip(self, payload))]
    pub async fn create_job<S>(&self, task_name: S, payload: Value) -> Result<String>
    where
        S: AsRef<str> + std::fmt::Debug,
    {
        self.create_job_with_config(task_name, payload, JobConfig::default())
            .await
    }

    /// Create a new job with custom configuration
    #[instrument(skip(self, payload, config))]
    pub async fn create_job_with_config<S>(
        &self,
        task_name: S,
        payload: Value,
        config: JobConfig,
    ) -> Result<String>
    where
        S: AsRef<str> + std::fmt::Debug,
    {
        let task_name = task_name.as_ref();
        if task_name.is_empty() {
            return Err(Error::Validation {
                message: "Task name cannot be empty".to_string(),
            });
        }

        #[derive(Serialize)]
        struct CreateJobRequest {
            task_name: String,
            payload: Value,
            config: JobConfigRequest,
        }
        
        #[derive(Serialize)]
        struct JobConfigRequest {
            priority: i32,
            max_retries: u32,
            timeout: u32,
            queue: String,
        }

        let request_body = CreateJobRequest {
            task_name: task_name.to_string(),
            payload,
            config: JobConfigRequest {
                priority: config.priority,
                max_retries: config.max_retries,
                timeout: config.timeout,
                queue: config.queue,
            },
        };

        let request = self.request(reqwest::Method::POST, "/jobs").json(&request_body);

        #[derive(Deserialize)]
        struct CreateJobResponse {
            job_id: String,
        }

        let response: CreateJobResponse = self.execute_request(request).await?;
        info!("Created job: {}", response.job_id);
        Ok(response.job_id)
    }

    /// Get job status
    #[instrument(skip(self))]
    pub async fn get_job<S>(&self, job_id: S) -> Result<Job>
    where
        S: AsRef<str> + std::fmt::Debug,
    {
        let job_id = job_id.as_ref();
        if job_id.is_empty() {
            return Err(Error::Validation {
                message: "Job ID cannot be empty".to_string(),
            });
        }

        let path = format!("/jobs/{}", urlencoding::encode(job_id));
        let request = self.request(reqwest::Method::GET, &path);
        
        self.execute_request(request).await.map_err(|e| {
            if let Error::Api { status: 404, .. } = e {
                Error::NotFound {
                    resource_type: "Job".to_string(),
                    id: job_id.to_string(),
                }
            } else {
                e
            }
        })
    }

    /// Cancel a job
    #[instrument(skip(self))]
    pub async fn cancel_job<S>(&self, job_id: S) -> Result<bool>
    where
        S: AsRef<str> + std::fmt::Debug,
    {
        let job_id = job_id.as_ref();
        if job_id.is_empty() {
            return Err(Error::Validation {
                message: "Job ID cannot be empty".to_string(),
            });
        }

        let path = format!("/jobs/{}/cancel", urlencoding::encode(job_id));
        let request = self.request(reqwest::Method::POST, &path);

        #[derive(Deserialize)]
        struct CancelResponse {
            success: bool,
        }

        let response: CancelResponse = self.execute_request(request).await?;
        Ok(response.success)
    }

    /// Wait for job completion with optimized polling
    #[instrument(skip(self))]
    pub async fn wait_for_job<S>(
        &self,
        job_id: S,
        timeout_duration: Duration,
        poll_interval: Duration,
    ) -> Result<Job>
    where
        S: AsRef<str> + std::fmt::Debug,
    {
        let start_time = Instant::now();
        let job_id = job_id.as_ref();
        
        // Use exponential backoff for polling
        let mut current_interval = poll_interval;
        let max_interval = Duration::from_secs(30);

        loop {
            let job = self.get_job(job_id).await?;

            if job.is_terminal() {
                return Ok(job);
            }

            if start_time.elapsed() >= timeout_duration {
                return Err(Error::Timeout {
                    timeout_seconds: timeout_duration.as_secs(),
                });
            }

            sleep(current_interval).await;
            
            // Increase interval with exponential backoff
            current_interval = (current_interval * 2).min(max_interval);
        }
    }

    /// Create a new workflow
    #[instrument(skip(self, steps))]
    pub async fn create_workflow<S>(&self, name: S, steps: Vec<WorkflowStep>) -> Result<String>
    where
        S: AsRef<str> + std::fmt::Debug,
    {
        let name = name.as_ref();
        if name.is_empty() {
            return Err(Error::Validation {
                message: "Workflow name cannot be empty".to_string(),
            });
        }

        if steps.is_empty() {
            return Err(Error::Validation {
                message: "Workflow must have at least one step".to_string(),
            });
        }

        // Validate step names are unique
        let mut step_names = std::collections::HashSet::new();
        for step in &steps {
            if !step_names.insert(&step.name) {
                return Err(Error::Validation {
                    message: format!("Duplicate step name: {}", step.name),
                });
            }
        }

        #[derive(Serialize)]
        struct CreateWorkflowRequest {
            name: String,
            steps: Vec<WorkflowStepRequest>,
        }
        
        #[derive(Serialize)]
        struct WorkflowStepRequest {
            name: String,
            task_name: String,
            payload: Value,
            depends_on: Vec<String>,
            #[serde(skip_serializing_if = "Option::is_none")]
            config: Option<JobConfigRequest>,
        }
        
        #[derive(Serialize)]
        struct JobConfigRequest {
            priority: i32,
            max_retries: u32,
            timeout: u32,
            queue: String,
        }

        let request_body = CreateWorkflowRequest {
            name: name.to_string(),
            steps: steps.into_iter().map(|step| WorkflowStepRequest {
                name: step.name,
                task_name: step.task_name,
                payload: step.payload,
                depends_on: step.depends_on,
                config: step.config.map(|c| JobConfigRequest {
                    priority: c.priority,
                    max_retries: c.max_retries,
                    timeout: c.timeout,
                    queue: c.queue,
                }),
            }).collect(),
        };

        let request = self.request(reqwest::Method::POST, "/workflows").json(&request_body);

        #[derive(Deserialize)]
        struct CreateWorkflowResponse {
            workflow_id: String,
        }

        let response: CreateWorkflowResponse = self.execute_request(request).await?;
        info!("Created workflow: {}", response.workflow_id);
        Ok(response.workflow_id)
    }

    /// Get workflow status
    #[instrument(skip(self))]
    pub async fn get_workflow<S>(&self, workflow_id: S) -> Result<Workflow>
    where
        S: AsRef<str> + std::fmt::Debug,
    {
        let workflow_id = workflow_id.as_ref();
        if workflow_id.is_empty() {
            return Err(Error::Validation {
                message: "Workflow ID cannot be empty".to_string(),
            });
        }

        let path = format!("/workflows/{}", urlencoding::encode(workflow_id));
        let request = self.request(reqwest::Method::GET, &path);
        
        self.execute_request(request).await.map_err(|e| {
            if let Error::Api { status: 404, .. } = e {
                Error::NotFound {
                    resource_type: "Workflow".to_string(),
                    id: workflow_id.to_string(),
                }
            } else {
                e
            }
        })
    }

    /// Cancel a workflow
    #[instrument(skip(self))]
    pub async fn cancel_workflow<S>(&self, workflow_id: S) -> Result<bool>
    where
        S: AsRef<str> + std::fmt::Debug,
    {
        let workflow_id = workflow_id.as_ref();
        if workflow_id.is_empty() {
            return Err(Error::Validation {
                message: "Workflow ID cannot be empty".to_string(),
            });
        }

        let path = format!("/workflows/{}/cancel", urlencoding::encode(workflow_id));
        let request = self.request(reqwest::Method::POST, &path);

        #[derive(Deserialize)]
        struct CancelResponse {
            success: bool,
        }

        let response: CancelResponse = self.execute_request(request).await?;
        Ok(response.success)
    }

    /// Wait for workflow completion with optimized polling
    #[instrument(skip(self))]
    pub async fn wait_for_workflow<S>(
        &self,
        workflow_id: S,
        timeout_duration: Duration,
        poll_interval: Duration,
    ) -> Result<Workflow>
    where
        S: AsRef<str> + std::fmt::Debug,
    {
        let start_time = Instant::now();
        let workflow_id = workflow_id.as_ref();
        
        // Use exponential backoff for polling
        let mut current_interval = poll_interval;
        let max_interval = Duration::from_secs(30);

        loop {
            let workflow = self.get_workflow(workflow_id).await?;

            if workflow.is_terminal() {
                return Ok(workflow);
            }

            if start_time.elapsed() >= timeout_duration {
                return Err(Error::Timeout {
                    timeout_seconds: timeout_duration.as_secs(),
                });
            }

            sleep(current_interval).await;
            
            // Increase interval with exponential backoff
            current_interval = (current_interval * 2).min(max_interval);
        }
    }

    /// Create multiple jobs in batch with parallel execution
    #[instrument(skip(self, jobs))]
    pub async fn create_jobs_batch(&self, jobs: Vec<BatchJobRequest>) -> Result<Vec<String>> {
        if jobs.is_empty() {
            return Err(Error::Validation {
                message: "Batch cannot be empty".to_string(),
            });
        }

        if jobs.len() > 100 {
            return Err(Error::Validation {
                message: "Batch size cannot exceed 100 jobs".to_string(),
            });
        }

        #[derive(Serialize)]
        struct BatchJobsRequest {
            jobs: Vec<BatchJobRequest>,
        }

        let request_body = BatchJobsRequest { jobs };
        let request = self.request(reqwest::Method::POST, "/jobs/batch").json(&request_body);

        #[derive(Deserialize)]
        struct BatchJobsResponse {
            job_ids: Vec<String>,
        }

        let response: BatchJobsResponse = self.execute_request(request).await?;
        info!("Created {} jobs in batch", response.job_ids.len());
        Ok(response.job_ids)
    }

    /// Get system statistics
    #[instrument(skip(self))]
    pub async fn stats(&self) -> Result<SystemStats> {
        let request = self.request(reqwest::Method::GET, "/stats");
        self.execute_request(request).await
    }

    /// Get health status
    #[instrument(skip(self))]
    pub async fn health(&self) -> Result<HealthStatus> {
        let request = self.request(reqwest::Method::GET, "/health");
        self.execute_request(request).await
    }

    /// Test connection to the API
    pub async fn test_connection(&self) -> bool {
        self.health().await.is_ok()
    }

    /// List jobs with pagination
    #[instrument(skip(self))]
    pub async fn list_jobs(
        &self,
        limit: Option<u64>,
        offset: Option<u64>,
        status: Option<JobStatus>,
    ) -> Result<ListResponse<Job>> {
        let mut query_params = Vec::new();
        
        if let Some(limit) = limit {
            query_params.push(format!("limit={}", limit));
        }
        if let Some(offset) = offset {
            query_params.push(format!("offset={}", offset));
        }
        if let Some(status) = status {
            query_params.push(format!("status={}", status));
        }

        let path = if query_params.is_empty() {
            "/jobs".to_string()
        } else {
            format!("/jobs?{}", query_params.join("&"))
        };

        let request = self.request(reqwest::Method::GET, &path);

        #[derive(Deserialize)]
        struct JobListResponse {
            jobs: Vec<Job>,
            total: u64,
            limit: u64,
            offset: u64,
        }

        let response: JobListResponse = self.execute_request(request).await?;
        
        Ok(ListResponse {
            items: response.jobs,
            total: response.total,
            limit: response.limit,
            offset: response.offset,
            has_more: response.offset + response.jobs.len() as u64 < response.total,
        })
    }

    /// List workflows with pagination
    #[instrument(skip(self))]
    pub async fn list_workflows(
        &self,
        limit: Option<u64>,
        offset: Option<u64>,
        status: Option<WorkflowStatus>,
    ) -> Result<ListResponse<Workflow>> {
        let mut query_params = Vec::new();
        
        if let Some(limit) = limit {
            query_params.push(format!("limit={}", limit));
        }
        if let Some(offset) = offset {
            query_params.push(format!("offset={}", offset));
        }
        if let Some(status) = status {
            query_params.push(format!("status={}", status));
        }

        let path = if query_params.is_empty() {
            "/workflows".to_string()
        } else {
            format!("/workflows?{}", query_params.join("&"))
        };

        let request = self.request(reqwest::Method::GET, &path);

        #[derive(Deserialize)]
        struct WorkflowListResponse {
            workflows: Vec<Workflow>,
            total: u64,
            limit: u64,
            offset: u64,
        }

        let response: WorkflowListResponse = self.execute_request(request).await?;
        
        Ok(ListResponse {
            items: response.workflows,
            total: response.total,
            limit: response.limit,
            offset: response.offset,
            has_more: response.offset + response.workflows.len() as u64 < response.total,
        })
    }
}

/// Job builder for fluent API
pub struct JobBuilder<'a> {
    client: &'a Client,
    task_name: String,
    payload: Value,
    config: JobConfig,
}

impl<'a> JobBuilder<'a> {
    pub fn new(client: &'a Client, task_name: String, payload: Value) -> Self {
        Self {
            client,
            task_name,
            payload,
            config: JobConfig::default(),
        }
    }

    pub fn priority(mut self, priority: i32) -> Self {
        self.config.priority = priority.clamp(-10, 10);
        self
    }

    pub fn max_retries(mut self, max_retries: u32) -> Self {
        self.config.max_retries = max_retries.min(10);
        self
    }

    pub fn retry_delay(mut self, retry_delay: u32) -> Self {
        self.config.retry_delay = retry_delay;
        self
    }

    pub fn timeout(mut self, timeout: u32) -> Self {
        self.config.timeout = timeout;
        self
    }

    pub fn queue<S: Into<String>>(mut self, queue: S) -> Self {
        self.config.queue = queue.into();
        self
    }

    pub async fn create(self) -> Result<String> {
        self.client
            .create_job_with_config(&self.task_name, self.payload, self.config)
            .await
    }
}

/// Workflow builder for fluent API
pub struct WorkflowBuilder<'a> {
    client: &'a Client,
    name: String,
    steps: Vec<WorkflowStep>,
    queue: String,
}

impl<'a> WorkflowBuilder<'a> {
    pub fn new(client: &'a Client, name: String) -> Self {
        Self {
            client,
            name,
            steps: Vec::new(),
            queue: client.inner.config.default_queue.clone(),
        }
    }

    pub fn step(mut self, step: WorkflowStep) -> Self {
        self.steps.push(step);
        self
    }

    pub fn queue<S: Into<String>>(mut self, queue: S) -> Self {
        self.queue = queue.into();
        self
    }

    pub async fn create(self) -> Result<String> {
        self.client.create_workflow(&self.name, self.steps).await
    }
}

impl Client {
    /// Create a job using fluent builder API
    pub fn job<S>(&self, task_name: S, payload: Value) -> JobBuilder<'_>
    where
        S: Into<String>,
    {
        JobBuilder::new(self, task_name.into(), payload)
    }

    /// Create a workflow using fluent builder API
    pub fn workflow<S>(&self, name: S) -> WorkflowBuilder<'_>
    where
        S: Into<String>,
    {
        WorkflowBuilder::new(self, name.into())
    }
}

// Re-export commonly used types
pub use chrono::{DateTime, Utc};
pub use serde_json::{json, Value};

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_job_config_builder() {
        let config = JobConfig::new()
            .priority(5)
            .max_retries(5)
            .retry_delay(30)
            .timeout(600)
            .queue("critical");

        assert_eq!(config.priority, 5);
        assert_eq!(config.max_retries, 5);
        assert_eq!(config.retry_delay, 30);
        assert_eq!(config.timeout, 600);
        assert_eq!(config.queue, "critical");
    }

    #[test]
    fn test_job_config_clamping() {
        let config = JobConfig::new()
            .priority(15)  // Should be clamped to 10
            .max_retries(20);  // Should be clamped to 10

        assert_eq!(config.priority, 10);
        assert_eq!(config.max_retries, 10);
    }

    #[test]
    fn test_workflow_step_builder() {
        let step = WorkflowStep::new(
            "test_step",
            "test_task",
            json!({"key": "value"})
        )
        .depends_on(vec!["step1".to_string(), "step2".to_string()])
        .config(JobConfig::new().priority(5));

        assert_eq!(step.name, "test_step");
        assert_eq!(step.task_name, "test_task");
        assert_eq!(step.depends_on.len(), 2);
        assert!(step.config.is_some());
    }

    #[test]
    fn test_job_status_display() {
        assert_eq!(JobStatus::Pending.to_string(), "pending");
        assert_eq!(JobStatus::Running.to_string(), "running");
        assert_eq!(JobStatus::Completed.to_string(), "completed");
        assert_eq!(JobStatus::Failed.to_string(), "failed");
        assert_eq!(JobStatus::Retrying.to_string(), "retrying");
        assert_eq!(JobStatus::Cancelled.to_string(), "cancelled");
    }

    #[test]
    fn test_client_config_builder() {
        let config = ClientConfig::new()
            .timeout(Duration::from_secs(60))
            .max_retries(5)
            .retry_delay(Duration::from_millis(2000))
            .default_queue("production")
            .user_agent("MyApp/1.0")
            .max_connections(200);

        assert_eq!(config.timeout, Duration::from_secs(60));
        assert_eq!(config.max_retries, 5);
        assert_eq!(config.retry_delay, Duration::from_millis(2000));
        assert_eq!(config.default_queue, "production");
        assert_eq!(config.user_agent, "MyApp/1.0");
        assert_eq!(config.max_connections, 200);
    }

    #[test]
    fn test_error_types() {
        let config_error = Error::Config {
            message: "Test error".to_string(),
        };
        assert!(config_error.to_string().contains("Configuration error"));

        let auth_error = Error::Authentication {
            message: "Invalid API key".to_string(),
        };
        assert!(auth_error.to_string().contains("Authentication failed"));

        let rate_limit_error = Error::RateLimit { retry_after: 60 };
        assert!(rate_limit_error.to_string().contains("Rate limit exceeded"));

        let validation_error = Error::Validation {
            message: "Invalid input".to_string(),
        };
        assert!(validation_error.to_string().contains("Validation error"));

        let not_found_error = Error::NotFound {
            resource_type: "Job".to_string(),
            id: "123".to_string(),
        };
        assert!(not_found_error.to_string().contains("Resource not found"));

        let timeout_error = Error::Timeout { timeout_seconds: 30 };
        assert!(timeout_error.to_string().contains("Operation timed out"));

        let api_error = Error::Api {
            status: 500,
            message: "Internal server error".to_string(),
        };
        assert!(api_error.to_string().contains("API error"));
    }

    #[test]
    fn test_batch_job_request() {
        let batch_job = BatchJobRequest::new(
            "test_task",
            json!({"test": "data"})
        )
        .config(JobConfig::new().priority(5));

        assert_eq!(batch_job.task_name, "test_task");
        assert!(batch_job.config.is_some());
        assert_eq!(batch_job.config.unwrap().priority, 5);
    }
}