use queueflow::{Client, JobConfig, JobStatus, Result, json};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize client
    let client = Client::new("http://localhost:8080", "your-api-key")?;

    // Example 1: Create a simple job
    let job_id = client
        .create_job("send_email", json!({
            "to": "user@example.com",
            "subject": "Welcome!",
            "body": "Thank you for signing up."
        }))
        .await?;
    println!("Created job: {}", job_id);

    // Example 2: Create a job with configuration
    let job_id2 = client
        .create_job_with_config(
            "process_data",
            json!({
                "file_url": "https://example.com/data.csv",
                "format": "csv"
            }),
            JobConfig::new()
                .priority(5)
                .max_retries(5)
                .timeout(600)
                .queue("high-priority"),
        )
        .await?;
    println!("Created high-priority job: {}", job_id2);

    // Example 3: Monitor job status
    let job = client.get_job(&job_id).await?;
    println!("Job status: {:?}", job.status);
    println!("Created at: {}", job.created_at);

    // Example 4: Wait for job completion
    match client.wait_for_job(&job_id, Duration::from_secs(300), Duration::from_secs(2)).await {
        Ok(completed_job) => {
            println!("Job completed with status: {:?}", completed_job.status);
            if let Some(result) = completed_job.result {
                println!("Result: {}", serde_json::to_string_pretty(&result)?);
            }
        }
        Err(e) => {
            eprintln!("Job failed or timed out: {}", e);
        }
    }

    // Example 5: List jobs with filtering
    let jobs = client.list_jobs(Some(10), Some(0), Some(JobStatus::Pending)).await?;
    println!("Found {} pending jobs (total: {})", jobs.items.len(), jobs.total);

    // Example 6: Cancel a job
    if client.cancel_job(&job_id2).await? {
        println!("Successfully cancelled job: {}", job_id2);
    }

    // Example 7: Test connection
    if client.test_connection().await {
        println!("Connection to QueueFlow API successful");
    }

    // Example 8: Get system statistics
    let stats = client.stats().await?;
    println!("System stats:");
    println!("  Jobs - Pending: {}, Running: {}, Completed: {}, Failed: {}",
        stats.jobs.pending, stats.jobs.running, stats.jobs.completed, stats.jobs.failed);
    println!("  Workers - Active: {}/{}", stats.workers.active, stats.workers.total);

    // Example 9: Using the builder pattern for jobs
    let job_id3 = client
        .job("generate_report", json!({"report_type": "monthly", "month": "2024-01"}))
        .priority(8)
        .queue("reports")
        .timeout(1800)
        .create()
        .await?;
    println!("Created report job: {}", job_id3);

    Ok(())
}