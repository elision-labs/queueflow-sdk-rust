use queueflow::{Client, WorkflowStep, WorkflowStatus, JobConfig, Result, json};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize client
    let client = Client::new("http://localhost:8080", "your-api-key")?;

    // Example 1: Video processing workflow
    let video_steps = vec![
        WorkflowStep::new("download", "download_file", json!({
            "url": "https://example.com/video.mp4"
        })),
        WorkflowStep::new("transcode", "transcode_video", json!({
            "format": "webm",
            "quality": "high"
        }))
        .depends_on(vec!["download".to_string()]),
        WorkflowStep::new("thumbnail", "generate_thumbnail", json!({
            "timestamps": [0, 30, 60]
        }))
        .depends_on(vec!["download".to_string()]),
        WorkflowStep::new("upload", "upload_to_cdn", json!({
            "bucket": "videos",
            "public": true
        }))
        .depends_on(vec!["transcode".to_string(), "thumbnail".to_string()]),
    ];

    let workflow_id = client.create_workflow("video_processing", video_steps).await?;
    println!("Created video processing workflow: {}", workflow_id);

    // Example 2: Data pipeline workflow with step configuration
    let data_steps = vec![
        WorkflowStep::new("fetch", "fetch_api_data", json!({
            "endpoint": "https://api.example.com/data",
            "auth_token": "secret"
        }))
        .config(JobConfig::new().timeout(300).max_retries(5)),
        
        WorkflowStep::new("validate", "validate_data", json!({
            "schema_version": "2.0",
            "strict": true
        }))
        .depends_on(vec!["fetch".to_string()]),
        
        WorkflowStep::new("transform", "etl_transform", json!({
            "format": "parquet",
            "compression": "snappy"
        }))
        .depends_on(vec!["validate".to_string()])
        .config(JobConfig::new().queue("heavy-compute").timeout(1800)),
        
        // Parallel storage operations
        WorkflowStep::new("store_warehouse", "store_to_warehouse", json!({
            "table": "processed_data",
            "partition": "2024-01"
        }))
        .depends_on(vec!["transform".to_string()]),
        
        WorkflowStep::new("store_cache", "store_to_redis", json!({
            "key_prefix": "data:processed:",
            "ttl": 3600
        }))
        .depends_on(vec!["transform".to_string()]),
        
        // Notification after both storage operations complete
        WorkflowStep::new("notify", "send_notification", json!({
            "channels": ["slack", "email"],
            "message": "Data pipeline completed successfully"
        }))
        .depends_on(vec!["store_warehouse".to_string(), "store_cache".to_string()]),
    ];

    let pipeline_id = client.create_workflow("data_pipeline", data_steps).await?;
    println!("Created data pipeline workflow: {}", pipeline_id);

    // Example 3: Monitor workflow progress
    let workflow = client.get_workflow(&workflow_id).await?;
    println!("Workflow status: {:?}", workflow.status);
    println!("Started at: {:?}", workflow.started_at);

    // Example 4: Wait for workflow completion
    match client.wait_for_workflow(&workflow_id, Duration::from_secs(600), Duration::from_secs(5)).await {
        Ok(completed_workflow) => {
            println!("Workflow completed with status: {:?}", completed_workflow.status);
            println!("Duration: {:?}", completed_workflow.duration());
            
            // Check for failed steps
            let failed_steps = completed_workflow.failed_steps();
            if !failed_steps.is_empty() {
                println!("Failed steps:");
                for step in failed_steps {
                    println!("  - {}: {}", step.name, step.error_message.as_ref().unwrap_or(&"Unknown error".to_string()));
                }
            }
        }
        Err(e) => {
            eprintln!("Workflow failed or timed out: {}", e);
        }
    }

    // Example 5: List workflows
    let workflows = client.list_workflows(Some(10), Some(0), Some(WorkflowStatus::Running)).await?;
    println!("Found {} running workflows", workflows.items.len());

    // Example 6: Using workflow builder pattern
    let workflow_id2 = client
        .workflow("image_processing")
        .step(WorkflowStep::new("download", "download_image", json!({"url": "https://example.com/image.jpg"})))
        .step(WorkflowStep::new("resize", "resize_image", json!({"width": 800, "height": 600}))
            .depends_on(vec!["download".to_string()]))
        .step(WorkflowStep::new("optimize", "optimize_image", json!({"quality": 85}))
            .depends_on(vec!["resize".to_string()]))
        .create()
        .await?;
    println!("Created image processing workflow: {}", workflow_id2);

    // Example 7: Cancel a workflow
    if client.cancel_workflow(&pipeline_id).await? {
        println!("Successfully cancelled workflow: {}", pipeline_id);
    }

    Ok(())
}