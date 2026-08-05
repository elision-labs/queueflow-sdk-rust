# Job

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**completed_at** | Option<**String**> |  | [optional]
**config** | [**models::JobConfig**](JobConfig.md) |  | 
**created_at** | **String** |  | 
**delivery_count** | Option<**i32**> | How many times this job has been claimed (delivered to a worker). Greater than `retry_count + 1` means a lease expired without a report — i.e. a worker crashed mid-run. | [optional]
**error_message** | Option<**String**> |  | [optional]
**id** | **String** |  | 
**idempotency_key** | Option<**String**> | Client-supplied key that makes job creation idempotent per tenant: re-submitting the same key returns the original job instead of creating a duplicate. | [optional]
**metadata** | Option<[**std::collections::HashMap<String, serde_json::Value>**](serde_json::Value.md)> |  | [optional]
**next_retry_at** | Option<**String**> | When this job's next retry becomes claimable (mirrors `scheduled_at` while the job is `retrying`; kept for audit/inspection). | [optional]
**payload** | Option<[**std::collections::HashMap<String, serde_json::Value>**](serde_json::Value.md)> |  | [optional]
**queue_name** | **String** |  | 
**result** | Option<[**std::collections::HashMap<String, serde_json::Value>**](serde_json::Value.md)> |  | [optional]
**retry_count** | **i32** |  | 
**scheduled_at** | **String** | When the job becomes claimable. `created_at` for immediate jobs, the requested `run_at` for scheduled jobs, and the next backoff instant while retrying — the durable delay lives in the row itself. | 
**started_at** | Option<**String**> |  | [optional]
**status** | [**models::JobStatus**](JobStatus.md) |  | 
**task_name** | **String** |  | 
**tenant_id** | Option<**String**> |  | [optional]
**workflow_id** | Option<**String**> |  | [optional]
**workflow_step_id** | Option<**String**> | The owning workflow step's name (steps are addressed by name). | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


