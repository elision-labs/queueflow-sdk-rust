# CronSchedule

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**config** | Option<[**models::JobConfig**](JobConfig.md)> |  | [optional]
**created_at** | **String** |  | 
**cron_expr** | **String** |  | 
**enabled** | **bool** |  | 
**id** | **String** |  | 
**last_enqueued_at** | Option<**String**> |  | [optional]
**name** | **String** | Unique per tenant. | 
**next_run_at** | **String** | The next instant this schedule fires. Missed occurrences (server down) collapse into at most one catch-up firing. | 
**payload** | Option<[**std::collections::HashMap<String, serde_json::Value>**](serde_json::Value.md)> |  | [optional]
**queue_name** | Option<**String**> | Queue for the enqueued jobs (the engine default when absent). | [optional]
**task_name** | **String** |  | 
**tenant_id** | Option<**String**> |  | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


