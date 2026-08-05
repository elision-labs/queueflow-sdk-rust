# DeadLetter

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**created_at** | **String** |  | 
**error_message** | Option<**String**> |  | [optional]
**id** | **i64** |  | 
**job_id** | **String** |  | 
**queue_name** | Option<**String**> |  | [optional]
**reason** | **String** | Why the job dead-lettered: `max_attempts_exceeded`, `non_retryable`, or `handler_not_found`. | 
**replay_job_id** | Option<**String**> | The fresh job created by the replay. | [optional]
**replayed_at** | Option<**String**> | Set once this entry has been replayed; a dead letter replays at most once. | [optional]
**task_name** | Option<**String**> |  | [optional]
**tenant_id** | Option<**String**> |  | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


