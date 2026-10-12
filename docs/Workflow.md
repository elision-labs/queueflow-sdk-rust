# Workflow

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**completed_at** | Option<**String**> |  | [optional]
**context** | Option<[**std::collections::HashMap<String, serde_json::Value>**](serde_json::Value.md)> | Accumulated step results, keyed by step name. Passed to downstream steps under the `_context` payload key. | [optional]
**created_at** | **String** |  | 
**deadline_at** | Option<**String**> | When the janitor fails the workflow if it is still running (set from `timeout_secs`; pushed forward by a retry). | [optional]
**error_message** | Option<**String**> | Why the workflow ended `failed` or `cancelled`, when known (an upstream failure, a timeout, a user cancel). | [optional]
**id** | **String** |  | 
**metadata** | Option<[**std::collections::HashMap<String, serde_json::Value>**](serde_json::Value.md)> |  | [optional]
**name** | **String** |  | 
**parent_step_name** | Option<**String**> | The parent's step that spawned this child. | [optional]
**parent_workflow_id** | Option<**String**> | Set on a child workflow spawned by a sub-workflow step. | [optional]
**retry_count** | Option<**i32**> | How many times `POST /workflows/{id}/retry` re-ran this workflow. | [optional]
**started_at** | Option<**String**> |  | [optional]
**status** | [**models::WorkflowStatus**](WorkflowStatus.md) |  | 
**steps** | [**Vec<models::WorkflowStep>**](WorkflowStep.md) |  | 
**tenant_id** | Option<**String**> |  | [optional]
**timeout_secs** | Option<**i64**> | The whole-workflow time budget it was created with, if any. | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


