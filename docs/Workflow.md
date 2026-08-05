# Workflow

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**completed_at** | Option<**String**> |  | [optional]
**context** | Option<[**std::collections::HashMap<String, serde_json::Value>**](serde_json::Value.md)> | Accumulated step results, keyed by step name. Passed to downstream steps under the `_context` payload key. | [optional]
**created_at** | **String** |  | 
**id** | **String** |  | 
**metadata** | Option<[**std::collections::HashMap<String, serde_json::Value>**](serde_json::Value.md)> |  | [optional]
**name** | **String** |  | 
**started_at** | Option<**String**> |  | [optional]
**status** | [**models::WorkflowStatus**](WorkflowStatus.md) |  | 
**steps** | [**Vec<models::WorkflowStep>**](WorkflowStep.md) |  | 
**tenant_id** | Option<**String**> |  | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


