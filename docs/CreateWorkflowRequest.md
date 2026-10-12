# CreateWorkflowRequest

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**context** | Option<[**std::collections::HashMap<String, serde_json::Value>**](serde_json::Value.md)> |  | [optional]
**metadata** | Option<[**std::collections::HashMap<String, serde_json::Value>**](serde_json::Value.md)> |  | [optional]
**name** | **String** |  | 
**steps** | [**Vec<models::WorkflowStep>**](WorkflowStep.md) |  | 
**timeout_secs** | Option<**i64**> | Whole-workflow time budget in seconds. When it runs out the janitor cancels every unfinished step and fails the workflow with `workflow timed out`. Unset means no limit. | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


