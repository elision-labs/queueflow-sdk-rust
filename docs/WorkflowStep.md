# WorkflowStep

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**config** | Option<[**models::JobConfig**](JobConfig.md)> |  | [optional]
**depends_on** | Option<**Vec<String>**> |  | [optional]
**metadata** | Option<[**std::collections::HashMap<String, serde_json::Value>**](serde_json::Value.md)> |  | [optional]
**name** | **String** |  | 
**on_failure** | Option<[**models::OnFailure**](OnFailure.md)> |  | [optional]
**on_success** | Option<[**models::OnSuccess**](OnSuccess.md)> |  | [optional]
**payload** | Option<[**std::collections::HashMap<String, serde_json::Value>**](serde_json::Value.md)> |  | [optional]
**task_name** | **String** |  | 

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


