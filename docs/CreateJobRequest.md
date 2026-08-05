# CreateJobRequest

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**config** | Option<[**models::JobConfigRequest**](JobConfigRequest.md)> |  | [optional]
**payload** | Option<[**std::collections::HashMap<String, serde_json::Value>**](serde_json::Value.md)> | Arbitrary JSON object passed to the handler. | [optional]
**run_at** | Option<**String**> | Don't run before this instant (RFC 3339). The job is created immediately but stays invisible to workers until then. | [optional]
**task_name** | **String** | The registered task handler to invoke. | 

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


