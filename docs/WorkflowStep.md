# WorkflowStep

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**config** | Option<[**models::JobConfig**](JobConfig.md)> |  | [optional]
**depends_on** | Option<**Vec<String>**> |  | [optional]
**fan_out** | Option<[**models::FanOut**](FanOut.md)> | Run the task once per element of an array in the context. | [optional]
**metadata** | Option<[**std::collections::HashMap<String, serde_json::Value>**](serde_json::Value.md)> |  | [optional]
**name** | **String** |  | 
**on_failure** | Option<[**models::OnFailure**](OnFailure.md)> |  | [optional]
**on_success** | Option<[**models::OnSuccess**](OnSuccess.md)> |  | [optional]
**payload** | Option<[**std::collections::HashMap<String, serde_json::Value>**](serde_json::Value.md)> |  | [optional]
**queue** | Option<**String**> | Queue for this step's job(s); the server's default queue when absent. | [optional]
**task_name** | Option<**String**> | The task handler to run. May be omitted for a sub-workflow step. | [optional]
**when** | Option<[**models::StepCondition**](StepCondition.md)> | Run only when this condition holds against the workflow context at the moment the step becomes ready; otherwise the step is skipped with reason `condition not met` and its dependents proceed. | [optional]
**workflow** | Option<[**models::CreateWorkflowRequest**](CreateWorkflowRequest.md)> | Run an inline child workflow instead of a task. The child's context is its own declared context plus this step's payload, with the parent's context under `_parent`; the step's result is the child's final context. | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


