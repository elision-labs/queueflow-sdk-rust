# WorkflowStepState

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**child_workflow_id** | Option<**String**> | The child workflow of a sub-workflow step, once spawned. | [optional]
**completed_at** | Option<**String**> | When the step reached a terminal status. | [optional]
**error** | Option<**String**> | Why the step failed, was skipped (`condition not met` for a false `when`) or was cancelled. | [optional]
**fan_out** | Option<[**models::FanOutProgress**](FanOutProgress.md)> | Progress of a fan-out step's children, once spawned. | [optional]
**job_id** | Option<**String**> | The job executing this step, once one has been scheduled (the first child, for a fan-out). | [optional]
**name** | **String** | The step's name (its address within the workflow). | 
**started_at** | Option<**String**> | When the step was scheduled (its job or child workflow created). | [optional]
**status** | [**models::StepStatus**](StepStatus.md) |  | 

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


