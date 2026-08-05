# HeartbeatResponse

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**status** | [**models::JobStatus**](JobStatus.md) | The job's current status. `running` means the lease was extended; anything else (`cancelled`, `completed`, ...) means it was not, and the worker should stop working on the job. | 

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


