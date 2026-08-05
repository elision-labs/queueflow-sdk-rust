# LeaseJobsRequest

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**lease_secs** | Option<**i32**> | Lease duration in seconds (1..=3600, default 30). Heartbeat to extend. | [optional]
**max_jobs** | Option<**i32**> | Maximum jobs to lease in one call (1..=100, default 1). | [optional]
**wait_secs** | Option<**i32**> | Long-poll wait when the queue is empty, in seconds (0..=30, default 0). | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


