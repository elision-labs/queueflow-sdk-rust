# QueueControlResponse

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**paused** | **bool** | Claims of the caller's jobs on this queue are paused. | 
**pending** | **i64** | Claimable jobs of the caller waiting on this queue. | 
**queue** | **String** |  | 
**running** | **i64** | Jobs of the caller still leased by workers. A paused queue is drained once this reaches zero. | 

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


