# QueueStats

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**oldest_pending_age_secs** | Option<**i64**> | Seconds the oldest claimable job has been waiting, `None` when nothing is claimable. The queue's primary health signal: it grows when workers cannot keep up and stays near zero when they can. | [optional]
**pending** | **i64** | Claimable now: `pending` or `retrying` with `scheduled_at` in the past. | 
**queue** | **String** |  | 
**running** | **i64** | Currently leased by a worker. | 
**scheduled** | **i64** | Waiting for a future `scheduled_at` (a `run_at` job or a backoff retry). | 

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


