# JobConfig

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**jitter_factor** | Option<**f64**> | Optional jitter in `0.0..=1.0`. `0.1` => +/-10% randomization of each retry delay, which spreads out thundering-herd retries. | [optional]
**max_retries** | **i32** |  | 
**priority** | **i32** | Higher is claimed first within a queue; ties break on `scheduled_at`, then `created_at`. | 
**retry_backoff** | Option<[**models::BackoffStrategy**](BackoffStrategy.md)> |  | [optional]
**retry_delay_secs** | **i64** |  | 
**retry_max_delay_secs** | **i64** |  | 
**timeout_secs** | **i64** |  | 

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


