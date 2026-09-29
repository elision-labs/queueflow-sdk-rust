# JobConfig

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**jitter_factor** | Option<**f64**> | Optional jitter in `0.0..=1.0`. `0.1` => +/-10% randomization of each retry delay, which spreads out thundering-herd retries. | [optional]
**max_retries** | Option<**i32**> |  | [optional]
**priority** | Option<**i32**> | Higher is claimed first within a queue; ties break on `scheduled_at`, then `created_at`. | [optional]
**retry_backoff** | Option<[**models::BackoffStrategy**](BackoffStrategy.md)> |  | [optional]
**retry_delay_secs** | Option<**i64**> |  | [optional]
**retry_max_delay_secs** | Option<**i64**> |  | [optional]
**timeout_secs** | Option<**i64**> |  | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


