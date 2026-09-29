# JobConfigRequest

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**jitter_factor** | Option<**f64**> | Retry-delay jitter in `0.0..=1.0` (e.g. `0.1` = +/-10%). | [optional]
**max_retries** | Option<**i32**> |  | [optional]
**priority** | Option<**i32**> | Higher is claimed first within a queue (ties: oldest first). | [optional]
**queue** | Option<**String**> | Override the destination queue. | [optional]
**retry_backoff** | Option<[**models::BackoffStrategy**](BackoffStrategy.md)> | How retry delays grow between attempts (default exponential). | [optional]
**retry_delay_secs** | Option<**i64**> | Base retry delay, in seconds. | [optional]
**retry_max_delay_secs** | Option<**i64**> | Upper bound on any computed retry delay, in seconds. | [optional]
**timeout** | Option<**i64**> | Per-attempt timeout, in seconds. | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


