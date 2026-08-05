# FailJobRequest

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**error** | **String** | Human-readable failure reason. | 
**lease_token** | **String** | The lease token returned by the lease call. | 
**retryable** | Option<**bool**> | Whether the engine may retry (subject to the job's max_retries). Defaults to true; send false for permanent failures (e.g. bad input). | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


