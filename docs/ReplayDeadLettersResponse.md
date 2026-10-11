# ReplayDeadLettersResponse

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**replayed** | **i64** | How many of `results` produced a job. | 
**results** | [**Vec<models::ReplayedDeadLetter>**](ReplayedDeadLetter.md) | Entries that were replayed (`job_id` set) or could not be (`error` set), oldest first. Entries already replayed are not listed. | 

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


