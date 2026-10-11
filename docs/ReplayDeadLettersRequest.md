# ReplayDeadLettersRequest

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**created_after** | Option<**String**> | Only entries created at or after this instant (RFC 3339, inclusive). | [optional]
**created_before** | Option<**String**> | Only entries created strictly before this instant (RFC 3339, exclusive). | [optional]
**limit** | Option<**i64**> | Consider at most this many entries (oldest first). Default 100; the maximum is the server's batch limit (`--max-batch`). | [optional]
**queue** | Option<**String**> | Only dead letters from this queue. | [optional]
**task_name** | Option<**String**> | Only dead letters for this task. | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


