# CancelJobsRequest

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**created_after** | Option<**String**> | Only jobs created at or after this instant (RFC 3339, inclusive). | [optional]
**created_before** | Option<**String**> | Only jobs created strictly before this instant (RFC 3339, exclusive). | [optional]
**limit** | Option<**i64**> | Cancel at most this many (oldest first). Default 100; the maximum is the server's batch limit (`--max-batch`, 1000 by default). | [optional]
**queue** | Option<**String**> | Only jobs on this queue. | [optional]
**status** | Option<**String**> | Only jobs in this state: `pending`, `retrying` or `running` (all three when omitted). Terminal states are rejected: history is never rewritten. | [optional]
**task_name** | Option<**String**> | Only jobs for this task. | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


