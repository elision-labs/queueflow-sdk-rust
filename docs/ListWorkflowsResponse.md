# ListWorkflowsResponse

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**has_more** | **bool** |  | 
**limit** | **i64** |  | 
**next_cursor** | Option<**String**> | Opaque keyset cursor for the next page (present when `has_more`). Pass it back as `cursor` to continue where this page ended; cheaper than deep OFFSET paging. | [optional]
**offset** | **i64** |  | 
**total** | Option<**i64**> | Exact total match count; only present when `include_total=true`. | [optional]
**workflows** | [**Vec<models::Workflow>**](Workflow.md) |  | 

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


