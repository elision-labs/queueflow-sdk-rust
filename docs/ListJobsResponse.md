# ListJobsResponse

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**has_more** | **bool** |  | 
**jobs** | [**Vec<models::Job>**](Job.md) |  | 
**limit** | **i64** |  | 
**offset** | **i64** |  | 
**total** | Option<**i64**> | Exact total match count. Only present when the request set `include_total=true`; computing it costs a full count over the filtered set, so it is opt-in. | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


