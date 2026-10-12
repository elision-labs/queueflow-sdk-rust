# FanOut

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**item_key** | Option<**String**> | Payload key the element is passed under (default `item`). | [optional]
**max_items** | Option<**i32**> | Upper bound on elements; more than this fails the step before any child runs. Default [`limits::MAX_FAN_OUT`]. | [optional]
**path** | **String** | Dotted path into the context, for example `fetch.images`. | 

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


