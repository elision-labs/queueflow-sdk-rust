# CreateCronRequest

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**config** | Option<[**models::JobConfig**](JobConfig.md)> |  | [optional]
**cron_expr** | **String** | 5-field crontab (UTC); 6/7 fields with leading seconds also accepted. | 
**name** | **String** | Unique per tenant. | 
**payload** | Option<[**std::collections::HashMap<String, serde_json::Value>**](serde_json::Value.md)> |  | [optional]
**queue** | Option<**String**> |  | [optional]
**task_name** | **String** |  | 

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


