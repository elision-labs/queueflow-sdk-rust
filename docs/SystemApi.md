# \SystemApi

All URIs are relative to *http://localhost:8000*

Method | HTTP request | Description
------------- | ------------- | -------------
[**get_stats**](SystemApi.md#get_stats) | **GET** /api/v1/stats | 
[**list_queues**](SystemApi.md#list_queues) | **GET** /api/v1/queues | 
[**list_tasks**](SystemApi.md#list_tasks) | **GET** /api/v1/tasks | 
[**pause_queue**](SystemApi.md#pause_queue) | **POST** /api/v1/queues/{queue}/pause | 
[**resume_queue**](SystemApi.md#resume_queue) | **POST** /api/v1/queues/{queue}/resume | 
[**who_am_i**](SystemApi.md#who_am_i) | **GET** /api/v1/whoami | 



## get_stats

> models::StatsSnapshot get_stats()


### Parameters

This endpoint does not need any parameter.

### Return type

[**models::StatsSnapshot**](StatsSnapshot.md)

### Authorization

[bearerAuth](../README.md#bearerAuth)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## list_queues

> models::QueuesResponse list_queues()


### Parameters

This endpoint does not need any parameter.

### Return type

[**models::QueuesResponse**](QueuesResponse.md)

### Authorization

[bearerAuth](../README.md#bearerAuth)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## list_tasks

> models::TasksResponse list_tasks()


### Parameters

This endpoint does not need any parameter.

### Return type

[**models::TasksResponse**](TasksResponse.md)

### Authorization

[bearerAuth](../README.md#bearerAuth)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## pause_queue

> models::QueueControlResponse pause_queue(queue)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**queue** | **String** | Queue name | [required] |

### Return type

[**models::QueueControlResponse**](QueueControlResponse.md)

### Authorization

[bearerAuth](../README.md#bearerAuth)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## resume_queue

> models::QueueControlResponse resume_queue(queue)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**queue** | **String** | Queue name | [required] |

### Return type

[**models::QueueControlResponse**](QueueControlResponse.md)

### Authorization

[bearerAuth](../README.md#bearerAuth)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## who_am_i

> models::WhoAmIResponse who_am_i()


### Parameters

This endpoint does not need any parameter.

### Return type

[**models::WhoAmIResponse**](WhoAmIResponse.md)

### Authorization

[bearerAuth](../README.md#bearerAuth)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

