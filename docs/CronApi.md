# \CronApi

All URIs are relative to *http://localhost:8000*

Method | HTTP request | Description
------------- | ------------- | -------------
[**create_cron**](CronApi.md#create_cron) | **POST** /api/v1/cron | 
[**delete_cron**](CronApi.md#delete_cron) | **DELETE** /api/v1/cron/{id} | 
[**get_cron**](CronApi.md#get_cron) | **GET** /api/v1/cron/{id} | 
[**list_crons**](CronApi.md#list_crons) | **GET** /api/v1/cron | 
[**pause_cron**](CronApi.md#pause_cron) | **POST** /api/v1/cron/{id}/pause | 
[**resume_cron**](CronApi.md#resume_cron) | **POST** /api/v1/cron/{id}/resume | 



## create_cron

> models::CreateCronResponse create_cron(create_cron_request)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**create_cron_request** | [**CreateCronRequest**](CreateCronRequest.md) |  | [required] |

### Return type

[**models::CreateCronResponse**](CreateCronResponse.md)

### Authorization

[bearerAuth](../README.md#bearerAuth)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## delete_cron

> delete_cron(id)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**id** | **String** | Cron schedule id | [required] |

### Return type

 (empty response body)

### Authorization

[bearerAuth](../README.md#bearerAuth)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## get_cron

> models::CronSchedule get_cron(id)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**id** | **String** | Cron schedule id | [required] |

### Return type

[**models::CronSchedule**](CronSchedule.md)

### Authorization

[bearerAuth](../README.md#bearerAuth)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## list_crons

> models::ListCronsResponse list_crons(status, queue, limit, offset, order_by, include_total, cursor)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**status** | Option<**String**> | Filter by status (e.g. `pending`, `completed`). |  |
**queue** | Option<**String**> | Filter by queue name (jobs only). |  |
**limit** | Option<**i64**> | Page size, 1..=100 (default 50). |  |
**offset** | Option<**i64**> | Number of records to skip (default 0). |  |
**order_by** | Option<**String**> | `created_at ASC` or `created_at DESC` (default DESC). |  |
**include_total** | Option<**bool**> | Include the exact `total` count in the response (default false; the count is an extra full scan over the filtered set). |  |
**cursor** | Option<**String**> | Opaque keyset cursor from a previous page's `next_cursor`. When set, `offset` is ignored and listing continues where that page ended. |  |

### Return type

[**models::ListCronsResponse**](ListCronsResponse.md)

### Authorization

[bearerAuth](../README.md#bearerAuth)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## pause_cron

> pause_cron(id)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**id** | **String** | Cron schedule id | [required] |

### Return type

 (empty response body)

### Authorization

[bearerAuth](../README.md#bearerAuth)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## resume_cron

> resume_cron(id)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**id** | **String** | Cron schedule id | [required] |

### Return type

 (empty response body)

### Authorization

[bearerAuth](../README.md#bearerAuth)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

