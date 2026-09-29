# \DlqApi

All URIs are relative to *http://localhost:8000*

Method | HTTP request | Description
------------- | ------------- | -------------
[**get_dead_letter**](DlqApi.md#get_dead_letter) | **GET** /api/v1/dlq/{id} | 
[**list_dead_letters**](DlqApi.md#list_dead_letters) | **GET** /api/v1/dlq | 
[**replay_dead_letter**](DlqApi.md#replay_dead_letter) | **POST** /api/v1/dlq/{id}/replay | 



## get_dead_letter

> models::DeadLetter get_dead_letter(id)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**id** | **i64** | Dead letter id | [required] |

### Return type

[**models::DeadLetter**](DeadLetter.md)

### Authorization

[bearerAuth](../README.md#bearerAuth)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## list_dead_letters

> models::ListDeadLettersResponse list_dead_letters(status, queue, limit, offset, order_by, include_total, cursor)


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

[**models::ListDeadLettersResponse**](ListDeadLettersResponse.md)

### Authorization

[bearerAuth](../README.md#bearerAuth)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## replay_dead_letter

> models::ReplayDeadLetterResponse replay_dead_letter(id)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**id** | **i64** | Dead letter id | [required] |

### Return type

[**models::ReplayDeadLetterResponse**](ReplayDeadLetterResponse.md)

### Authorization

[bearerAuth](../README.md#bearerAuth)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

