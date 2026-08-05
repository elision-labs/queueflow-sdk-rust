# \JobsApi

All URIs are relative to *http://localhost:8000*

Method | HTTP request | Description
------------- | ------------- | -------------
[**cancel_job**](JobsApi.md#cancel_job) | **POST** /api/v1/jobs/{id}/cancel | 
[**create_batch_jobs**](JobsApi.md#create_batch_jobs) | **POST** /api/v1/jobs/batch | 
[**create_job**](JobsApi.md#create_job) | **POST** /api/v1/jobs | 
[**get_job**](JobsApi.md#get_job) | **GET** /api/v1/jobs/{id} | 
[**list_jobs**](JobsApi.md#list_jobs) | **GET** /api/v1/jobs | 
[**stream_job_events**](JobsApi.md#stream_job_events) | **GET** /api/v1/jobs/{id}/events | Stream a job's status transitions as Server-Sent Events until it reaches a terminal state. Lets clients await completion without polling the REST endpoint themselves.



## cancel_job

> cancel_job(id)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**id** | **String** | Job id | [required] |

### Return type

 (empty response body)

### Authorization

[bearerAuth](../README.md#bearerAuth)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## create_batch_jobs

> models::CreateBatchJobsResponse create_batch_jobs(create_batch_jobs_request)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**create_batch_jobs_request** | [**CreateBatchJobsRequest**](CreateBatchJobsRequest.md) |  | [required] |

### Return type

[**models::CreateBatchJobsResponse**](CreateBatchJobsResponse.md)

### Authorization

[bearerAuth](../README.md#bearerAuth)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## create_job

> models::CreateJobResponse create_job(create_job_request, idempotency_key)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**create_job_request** | [**CreateJobRequest**](CreateJobRequest.md) |  | [required] |
**idempotency_key** | Option<**String**> | Optional client-supplied key making this create idempotent per tenant: retrying with the same key returns the original job instead of creating a duplicate. |  |

### Return type

[**models::CreateJobResponse**](CreateJobResponse.md)

### Authorization

[bearerAuth](../README.md#bearerAuth)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## get_job

> models::Job get_job(id)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**id** | **String** | Job id | [required] |

### Return type

[**models::Job**](Job.md)

### Authorization

[bearerAuth](../README.md#bearerAuth)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## list_jobs

> models::ListJobsResponse list_jobs(status, queue, limit, offset, order_by, include_total)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**status** | Option<**String**> | Filter by status (e.g. `pending`, `completed`). |  |
**queue** | Option<**String**> | Filter by queue name (jobs only). |  |
**limit** | Option<**i64**> | Page size, 1..=100 (default 50). |  |
**offset** | Option<**i64**> | Number of records to skip (default 0). |  |
**order_by** | Option<**String**> | `created_at ASC` or `created_at DESC` (default DESC). |  |
**include_total** | Option<**bool**> | Include the exact `total` count in the response (default false; the count is an extra full scan over the filtered set). |  |

### Return type

[**models::ListJobsResponse**](ListJobsResponse.md)

### Authorization

[bearerAuth](../README.md#bearerAuth)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## stream_job_events

> String stream_job_events(id)
Stream a job's status transitions as Server-Sent Events until it reaches a terminal state. Lets clients await completion without polling the REST endpoint themselves.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**id** | **String** | Job id | [required] |

### Return type

**String**

### Authorization

[bearerAuth](../README.md#bearerAuth)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: text/event-stream, application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

