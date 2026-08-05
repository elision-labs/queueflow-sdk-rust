# \WorkerApi

All URIs are relative to *http://localhost:8000*

Method | HTTP request | Description
------------- | ------------- | -------------
[**complete_job**](WorkerApi.md#complete_job) | **POST** /api/v1/jobs/{id}/complete | 
[**fail_job**](WorkerApi.md#fail_job) | **POST** /api/v1/jobs/{id}/fail | 
[**heartbeat_job**](WorkerApi.md#heartbeat_job) | **POST** /api/v1/jobs/{id}/heartbeat | 
[**lease_jobs**](WorkerApi.md#lease_jobs) | **POST** /api/v1/queues/{queue}/lease | 



## complete_job

> complete_job(id, complete_job_request)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**id** | **String** | Job id | [required] |
**complete_job_request** | [**CompleteJobRequest**](CompleteJobRequest.md) |  | [required] |

### Return type

 (empty response body)

### Authorization

[bearerAuth](../README.md#bearerAuth)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## fail_job

> fail_job(id, fail_job_request)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**id** | **String** | Job id | [required] |
**fail_job_request** | [**FailJobRequest**](FailJobRequest.md) |  | [required] |

### Return type

 (empty response body)

### Authorization

[bearerAuth](../README.md#bearerAuth)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## heartbeat_job

> models::HeartbeatResponse heartbeat_job(id, heartbeat_request)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**id** | **String** | Job id | [required] |
**heartbeat_request** | [**HeartbeatRequest**](HeartbeatRequest.md) |  | [required] |

### Return type

[**models::HeartbeatResponse**](HeartbeatResponse.md)

### Authorization

[bearerAuth](../README.md#bearerAuth)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## lease_jobs

> models::LeaseJobsResponse lease_jobs(queue, lease_jobs_request)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**queue** | **String** | Queue to lease from | [required] |
**lease_jobs_request** | [**LeaseJobsRequest**](LeaseJobsRequest.md) |  | [required] |

### Return type

[**models::LeaseJobsResponse**](LeaseJobsResponse.md)

### Authorization

[bearerAuth](../README.md#bearerAuth)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

