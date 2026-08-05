# \WorkflowsApi

All URIs are relative to *http://localhost:8000*

Method | HTTP request | Description
------------- | ------------- | -------------
[**cancel_workflow**](WorkflowsApi.md#cancel_workflow) | **POST** /api/v1/workflows/{id}/cancel | 
[**create_workflow**](WorkflowsApi.md#create_workflow) | **POST** /api/v1/workflows | 
[**get_workflow**](WorkflowsApi.md#get_workflow) | **GET** /api/v1/workflows/{id} | 
[**get_workflow_diagram**](WorkflowsApi.md#get_workflow_diagram) | **GET** /api/v1/workflows/{id}/diagram | 
[**list_workflows**](WorkflowsApi.md#list_workflows) | **GET** /api/v1/workflows | 



## cancel_workflow

> cancel_workflow(id)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**id** | **String** | Workflow id | [required] |

### Return type

 (empty response body)

### Authorization

[bearerAuth](../README.md#bearerAuth)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## create_workflow

> models::CreateWorkflowResponse create_workflow(create_workflow_request)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**create_workflow_request** | [**CreateWorkflowRequest**](CreateWorkflowRequest.md) |  | [required] |

### Return type

[**models::CreateWorkflowResponse**](CreateWorkflowResponse.md)

### Authorization

[bearerAuth](../README.md#bearerAuth)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## get_workflow

> models::Workflow get_workflow(id)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**id** | **String** | Workflow id | [required] |

### Return type

[**models::Workflow**](Workflow.md)

### Authorization

[bearerAuth](../README.md#bearerAuth)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## get_workflow_diagram

> models::WorkflowDiagramResponse get_workflow_diagram(id)


### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**id** | **String** | Workflow id | [required] |

### Return type

[**models::WorkflowDiagramResponse**](WorkflowDiagramResponse.md)

### Authorization

[bearerAuth](../README.md#bearerAuth)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## list_workflows

> models::ListWorkflowsResponse list_workflows(status, queue, limit, offset, order_by, include_total)


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

[**models::ListWorkflowsResponse**](ListWorkflowsResponse.md)

### Authorization

[bearerAuth](../README.md#bearerAuth)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

