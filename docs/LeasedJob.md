# LeasedJob

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**job** | [**models::Job**](Job.md) |  | 
**lease_token** | **String** | Opaque, unguessable proof of lease ownership, regenerated on every claim. Pass it back on heartbeat/complete/fail; a stale token (the lease expired and the job was reclaimed) is rejected. | 

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


