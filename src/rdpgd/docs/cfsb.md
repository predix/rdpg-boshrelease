# CFSB

Cloud Foundry Service Broker (CFSB) resides on the management cluster, it talks to CF Cloud Controller (CF CC) providing CFSB API endpoints and also communicates with service clusters in order to manage the assignment and disabling of databases. 

The following functionalities are provided:

* Catalog Management
* Instance Provision
* Instance Binding
* Instance Unbinding
* Instance Deprovision

## Catalog Management

When the CFSB API endpoint is registered with CF CC, the CF CC then requests the catalog from the CFSB API. The CFSB API returns a list of available services and plans with their respective information. After this point services and plans will show up in the CF marketplace, for any user which the command `cf enable-service-access ...` is run for.

See `docs/cloudfoundry.md` for details.

## Instance Provision

When CFSB API receives an instance provision request from the CF CC, it will select the first available database from a service cluster which has the oldest available timestamp as the instance it assigns. Databases used are balanced over time among multiple service clusters to improve performance and capacity management.

## Instance Binding

When CFSB API receives instance binding request from CF CC,it will return the binding information used to bind (eg. connection credentials) the instance selected in the instance provision stage.

## Instance Unbinding

When CFSB API receives an unbinding request for a given instance, it updates the administrative database both in the management cluster and corresponding service cluster to disable the binding.

Note: Currently this is a no-op, actual credentials removal is done during instance deprovision at this time.

## Instance Deprovision

When deprovision an instance, it will schedule the deletion and release the resources it occupied and disables the ability to connect to the instance.
