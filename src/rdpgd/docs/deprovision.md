# Deprovision

Deprovision is the process by which all resources associated with a given service
instance that were created during the provisioning process are freed.

## Workflow

Below we walk through the entire instance deprovision process at the conceptual
level.

### Management Cluster - Deprovision API Endpoint

The CFSB API (which exists only on the management cluster) endpoint for 
deprovisioning a service instance is called first.  The API handler marks the 
instance to be deprovisioned as ineffective and also creates a new task to be
run to handle the actual deprovisioning of the instance. Then the management 
cluster API returns `HTTP 200/OK` to the caller.

### Management Cluster - DecommissionDatabase Task

The task is picked up from the task queue by workers on the management cluster.
The worker calls the correct service cluster API to deprovision the instance
on the service cluster.

### Service Cluster - Deprovision API Endpoint

The service cluster API handler receives the request from the management cluster
and does the following.  First it marks the instance as ineffective.
Then it schedules a task to do the decommissioning.

### Service Cluster - DecommissionDatabase Task

Next the decommissioning task runs on one of the nodes in the service cluster 
(whichever gets the task lock). The first thing that the task does is to create
a scheduled task for each node in the service cluster to reconfigure PGBouncer, 
this will cause access to be denied to new connections (since the instance is 
marked as ineffective). Then the handler goes on to perform the final 
decomission. 

Final decommission of the service instance (database) involves:

* disabling connections to the database
* removing permissions to the database by changing ownership
* terminating all connections to the database
* removing replication slots.
* performing a final backup
* drop the database
* drop the user
* mark the instance as decomissioned

