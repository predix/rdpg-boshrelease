# Management 

The management cluster is responsible for managing all the service clusters, scheduling certain backgroud tasks, providing service broker API for CF cloud controller to use and health check for the rdpgd system.

It mainly has four components.

* CFSB API endpoints
* Service Cluster Management
* Scheduler
* Health Check

The management cluster has at least three nodes, internally one is selected as a write master, which is transparent to outside clients (eg. CF CC). The management cluster talks with CF cloud controller and also service clusters.

## CFSB API 
CF cloud controller talks to the management cluster through CFSB API endpoints to request catalog, instance provision/de-provision/bind/unbind. When CF requests instance and binding, the managemnet cluster will assign database from a service cluster to the app. When CF requests instance ubinding and deprovison, the management cluster will tell the corresponding service cluster to unbinding/disable the instance.

The details of CFSB API please see `docs/cfsb.md`.

## Service Cluster Management
The management cluster manages all the service clusters through administrative databases. It will store records for all the available and active databases on each service cluster, and compare its records with database records on each service cluster and reconcile if it is not consistant. 

## Scheduler
The management cluster has a scheduler which schedules the running backgroud tasks. The details of scheduler is described in `docs/scheduler.md`.


