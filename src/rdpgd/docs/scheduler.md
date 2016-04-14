# Scheduler

The scheduler exists to schedule the running of background tasks such as:

* Reconciliation (Management)
* Database Creation (Service)
* Database Maintenance (Both)
* Database Decommissioning (Service)
* Database Backups (Both)

Both the management cluster and each service cluster all have their own scheduler.
The scheduler shedules tasks at a certain given frequency and create tasks in the cluster's task queue.
The scheduler uses a `last_schedued_at` timestamp together with a `frequency` interval to keep track of when the given scheduler should be run. Eg. If the `last_scheduled_at` plus the `frequency` exceeds the current timestamp then the scheduler is selected to run.

(Note that this method only ensures that each tasks scheduler will be run at timestamps of a given minimum period apart and it does not guarantee that a scheduler will be run at a specific point in time.)

For both management cluster and servie cluster, the tasks that the scheduler schedules are as follows.

## Management Cluster Scheduler

What follows is the details of the scheduled background tasks listed above specific to the maintenance cluster.

### Reconciliation 

The act of reconciliation is to ensure that the "known state" of the system between the management cluster and the service clusters are in sync and accurate.

#### Reconcile All Databases

Responsible for fetching the list of all databases from all service clusters and 
comparing them with the list on the management cluster. If any inconsistencies 
are found then it reconciles the differences.

#### Reconcile Available Databases

Responsible for fetching the list of currently available databases from each 
service cluster and comparing against the management cluster's master list to 
find any missing/incomplete records and reconcile accordingly.

On the management cluster the scheduler performs the following (default) schedules.

## Service Cluster Scheduler

### Database Creation 

Responsible for ensuring that a specific pool size of pre-created databases is maintained.

## All Clusters

The following schedules are run on all clusters.

### Database Maintenance

Responsible for scheduling Vacuum tasks to run which reclaim storage occupied by dead tuples within the databases on the cluster.

### Database Backups

Responsible for scheduling backup tasks to run. On the managlement cluster these tasks will
notify service clusters to schedule a task to backup a specific database on their cluster.
Service clusters schedule tasks to perform the backups.

