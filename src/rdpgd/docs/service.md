# Service

The service clusters provide database as a service to clients. It has both user databases and limited administrative databases. Each service cluster has two nodes, one of which is selected internally as write master and the other is replication.

The service cluster mainly has the following parts:

* User Databases
* Administrative Databases
* Scheduler

## User Databases

On each service cluster, a database pool of a given size is maintained for clients to use. Each database will have a user and appropriate permissions. The user databases are replicated in the two nodes within the same service cluster. One user database (eg. service instance) can be bound to multiple apps.

## Administrative Databases

Each service cluster has administrative databases which keep records of all user databases. Once a user database is created, a record will be inserted to administrative database and when user database is assigned/disabled, the  administrative databases will be updated to reflect the changes. The service cluster's local administrative database records are also used to compare with records in administrative databases in management cluster, if they are not consistent, then the two are reconciled.

## Scheduler

Each service cluster has a scheduler which will schedule background tasks such as pre-creation and maintenance of databases. For details, please see `docs/scheduler.md`.
