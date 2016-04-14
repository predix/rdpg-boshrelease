-	[Usage](https://github.com/starkandwayne/rdpg-boshrelease#usage-configuration--deployment)
-	[Setup with Cloud Foundry as service broker](https://github.com/starkandwayne/rdpg-boshrelease/blob/master/docs/cloudfoundry.md)
-	[Monitoring](https://github.com/starkandwayne/rdpg-boshrelease/blob/master/docs/monitoring.md)
-	[Debugging & QA](https://github.com/starkandwayne/rdpg-boshrelease#debugging--qa)

# Releases

## Current Release Overview

Changes in this release include:
 - Change to allow backups to work on databases larger than the available RAM

## Previous Release Overview
v0.2.64
 - Create two new scheduled tasks: ClearStuckTasks and Reconfigure (for pgBouncer)
 - Making some of the acceptance tests less verbose, bosh errands have a limit on the size of the output
 - Added the ability to specify additional extensions from the deployment manifest per cluster group.  Note that 'btree_gist', 'pg_stat_statements', 'uuid-ossp', 'hstore', 'pg_trgm' are  already added by default. The following extensions are available:
  - postgis
  - ltree
  - tablefunc
  - isn
  - cube
  - btree_gin
  - dict_int
  - chkpass
  - unaccent
  - intagg
  - tsearch2
  - dblink
  - pgrowlocks
  - earthdistance
  - fuzzystrmatch
  - intarray
  - postgres_fdw
 - Added pgcrypto as a default to all future user databases created in PrecreateDatabase, also added to acceptance tests

v0.2.62
 - Custom file retention policies are createable.
 - File retention is now enforced on the local filesystem and for S3 storage.
 - Endpoints were created to view file retention policies.
 - Files can be copied to S3 from an endpoint.
 - Automated restores are now supported for non-BDR databases.
 - Automated restores can be requested from the admin API.
 - Endpoints were created to generate diffs for which files are between local and s3.
 - Bug Fix: PrecreateDatabases will no longer create databases indefinitely if connection to the rdpg database is lost.
 - Bug Fix: Checking remote backups will no longer spin indefinitely if the number of backups exceeds 1000.

v0.2.61
 - Added EnforceRemoteFileRetention scheduled task to migration

v0.2.60
 - Added "num_user_databases" and "num_limit_databases" to admin stats API endpoint
 - Added `/stats/locks/{database}` endpoint for monitoring
 - Bug Fix: ReconcileAvailableDatabases was not copying the cluster_service column

v0.2.59
 - Added task "BackupAllDatabases" to do a hourly pg_dumpall
 - Added S3 download task
 - Added greater S3 support for backup/restore functions
 - Added task "RestoreDatabaseFromFile"
 - Added backup tests
 - Added additional global variables to allow for better customization

v0.2.58
 - Bug Fix: Decommission for SOLO drops connections so the database can be dropped
 - Bug Fix: PrecreateDatabases now ignores inactive databases from its counts when determining the number of databases to repopulate the pool
 - Set default backup schedule to 1 hour for all databases
 - Backups are now combined into 1 file with the data and schema, the bdr schema is still ignored

v0.2.56
 - Added extension "pg_trgm" to newly created databases
 - Modified default set of service plans to include `shared-nr` which is standard PostgreSQL 9.4.5

v0.2.55
 - Added support for multiple services & plans allowing different types of replication
 - Added migration for S3 file copy for existing deployments
 - Raised ulimit default values for pgbdr, pgbouncer and haproxy to match consul

v0.2.47-54 (skipped)

v0.2.46
 - Fix issue with instances that were decommission but not ineffective
 - Allowed stats endpoint to use db_pass

v0.2.45 (skipped)

v0.2.44
 - Bugfixes
 - Added stats endpoint
 - Added ability to copy backups to S3.  Refer to documentation in `docs/backups.md` for more information.
 - Added default file retention policies for backups.  If S3 is configured backups are removed from local disk if the copy was successful.  If S3 is not configured then the last 48 hours of backups are kept.
 - Cleanup of Tasks, Schedules and Configs which now have inserts from corresponding structs

v0.2.42
 - Patch for acceptance tests to check bdr replication to ignore retired databases

v0.2.41
 - Patch for API to remove newline character which isn't compatible with json

v0.2.40
 - Patch for acceptance tests to ignore retired databases

v0.2.39
 - Updated Go Language runtime to version v1.5
 - Updated RDPGD code to support soft and hard capacity limits
 - Updated RDPGD code to support RDPG CFSB deprovisioning
 - Updated documentation to current levels

v0.2.38
 - Added small sleep for migration tests to prevent duplicate inserts
 - Documentation changes
 - Added new acceptance test for Consul
 - Modified rdpgd-dev to properly handle symbolic links for go convey

v0.2.37
 - Modified Consul to accept a spec value for datacenter.
 - Added acceptance tests for Consul and datacenter changes.
 - Added PGBDR DSN value for manifestIP, consulDNS, or manual IP or DNS entry
 - Bug fixes

v0.2.36
 - Bug fixes: Fix for pgBouncer stuck tasks during decommission
 - Change to default service broker plan name, changed from "rdpg" to "postgres", only for new deployments

v0.2.35
 - Bug fixes: Master cluster did not receive migration script to create rdpg backup
 - Bug fixes: Acceptance tests weren't closing connections properly
 - Documentation additions

v0.2.34
 - Prevent duplicate scheduled tasks and configs during an upgrade
 - Added rdpg system database backup of ``--global-only` option and documentation

v0.2.33
 - Fixed bug with the service broker's unbind endpoint

v0.2.32
 - New acceptance test to detect broken bdr databases
 - Additional acceptance tests to detect stuck or malformed tasks
 - Initial schedule of backup of rdpg system database on each cluster

v0.2.31
 - ServiceMatrix Implementation
 - Acceptance Tests now test all clusters without hard-coding cluster names

v0.2.30
 - Temporary rollback of Consul datacenter property changes from 0.2.28 and 0.2.29
 - Critical bugfix so the pgbouncer reconfigure task is properly inserted and run

v0.2.29
 - Modified Consul to accept a spec value for datacenter.
 - Added acceptance tests for Consul and datacenter changes.
 - Bug fixes

v0.2.28
 - Modified Consul to accept a spec value for datacenter.
 - Added acceptance tests for Consul and datacenter changes.

v0.2.27
 - Additional notes on fixing BDR databases with broken replication.
 - Modification to deployment manifest and pgbdr job to support 100+ user databases

v0.2.26
 - Additional migration tasks were added to support the creation of scheduled tasks that would be done by a bootstrap for a new deployment

v0.2.25
 - Moved creation of extensions to post-bootstrap position

v0.2.24
 - Adding retry logic when creating extensions, previously prevented upgrades when redeploying with BOSH
 - Updated to bdr version

v0.2.23: Changes in this release include:
 - Scheduling of a default set of backups for all user databases.
 - A scheduled task which performs backups at regular intervals.
 - Tracking of backup history.
 - Define if a scheduled tasks should run on a `read`, `write`, or `any` node within a cluster.
 - A scheduled task which will delete old rows in the backup history.
 - Additional acceptance tests for backups, the acceptance-tests.md was updated to reflect these changes.
 - Migration scripts to migrate deployments from previous to this version's rdpg schema definition and defaults in rdpg.config.
 - A `backups.md` file describing the current functionality of the backups was added to the `rdpg-boshrelease/docs` folder.
