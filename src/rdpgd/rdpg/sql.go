package rdpg

//uuid_generate_v1mc(), // uuid-ossp

//SQL - contains db objects which will be created during bootstraps
var SQL = map[string]string{
	"postgres_schemas": `
CREATE SCHEMA IF NOT EXISTS rdpg;
  `,
	"rdpg_schemas": `
CREATE SCHEMA IF NOT EXISTS rdpg;
CREATE SCHEMA IF NOT EXISTS cfsb;
CREATE SCHEMA IF NOT EXISTS tasks;
CREATE SCHEMA IF NOT EXISTS backups;
CREATE SCHEMA IF NOT EXISTS metrics;
CREATE SCHEMA IF NOT EXISTS audit;
`,
	"create_table_backups_file_history": `
CREATE TABLE IF NOT EXISTS backups.file_history (
  id               BIGSERIAL PRIMARY KEY NOT NULL,
  cluster_id        TEXT NOT NULL,
  dbname            TEXT NOT NULL,
  node              TEXT NOT NULL,
  file_name         TEXT NOT NULL,
  action            TEXT NOT NULL,
  status            TEXT NOT NULL,
  params            json DEFAULT '{}'::json,
  created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  duration          INT,
  removed_at        TIMESTAMP
);
`,
	"create_table_backups_retention_rules": `
CREATE TABLE IF NOT EXISTS backups.retention_rules (
	dbname					 TEXT,
	hours						 FLOAT NOT NULL,
	is_remote_rule	 BOOLEAN,
	PRIMARY KEY(dbname, is_remote_rule)
);
`,
	"create_table_cfsb_services": `
CREATE TABLE IF NOT EXISTS cfsb.services (
  id               BIGSERIAL PRIMARY KEY NOT NULL,
  service_id       TEXT UNIQUE NOT NULL DEFAULT gen_random_uuid(),
  name             TEXT NOT NULL,
  description      TEXT NOT NULL,
  bindable         BOOLEAN NOT NULL DEFAULT true,
  dashboard_client json DEFAULT '{}'::json,
  created_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  effective_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  ineffective_at   TIMESTAMP
);
`,
	"create_table_cfsb_plans": `
CREATE TABLE IF NOT EXISTS cfsb.plans (
  id              BIGSERIAL    PRIMARY KEY NOT NULL,
  service_id      TEXT NOT NULL,
  plan_id         TEXT DEFAULT gen_random_uuid(),
	cluster_service TEXT NOT NULL,
  name            TEXT,
  description     TEXT,
  free            BOOLEAN   DEFAULT true,
  created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  effective_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  ineffective_at  TIMESTAMP
);
`,
	"create_table_cfsb_instances": `
CREATE TABLE IF NOT EXISTS cfsb.instances (
  id                BIGSERIAL PRIMARY KEY NOT NULL,
  cluster_service   TEXT NOT NULL,
  cluster_id        TEXT NOT NULL,
  instance_id       TEXT,
  service_id        TEXT,
  plan_id           TEXT,
  organization_id   TEXT,
  space_id          TEXT,
  dbname            TEXT NOT NULL UNIQUE,
  dbuser            TEXT NOT NULL,
  dbpass            TEXT NOT NULL,
  created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  effective_at      TIMESTAMP,
  ineffective_at    TIMESTAMP,
  decommissioned_at TIMESTAMP
);`,
	"create_table_cfsb_bindings": `
CREATE TABLE IF NOT EXISTS cfsb.bindings (
  id             BIGSERIAL PRIMARY KEY NOT NULL,
  instance_id    TEXT      NOT NULL,
  binding_id     TEXT      NOT NULL,
  created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  effective_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  ineffective_at TIMESTAMP
);`,
	"create_table_cfsb_credentials": `
CREATE TABLE IF NOT EXISTS cfsb.credentials (
  id             BIGSERIAL PRIMARY KEY NOT NULL,
  instance_id    TEXT      NOT NULL,
  binding_id     TEXT      NOT NULL,
  host           TEXT,
  port           TEXT,
  dbuser         TEXT,
  dbpass         TEXT,
  dbname         TEXT,
  created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  effective_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  ineffective_at TIMESTAMP
);`,
	"create_table_rdpg_consul_watch_notifications": `
CREATE TABLE IF NOT EXISTS rdpg.consul_watch_notifications (
  id BIGSERIAL NOT NULL,
  host TEXT NOT NULL,
  msg TEXT NOT NULL,
  created_at TIMESTAMP DEFAULT NOW(),
  CONSTRAINT consul_watch_notification_pkey PRIMARY KEY (id, host)
);`,
	"create_table_rdpg_events": `
CREATE TABLE IF NOT EXISTS rdpg.events (
  id BIGSERIAL NOT NULL PRIMARY KEY,
  host TEXT NOT NULL,
  key TEXT NOT NULL,
  msg TEXT NOT NULL,
  created_at TIMESTAMP DEFAULT NOW()
);`,
	"create_table_tasks_tasks": `
CREATE TABLE IF NOT EXISTS tasks.tasks (
  id BIGSERIAL NOT NULL PRIMARY KEY,
  cluster_id TEXT NOT NULL,
  cluster_service TEXT NOT NULL,
  node TEXT NOT NULL DEFAULT '*',
  role TEXT NOT NULL,
  action TEXT NOT NULL,
  data TEXT NOT NULL,
  ttl INTEGER NOT NULL DEFAULT 3600,
  node_type TEXT NOT NULL DEFAULT 'any',
  locked_by TEXT,
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  processing_at TIMESTAMP
);`,
	"create_table_tasks_schedules": `
CREATE TABLE IF NOT EXISTS tasks.schedules (
  id BIGSERIAL NOT NULL PRIMARY KEY,
  cluster_id TEXT NOT NULL,
	cluster_service TEXT NOT NULL,
  role TEXT NOT NULL,
  action TEXT NOT NULL,
  data TEXT NOT NULL DEFAULT '',
  frequency INTERVAL NOT NULL DEFAULT '1 hour'::interval,
  ttl INT NOT NULL DEFAULT 3600,
  node_type TEXT NOT NULL DEFAULT 'any',
  enabled BOOLEAN NOT NULL DEFAULT true,
  last_scheduled_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);`,
	"create_table_rdpg_config": `
CREATE TABLE IF NOT EXISTS rdpg.config (
  cluster_id TEXT NOT NULL,
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL,
  updated_at TIMESTAMP,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);`,
	"insert_default_cfsb_services": `
INSERT INTO cfsb.services (name,description,bindable,dashboard_client)
VALUES
  ('postgres', 'Reliable PostgrSQL Service', true, '{}') ;
`,
	"insert_default_cfsb_plans": `
INSERT INTO cfsb.plans (service_id,name,description,free,cluster_service)
VALUES
 ((SELECT service_id FROM cfsb.services WHERE name='postgres' LIMIT 1),'shared-nr',  'A PostgreSQL database with no replication on a shared server.', true, 'postgresql'),
 ((SELECT service_id FROM cfsb.services WHERE name='postgres' LIMIT 1),'shared',     'A Reliable PostgreSQL database on a shared server.', true, 'pgbdr');
`,
	"create_function_rdpg_disable_database": `
CREATE OR REPLACE FUNCTION rdpg.disable_database(name text) RETURNS VOID
AS $FUNC$
-- NOTE: This may only be run on the 'postgres' datbase
DECLARE
  r RECORD;
BEGIN
  IF name IN ('postgres','rdpg','template0','template1')
  THEN RETURN;
  END IF;

  UPDATE pg_database
    SET datallowconn = 'false'
    WHERE datname = name;

  EXECUTE 'ALTER DATABASE ' || name || ' OWNER TO rdpg;';

  PERFORM pg_terminate_backend(pg_stat_activity.pid)
    FROM pg_stat_activity
    WHERE pg_stat_activity.datname = name
    AND pid <> pg_backend_pid();

  PERFORM pg_sleep(1);

  FOR r IN
    SELECT slot_name
    FROM pg_replication_slots
    WHERE database = name
  LOOP
    PERFORM pg_drop_replication_slot(r.slot_name);
  END LOOP;
END;
$FUNC$ LANGUAGE plpgsql;
`,
}
