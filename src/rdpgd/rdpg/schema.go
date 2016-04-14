package rdpg

import (
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"database/sql"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq" //Used by sqlx
	"github.com/starkandwayne/rdpgd/config"
	"github.com/starkandwayne/rdpgd/globals"
	"github.com/starkandwayne/rdpgd/log"
	"github.com/starkandwayne/rdpgd/pg"
	"github.com/starkandwayne/rdpgd/tasks"
	"github.com/starkandwayne/rdpgd/utils/backup"
	"github.com/starkandwayne/rdpgd/utils/rdpgs3"
)

// InitSchema - Initialize the rdpg system database schemas.
func (r *RDPG) InitSchema() (err error) {
	log.Trace(fmt.Sprintf(`rdpg.RDPG<%s>#InitSchema() Initializing Schema for Cluster...`, ClusterID))

	var name string
	p := pg.NewPG(`127.0.0.1`, pgPort, `rdpg`, `rdpg`, pgPass)
	db, err := p.Connect()
	if err != nil {
		log.Error(fmt.Sprintf(`rdpg.RDPG#InitSchema(%s) Opening db connection ! %s`, globals.ServiceRole, err))
		return err
	}
	//defer db.Close()
	if globals.ClusterService == "pgbdr" {
		_, err = db.Exec(`SELECT bdr.bdr_node_join_wait_for_ready();`)
		if err != nil {
			log.Error(fmt.Sprintf(`RDPG#initSchema() bdr.bdr_node_join_wait_for_ready ! %s`, err))
		}
	}

	ddlLockRE := regexp.MustCompile(`cannot acquire DDL lock|Database is locked against DDL operations`)
	for { // Retry loop for acquiring DDL schema lock.
		log.Trace(fmt.Sprintf("RDPG#initSchema() SQL[%s]", "rdpg_schemas"))
		_, err = db.Exec(SQL["rdpg_schemas"])
		if err != nil {
			if ddlLockRE.MatchString(err.Error()) {
				log.Trace("RDPG#initSchema() DDL Lock not available, waiting...")
				time.Sleep(1 * time.Second)
				continue
			}
			log.Error(fmt.Sprintf("RDPG#initSchema() ! %s", err))
		}
		break
	}

	keys := []string{
		"create_table_cfsb_services",
		"create_table_cfsb_plans",
		"create_table_cfsb_instances",
		"create_table_cfsb_bindings",
		"create_table_cfsb_credentials",
		"create_table_tasks_schedules",
		"create_table_tasks_tasks",
		"create_table_rdpg_consul_watch_notifications",
		"create_table_rdpg_events",
		"create_table_rdpg_config",
		"create_table_backups_file_history",
		"create_table_backups_retention_rules",
	}
	for _, key := range keys {
		k := strings.Split(strings.Replace(strings.Replace(key, "create_table_", "", 1), "_", ".", 1), ".")
		sq := fmt.Sprintf(`SELECT table_name FROM information_schema.tables where table_schema='%s' AND table_name='%s';`, k[0], k[1])

		log.Trace(fmt.Sprintf("RDPG#initSchema() %s", sq))
		if err := db.QueryRow(sq).Scan(&name); err != nil {
			if err == sql.ErrNoRows {
				log.Trace(fmt.Sprintf("RDPG#initSchema() SQL[%s]", key))
				_, err = db.Exec(SQL[key])
				if err != nil {
					log.Error(fmt.Sprintf("RDPG#initSchema() ! %s", err))
				}
			} else {
				log.Error(fmt.Sprintf("rdpg.initSchema() ! %s", err))
			}
		}
	}

	err = insertDefaultSchedules(db)
	if err != nil {
		log.Error(fmt.Sprintf(`rdpg.initSchema() service task schedules ! %s`, err))
	}

	log.Trace("RDPG#initSchema(): DefaultConfig, newDefaultConfig.Add().")
	newDefaultConfig := config.DefaultConfig{Key: `BackupsPath`, ClusterID: ClusterID, Value: globals.LocalBackupPath}
	backup.AddBackupPathConfig(&newDefaultConfig)

	newDefaultConfig = config.DefaultConfig{Key: `BackupPort`, ClusterID: ClusterID, Value: `7432`}
	newDefaultConfig.Add()

	if globals.ClusterService == "pgbdr" {
		newDefaultConfig = config.DefaultConfig{Key: `pgDumpBinaryLocation`, ClusterID: ClusterID, Value: `/var/vcap/packages/pgbdr/bin/pg_dump`}
		newDefaultConfig.Add()
	} else {
		newDefaultConfig = config.DefaultConfig{Key: `pgDumpBinaryLocation`, ClusterID: ClusterID, Value: `/var/vcap/packages/postgresql-9.4/bin/pg_dump`}
		newDefaultConfig.Add()
	}
	newDefaultConfig = config.DefaultConfig{Key: `defaultDaysToKeepFileHistory`, ClusterID: ClusterID, Value: `180`}
	newDefaultConfig.Add()

	// TODO: Move initial population of services out of rdpg to Admin API.
	if err := db.QueryRow(`SELECT name FROM cfsb.services WHERE name IN ('postgres', 'rdpg') LIMIT 1;`).Scan(&name); err != nil {
		if err == sql.ErrNoRows {
			if _, err = db.Exec(SQL["insert_default_cfsb_services"]); err != nil {
				log.Error(fmt.Sprintf("rdpg.initSchema(insert_default_cfsb_services) %s", err))
			}
		} else {
			log.Error(fmt.Sprintf("rdpg.initSchema() ! %s", err))
		}
	}

	// TODO: Move initial population of services out of rdpg to Admin API.
	if err = db.QueryRow(`SELECT name FROM cfsb.plans WHERE name='shared' LIMIT 1;`).Scan(&name); err != nil {
		if err == sql.ErrNoRows {
			if _, err = db.Exec(SQL["insert_default_cfsb_plans"]); err != nil {
				log.Error(fmt.Sprintf("rdpg.initSchema(insert_default_cfsb_plans) %s", err))
			}
		} else {
			log.Error(fmt.Sprintf("rdpg.initSchema() ! %s", err))
		}
	}
	db.Close()

	cluster, err := NewCluster(ClusterID, r.ConsulClient)
	for _, pg := range cluster.Nodes {
		pg.PG.Set(`database`, `postgres`)

		db, err := pg.PG.Connect()
		if err != nil {
			log.Error(fmt.Sprintf("RDPG#DropUser(%s) %s ! %s", name, pg.PG.IP, err))
		}

		log.Trace(fmt.Sprintf("RDPG#initSchema() SQL[%s]", "postgres_schemas"))
		_, err = db.Exec(SQL["postgres_schemas"])
		if err != nil {
			log.Error(fmt.Sprintf("RDPG#initSchema() ! %s", err))
		}

		keys = []string{ // These are for the postgres database only
			"create_function_rdpg_disable_database",
		}
		for _, key := range keys {
			k := strings.Split(strings.Replace(strings.Replace(key, "create_function_", "", 1), "_", ".", 1), ".")
			// TODO: move this into a pg.PG#FunctionExists()
			sq := fmt.Sprintf(`SELECT routine_name FROM information_schema.routines WHERE routine_type='FUNCTION' AND routine_schema='%s' AND routine_name='%s';`, k[0], k[1])

			log.Trace(fmt.Sprintf("RDPG#initSchema() %s", sq))
			if err := db.QueryRow(sq).Scan(&name); err != nil {
				if err == sql.ErrNoRows {
					log.Trace(fmt.Sprintf("RDPG#initSchema() SQL[%s]", key))
					_, err = db.Exec(SQL[key])
					if err != nil {
						log.Error(fmt.Sprintf("rdpg.RDPG#initSchema() %s", err))
					}
				} else {
					log.Error(fmt.Sprintf("rdpg.initSchema() %s", err))
					db.Close()
					return err
				}
			}
		}

		db.Close()
	}

	err = columnMigrations()
	if err != nil {
		log.Error(fmt.Sprintf("rdpg.RDPG#initSchema() columnMigrations() %s", err))
	}

	log.Info(fmt.Sprintf(`rdpg.RDPG<%s>#InitSchema() Schema Initialized.`, ClusterID))
	return nil
}

/*
Default Schedules, general then by role.
*/
func insertDefaultSchedules(db *sqlx.DB) (err error) {
	log.Trace(fmt.Sprintf(`rdpg.insertDefaultSchedules(%s)...`, globals.ServiceRole))

	schedules := []tasks.Schedule{}
	if globals.ClusterService == "pgbdr" {
		sq := fmt.Sprintf(`INSERT INTO tasks.schedules (cluster_id,role,action,data,frequency,enabled,node_type,cluster_service) SELECT '%s','all','Vacuum','tasks.tasks','5 minutes'::interval, true,'read','%s' WHERE NOT EXISTS (SELECT id FROM tasks.schedules WHERE action = 'Vacuum' AND node_type = 'read') `, ClusterID, globals.ClusterService)
		log.Trace(fmt.Sprintf(`rdpg.insertDefaultSchedules(%s) > %s`, globals.ServiceRole, sq))
		re := regexp.MustCompile(`global sequence.*not initialized yet`)
		for { // Ensure that we wait for global sequence initialization (post create)
			_, err = db.Exec(sq)
			if err != nil {
				if re.MatchString(err.Error()) {
					time.Sleep(1 * time.Second)
					continue
				} else {
					log.Error(fmt.Sprintf(`rdpg.insertDefaultSchedules() service task schedules ! %s`, err))
					return err
				}
			}
			break
		}

		schedules = append(schedules, tasks.Schedule{ClusterID: ClusterID, ClusterService: globals.ClusterService, Role: `all`, Action: `Vacuum`, Data: `tasks.tasks`, NodeType: `write`, Frequency: `5 minutes`, Enabled: true})
		schedules = append(schedules, tasks.Schedule{ClusterID: ClusterID, ClusterService: globals.ClusterService, Role: `all`, Action: `DeleteBackupHistory`, Data: ``, NodeType: `read`, Frequency: `1 hour`, Enabled: true})
		schedules = append(schedules, tasks.Schedule{ClusterID: ClusterID, ClusterService: globals.ClusterService, Role: `all`, Action: `BackupDatabase`, Data: `rdpg`, NodeType: `read`, Frequency: `1 hour`, Enabled: true})
		schedules = append(schedules, tasks.Schedule{ClusterID: ClusterID, ClusterService: globals.ClusterService, Role: `all`, Action: `EnforceFileRetention`, Data: ``, NodeType: `read`, Frequency: `1 hour`, Enabled: true})
		schedules = append(schedules, tasks.Schedule{ClusterID: ClusterID, ClusterService: globals.ClusterService, Role: `all`, Action: `EnforceFileRetention`, Data: ``, NodeType: `write`, Frequency: `1 hour`, Enabled: true})
		schedules = append(schedules, tasks.Schedule{ClusterID: ClusterID, ClusterService: globals.ClusterService, Role: `all`, Action: `ClearStuckTasks`, Data: ``, NodeType: `read`, Frequency: `1 hour`, Enabled: true})
		schedules = append(schedules, tasks.Schedule{ClusterID: ClusterID, ClusterService: globals.ClusterService, Role: `all`, Action: `ClearStuckTasks`, Data: ``, NodeType: `write`, Frequency: `1 hour`, Enabled: true})

		if strings.ToUpper(os.Getenv(`RDPGD_S3_BACKUPS`)) == "ENABLED" {
			schedules = append(schedules, tasks.Schedule{ClusterID: ClusterID, ClusterService: globals.ClusterService, Role: `all`, Action: `FindFilesToCopyToS3`, Data: ``, NodeType: `read`, Frequency: `5 minutes`, Enabled: true})
			schedules = append(schedules, tasks.Schedule{ClusterID: ClusterID, ClusterService: globals.ClusterService, Role: `all`, Action: `FindFilesToCopyToS3`, Data: ``, NodeType: `write`, Frequency: `5 minutes`, Enabled: true})
		} else {
			schedules = append(schedules, tasks.Schedule{ClusterID: ClusterID, ClusterService: globals.ClusterService, Role: `all`, Action: `FindFilesToCopyToS3`, Data: ``, NodeType: `read`, Frequency: `5 minutes`, Enabled: false})
			schedules = append(schedules, tasks.Schedule{ClusterID: ClusterID, ClusterService: globals.ClusterService, Role: `all`, Action: `FindFilesToCopyToS3`, Data: ``, NodeType: `write`, Frequency: `5 minutes`, Enabled: false})
		}

		if globals.ServiceRole == "manager" {
			schedules = append(schedules, tasks.Schedule{ClusterID: ClusterID, ClusterService: globals.ClusterService, Role: `manager`, Action: `ReconcileAvailableDatabases`, Data: ``, NodeType: `read`, Frequency: `1 minute`, Enabled: true})
			schedules = append(schedules, tasks.Schedule{ClusterID: ClusterID, ClusterService: globals.ClusterService, Role: `manager`, Action: `ReconcileAllDatabases`, Data: ``, NodeType: `read`, Frequency: `5 minutes`, Enabled: true})
		}

		if globals.ServiceRole == "service" {
			schedules = append(schedules, tasks.Schedule{ClusterID: ClusterID, ClusterService: globals.ClusterService, Role: `service`, Action: `ScheduleNewDatabaseBackups`, Data: ``, NodeType: `write`, Frequency: `1 minute`, Enabled: true})
			schedules = append(schedules, tasks.Schedule{ClusterID: ClusterID, ClusterService: globals.ClusterService, Role: `service`, Action: `PrecreateDatabases`, Data: ``, NodeType: `write`, Frequency: `1 minute`, Enabled: true})
			schedules = append(schedules, tasks.Schedule{ClusterID: ClusterID, ClusterService: globals.ClusterService, Role: `service`, Action: `DecommissionDatabases`, Data: ``, NodeType: `write`, Frequency: `15 minutes`, Enabled: true})
			schedules = append(schedules, tasks.Schedule{ClusterID: ClusterID, ClusterService: globals.ClusterService, Role: `service`, Action: `Reconfigure`, Data: `pgbouncer`, NodeType: `read`, Frequency: `1 hour`, Enabled: true})
			schedules = append(schedules, tasks.Schedule{ClusterID: ClusterID, ClusterService: globals.ClusterService, Role: `service`, Action: `Reconfigure`, Data: `pgbouncer`, NodeType: `write`, Frequency: `1 hour`, Enabled: true})

		}

	} else { // Currently else is specifically postgresql only... we'll have to move this to a switch statement later :)
		schedules = append(schedules, tasks.Schedule{ClusterID: ClusterID, ClusterService: globals.ClusterService, Role: `all`, Action: `Vacuum`, Data: `tasks.tasks`, NodeType: `write`, Frequency: `5 minutes`, Enabled: true})
		schedules = append(schedules, tasks.Schedule{ClusterID: ClusterID, ClusterService: globals.ClusterService, Role: `all`, Action: `DeleteBackupHistory`, Data: ``, NodeType: `write`, Frequency: `1 hour`, Enabled: true})
		schedules = append(schedules, tasks.Schedule{ClusterID: ClusterID, ClusterService: globals.ClusterService, Role: `all`, Action: `BackupDatabase`, Data: `rdpg`, NodeType: `write`, Frequency: `1 hour`, Enabled: true})
		schedules = append(schedules, tasks.Schedule{ClusterID: ClusterID, ClusterService: globals.ClusterService, Role: `all`, Action: `EnforceFileRetention`, Data: ``, NodeType: `write`, Frequency: `1 hour`, Enabled: true})
		schedules = append(schedules, tasks.Schedule{ClusterID: ClusterID, ClusterService: globals.ClusterService, Role: `all`, Action: `BackupAllDatabases`, Data: ``, NodeType: `write`, Frequency: `1 hour`, Enabled: true})
		schedules = append(schedules, tasks.Schedule{ClusterID: ClusterID, ClusterService: globals.ClusterService, Role: `all`, Action: `EnforceRemoteFileRetention`, Data: ``, NodeType: `write`, Frequency: `1 hour`, Enabled: rdpgs3.Configured})
		schedules = append(schedules, tasks.Schedule{ClusterID: ClusterID, ClusterService: globals.ClusterService, Role: `all`, Action: `ClearStuckTasks`, Data: ``, NodeType: `write`, Frequency: `1 hour`, Enabled: true})

		if rdpgs3.Configured {
			schedules = append(schedules, tasks.Schedule{ClusterID: ClusterID, ClusterService: globals.ClusterService, Role: `all`, Action: `FindFilesToCopyToS3`, Data: ``, NodeType: `write`, Frequency: `5 minutes`, Enabled: true})
		} else {
			schedules = append(schedules, tasks.Schedule{ClusterID: ClusterID, ClusterService: globals.ClusterService, Role: `all`, Action: `FindFilesToCopyToS3`, Data: ``, NodeType: `write`, Frequency: `5 minutes`, Enabled: false})
		}

		if globals.ServiceRole == "manager" {
			schedules = append(schedules, tasks.Schedule{ClusterID: ClusterID, ClusterService: globals.ClusterService, Role: `manager`, Action: `ReconcileAvailableDatabases`, Data: ``, NodeType: `write`, Frequency: `1 minute`, Enabled: true})
			schedules = append(schedules, tasks.Schedule{ClusterID: ClusterID, ClusterService: globals.ClusterService, Role: `manager`, Action: `ReconcileAllDatabases`, Data: ``, NodeType: `write`, Frequency: `5 minutes`, Enabled: true})
		}

		if globals.ServiceRole == "service" {
			schedules = append(schedules, tasks.Schedule{ClusterID: ClusterID, ClusterService: globals.ClusterService, Role: `service`, Action: `ScheduleNewDatabaseBackups`, Data: ``, NodeType: `write`, Frequency: `1 minute`, Enabled: true})
			schedules = append(schedules, tasks.Schedule{ClusterID: ClusterID, ClusterService: globals.ClusterService, Role: `service`, Action: `PrecreateDatabases`, Data: ``, NodeType: `write`, Frequency: `1 minute`, Enabled: true})
			schedules = append(schedules, tasks.Schedule{ClusterID: ClusterID, ClusterService: globals.ClusterService, Role: `service`, Action: `DecommissionDatabases`, Data: ``, NodeType: `write`, Frequency: `15 minutes`, Enabled: true})
			schedules = append(schedules, tasks.Schedule{ClusterID: ClusterID, ClusterService: globals.ClusterService, Role: `service`, Action: `Reconfigure`, Data: `pgbouncer`, NodeType: `write`, Frequency: `1 hour`, Enabled: true})
		}
	}

	for index := range schedules {
		err = schedules[index].Add()
		if err != nil {
			log.Error(fmt.Sprintf(`schema.insertDefaultSchedules() Schedule: ! %s`, err))
			continue
		}
	}

	return
}

/*
 columnMigrations migrates columns testing for conditions,
 eg. handle the migration of pre-existing environments.
*/
func columnMigrations() (err error) {
	p := pg.NewPG(`127.0.0.1`, pgPort, `rdpg`, `rdpg`, pgPass)
	p.Set(`database`, `rdpg`)

	db, err := p.Connect()
	if err != nil {
		log.Error(fmt.Sprintf(`rdpg.columnMigration() Could not open connection ! %s`, err))
		return
	}
	defer db.Close()

	sq := fmt.Sprintf(`SELECT constraint_name FROM information_schema.table_constraints WHERE table_name='instances' AND constraint_type='UNIQUE';`)
	log.Trace(fmt.Sprintf("rdpg.columnMigrations() %s", sq))
	var constraintName string
	if err = db.QueryRow(sq).Scan(&constraintName); err != nil {
		if err == sql.ErrNoRows {
			log.Trace(fmt.Sprintf("The instance table db name is not set UNIQUE constraints"))
			_, err = db.Exec(`ALTER TABLE cfsb.instances ADD CONSTRAINT instances_dbname_key UNIQUE (dbname)`)
			if err != nil {
				log.Error(fmt.Sprintf("rdpg.columnMigrations()%s", err))
				return
			}
		} else {
			log.Error(fmt.Sprintf("rdpg.columnMigrations() ! %s", err))
			return
		}
	}
	return
}
