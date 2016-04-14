package tasks

import (
	"encoding/json"
	"fmt"

	"github.com/starkandwayne/rdpgd/globals"
	"github.com/starkandwayne/rdpgd/utils/backup"

	"github.com/starkandwayne/rdpgd/log"
	"github.com/starkandwayne/rdpgd/pg"
)

type restoreParams struct {
	pgPsqlPath string `json:"psql_path"`
	pgPort     string `json:"pg_port"`
	dbname     string `json:"database_name"`
	fileName   string `json:"base_file_name"`
	node       string `json:"node"`
}

//Assumes task params are {"dbname":"test","fileName":"absolute path to the file"}

//RestoreDatabaseFromFile - Perform the restore of a backup file to a new clean database - ONLY SUPPORTING SOLO DBS FOR NOW...
func (t *Task) RestoreDatabaseFromFile() (err error) {

	taskParams := []byte(t.Data)
	b := restoreParams{}

	var dat map[string]interface{}
	if err := json.Unmarshal(taskParams, &dat); err != nil {
		log.Error(fmt.Sprintf("tasks.restoreDatabase() Could not JSON parse task.task.data value %s ! %s", t.Data, err))
	}
	b.dbname = dat["dbname"].(string)
	b.fileName = dat["fileName"].(string)

	log.Trace(fmt.Sprintf("tasks.restoreDatabase() Restoring database: %s on node: %s with file: %s", b.dbname, globals.MyIP, b.fileName))

	err = backup.ImportSqlFile(b.dbname, b.fileName)
	if err != nil {
		log.Error(fmt.Sprintf("tasks.restoreDatabase() Could not import file '%s' for database %s ! %s", b.fileName, b.dbname, err))
	}

	return
}

// CreateTestDB - Create an empty SOLO database called 'test'
func (t *Task) CreateTestDB() (err error) {

	dbname := "test"
	dbuser := "test"
	dbpass := "test"
	err = createDatabaseAndUser(dbname, dbuser, dbpass)
	return
}

func createDatabaseAndUser(dbname string, dbuser string, dbpass string) (err error) {
	//NOTE: swap in the current ClusterID for the hard coded value in the inserts, also do some sort of check to make sure this is a SOLO cluster

	clusterService := "postgresql"
	p := pg.NewPG(`127.0.0.1`, pgPort, `rdpg`, `rdpg`, pgPass)
	db, err := p.Connect()
	if err != nil {
		log.Error(fmt.Sprintf("tasks.Work() Failed connecting to %s err: %s", p.URI, err))
		return err
	}
	defer db.Close()

	err = p.CreateUser(dbuser, dbpass)
	if err != nil {
		log.Error(fmt.Sprintf("tasks.Task#createDatabaseAndUser(%s) CreateUser(%s) ! %s", dbname, dbuser, err))
		return err
	}

	err = p.CreateDatabase(dbname, dbuser)
	if err != nil {
		log.Error(fmt.Sprintf("tasks.Task#createDatabaseAndUser(%s) CreateDatabase(%s,%s) ! %s", dbname, dbname, dbuser, err))
		return err
	}

	if dbname == "test" {
		sq := fmt.Sprintf(`INSERT INTO cfsb.instances (cluster_service,cluster_id,instance_id,service_id,plan_id,organization_id,space_id,dbname,dbuser,dbpass,created_at,effective_at) VALUES ('%s','%s','i','s','p','o','s','%s','%s','%s',current_timestamp,current_timestamp);`, clusterService, ClusterID, dbname, dbuser, dbpass)
		log.Trace(fmt.Sprintf(`tasks.createDatabaseAndUser(%s) > %s`, dbname, sq))
		_, err = db.Query(sq)
		if err != nil {
			log.Error(fmt.Sprintf(`tasks.createDatabaseAndUser(%s) ! %s`, dbname, err))
			return err
		}
	}

	return
}

// CreateAndRestoreAllUserDatabases - Overarching task which will create and restore all user databases
func (t *Task) CreateAndRestoreAllUserDatabases() (err error) {

	// Select the list of active databases, their user/role, their password, and most recent db backup

	// For each row
	//		## Alternate Create
	//    PSQL="/var/vcap/packages/postgresql-9.4/bin/psql -p7432 -U vcap";
	//    $PSQL rdpg -c "INSERT INTO tasks.tasks (cluster_id, node, role, action, data, node_type, cluster_service) VALUES ('sc-pgbdr-m1-c0', '*', 'all', 'CreateTestDB', 'test', 'write', 'postgresql');"
	//
	//    ## Alternate restore
	//    PSQL="/var/vcap/packages/postgresql-9.4/bin/psql -p7432 -U vcap";
	//    fileName="/var/vcap/store/postgresql/restore/testdb.sql"
	//    $PSQL rdpg -c "INSERT INTO tasks.tasks (cluster_id, node, role, action, data, node_type, cluster_service) VALUES ('rdpgsc1', '*', 'all', 'RestoreDatabaseFromFile', '{\"dbname\":\"test\",\"fileName\":\"${fileName}\"}', 'write', 'postgresql');"
	// END

	// Other info
	//    ## Connect to the test db and verify restore
	//    dbname="test";dbuser="test";dbpass="test"
	//    PSQL="/var/vcap/packages/postgresql-9.4/bin/psql -h 127.0.0.1 -p7432 -U ${dbuser}"
	//    $PSQL ${dbname} -c "SELECT * FROM test.testtable;"
	//
	//    ## Alternate drop
	//    PSQL="/var/vcap/packages/postgresql-9.4/bin/psql -p7432 -U vcap";
	//    $PSQL rdpg -c "INSERT INTO tasks.tasks (cluster_id, node, role, action, data, node_type, cluster_service) VALUES ('sc-pgbdr-m1-c0', '*', 'all', 'DecommissionDatabase', 'test', 'write', 'postgresql');"

	return
}
