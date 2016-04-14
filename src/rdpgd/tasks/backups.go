package tasks

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"time"

	consulapi "github.com/hashicorp/consul/api"

	"github.com/starkandwayne/rdpgd/config"
	"github.com/starkandwayne/rdpgd/globals"
	"github.com/starkandwayne/rdpgd/history"
	"github.com/starkandwayne/rdpgd/log"
	"github.com/starkandwayne/rdpgd/utils/rdpgconsul"
	"github.com/starkandwayne/rdpgd/utils/rdpgpg"
)

type backupParams struct {
	pgDumpPath   string `json:"pg_dump_path"`
	pgPort       string `json:"pg_port"`
	basePath     string `json:"base_path"`
	databaseName string `json:"database_name"`
	baseFileName string `json:"base_file_name"`
	node         string `json:"node"`
}

//ScheduleNewDatabaseBackups - Responsible for adding any databases which are in
//cfsb.instances and aren't already scheduled in tasks.schedules
func (t *Task) ScheduleNewDatabaseBackups() (err error) {

	//SELECT active databases in cfsb.instances which aren't in tasks.schedules
	address := `127.0.0.1`
	sq := `SELECT name FROM ( (SELECT dbname AS name FROM cfsb.instances WHERE effective_at IS NOT NULL AND decommissioned_at IS NULL) EXCEPT (SELECT data AS name FROM tasks.schedules WHERE action = 'BackupDatabase' ) ) AS missing_databases; `
	listMissingDatabases, err := rdpgpg.GetList(address, sq)
	if err != nil {
		log.Error(fmt.Sprintf(`tasks.Task<%d>#ScheduleNewDatabaseBackups() Failed to load list of databases ! %s`, t.ID, err))
	}

	for _, databaseName := range listMissingDatabases {
		log.Trace(fmt.Sprintf("tasks.BackupDatabase() > Attempting to add %s", databaseName))

		nodeType := `write`
		if t.ClusterService == "pgbdr" {
			nodeType = `read`
		}
		newScheduledTask := Schedule{ClusterID: ClusterID, ClusterService: t.ClusterService, Role: `service`, Action: `BackupDatabase`, Data: databaseName, NodeType: nodeType, Frequency: `1 hour`, Enabled: true}
		err = newScheduledTask.Add()

	}
	return
}

//BackupDatabase - Perform a schema and database backup of a given database to local disk
func (t *Task) BackupDatabase() (err error) {
	b := backupParams{}

	//Make sure database actually exists first.
	b.databaseName = t.Data
	if b.databaseName != "rdpg" {
		address := `127.0.0.1`
		sq := fmt.Sprintf(`SELECT 1 FROM cfsb.instances WHERE effective_at IS NOT NULL AND decommissioned_at IS NULL AND dbname = '%s';`, b.databaseName)
		databasesWithThatName, err := rdpgpg.GetList(address, sq)
		if err != nil {
			log.Error(fmt.Sprintf("Tasks.BackupDatabase() utils/backup.GetList(%s, %s) Error trying to query for database.", address, b.databaseName))
			return err
		}
		if len(databasesWithThatName) == 0 {
			log.Error(fmt.Sprintf("Task.BackupDatabase() Attempt to back up non-existant or non-commissioned database with name: %s", b.databaseName))
			return errors.New("Database doesn't exist.")
		}
	}
	lockAcquired, sessID := acquireBackupLock(b.databaseName)
	if !lockAcquired {
		log.Warn("Aborting Backup: Unable to acquire database lock. Is another backup already in progress?")
		return errors.New("Unable to acquire database lock")
	}
	defer releaseBackupLock(b.databaseName, sessID)

	b.pgDumpPath, err = config.GetValue(`pgDumpBinaryLocation`)
	if err != nil {
		return err
	}
	b.pgPort, err = config.GetValue(`BackupPort`)
	if err != nil {
		return err
	}
	b.basePath, err = config.GetValue(`BackupsPath`)
	if err != nil {
		return err
	}
	b.node, err = rdpgconsul.GetNode()
	if err != nil {
		return err
	}
	b.baseFileName = getBaseFileName() //Use this to keep schema and data file names the same

	err = createTargetFolder(b.basePath + `/` + b.databaseName)
	if err != nil {
		log.Error(fmt.Sprintf("tasks.BackupDatabase() Could not create target folder %s ! %s", b.basePath, err))
		return err
	}

	schemaDataFileHistory, err := createSchemaAndDataFile(b)
	if err != nil {
		log.Error(fmt.Sprintf("tasks.BackupDatabase() Could not create schema and data file for database %s ! %s", b.databaseName, err))
		schemaDataFileHistory.Status = `error`
	}
	err = history.InsertBackupHistory(schemaDataFileHistory)

	if b.databaseName == `rdpg` {
		globalsFileHistory, err := createGlobalsFile(b)
		if err != nil {
			log.Error(fmt.Sprintf("tasks.BackupDatabase() Could not create globals file for database %s ! %s", b.databaseName, err))
			globalsFileHistory.Status = `error`
		}

		err = history.InsertBackupHistory(globalsFileHistory)

	}
	return
}

// createTargetFolder - On the os, create the backup folder if it doesn't exist
func createTargetFolder(fullPath string) (err error) {
	err = os.MkdirAll(fullPath, 0777)
	return err
}

// createSchemaFile - Create a pg backup file which contains the schema to recreate
// the user database.
func createSchemaFile(b backupParams) (f history.BackupFileHistory, err error) {

	start := time.Now()
	f.Duration = 0
	f.Status = `ok`
	f.BackupFile = b.baseFileName + ".schema"
	f.BackupPathAndFile = b.basePath + "/" + b.databaseName + "/" + f.BackupFile
	f.DBName = b.databaseName
	f.Node = b.node

	_, err = exec.Command(b.pgDumpPath, "-p", b.pgPort, "-U", "vcap", "-f", f.BackupPathAndFile, "-c", "-s", "-N", `"bdr"`, b.databaseName).CombinedOutput()
	if err != nil {
		log.Error(fmt.Sprintf(`tasks.createSchemaFile() Error running pg_dump command for: %s file: %s ! %s`, b.databaseName, f.BackupPathAndFile, err))
		return
	}

	f.Duration = int(time.Since(start).Seconds())
	return
}

// createSchemaAndDataFile - Create a pg backup file which contains both the
// data and schema
func createSchemaAndDataFile(b backupParams) (f history.BackupFileHistory, err error) {

	start := time.Now()
	f.Duration = 0
	f.Status = `ok`
	f.BackupFile = b.baseFileName + ".sql"
	f.BackupPathAndFile = b.basePath + "/" + b.databaseName + "/" + f.BackupFile
	f.DBName = b.databaseName
	f.Node = b.node

	_, err = exec.Command(b.pgDumpPath, "-p", b.pgPort, "-U", "vcap", "-f", f.BackupPathAndFile, "-b", "-N", `"bdr"`, b.databaseName).CombinedOutput()
	if err != nil {
		log.Error(fmt.Sprintf(`tasks.createSchemaAndDataFile() Error running pg_dump command for: %s file: %s ! %s`, b.databaseName, f.BackupPathAndFile, err))
		return
	}

	f.Duration = int(time.Since(start).Seconds())
	return

}

// createDataFile - Create a pg backup file which contains only data which can be
// copied back to an existing schema
func createDataFile(b backupParams) (f history.BackupFileHistory, err error) {

	start := time.Now()
	f.Duration = 0
	f.Status = `ok`
	f.BackupFile = b.baseFileName + ".data"
	f.BackupPathAndFile = b.basePath + "/" + b.databaseName + "/" + f.BackupFile
	f.DBName = b.databaseName
	f.Node = b.node

	_, err = exec.Command(b.pgDumpPath, "-p", b.pgPort, "-U", "vcap", "-f", f.BackupPathAndFile, "-a", "-b", "-N", `"bdr"`, b.databaseName).CombinedOutput()
	if err != nil {
		log.Error(fmt.Sprintf(`tasks.createDataFile() Error running pg_dump command for: %s file: %s ! %s`, b.databaseName, f.BackupPathAndFile, err))
		return
	}

	f.Duration = int(time.Since(start).Seconds())
	return

}

// createGlobalsFile - Create a pg backup file which contains only globals (roles/logins)
func createGlobalsFile(b backupParams) (f history.BackupFileHistory, err error) {

	start := time.Now()
	f.Duration = 0
	f.Status = `ok`
	f.BackupFile = b.baseFileName + ".globals"
	f.BackupPathAndFile = b.basePath + "/" + b.databaseName + "/" + f.BackupFile
	f.DBName = b.databaseName
	f.Node = b.node

	pgDumpallPath := b.pgDumpPath + `all`

	_, err = exec.Command(pgDumpallPath, "-p", b.pgPort, "-U", "vcap", "-f", f.BackupPathAndFile, "--globals-only").CombinedOutput()
	if err != nil {
		log.Error(fmt.Sprintf(`tasks.createGlobalsFile() Error running pg_dumpall command for: %s file: %s ! %s`, b.databaseName, f.BackupPathAndFile, err))
		return
	}

	f.Duration = int(time.Since(start).Seconds())
	return

}

// createDumpAllFile - Create a pg backup file which contains a full pg_dumpall
func createDumpAllFile(b backupParams) (f history.BackupFileHistory, err error) {

	start := time.Now()
	f.Duration = 0
	f.Status = `ok`
	f.BackupFile = b.baseFileName + ".sql"
	f.BackupPathAndFile = b.basePath + "/" + b.databaseName + "/" + f.BackupFile
	f.DBName = b.databaseName
	f.Node = b.node

	pgDumpallPath := b.pgDumpPath + `all`

	_, err = exec.Command(pgDumpallPath, "-p", b.pgPort, "-b", "-U", "vcap", "-f", f.BackupPathAndFile).CombinedOutput()
	if err != nil {
		log.Error(fmt.Sprintf(`tasks.createDumpAllFile() Error running pg_dumpall command for: %s file: %s ! %s`, b.databaseName, f.BackupPathAndFile, err))
		return
	}

	f.Duration = int(time.Since(start).Seconds())
	return
}

func getBaseFileName() (baseFileName string) {
	baseFileName = time.Now().Format(globals.TIME_FORMAT)
	return
}

func acquireBackupLock(dbname string) (locked bool, sessID string) {
	key := fmt.Sprintf("rdpg/%s/tasks/backups/%s/lock", globals.ClusterID, dbname)
	client, _ := consulapi.NewClient(consulapi.DefaultConfig())
	sessID, _, err := client.Session().Create(nil, nil)
	if err != nil {
		log.Error(fmt.Sprintf("tasks.backups.acquireBackupLock() Error Creating Session %s ! %s", key, err))
		locked = false
	}
	locked, _, err = client.KV().Acquire(&consulapi.KVPair{Key: key, Value: []byte("locked"), Session: sessID}, nil)
	if err != nil {
		log.Error(fmt.Sprintf("tasks.backups.acquireBackupLock() Error Locking Backup Key %s ! %s", key, err))
		locked = false
	}
	return
}

func releaseBackupLock(dbname, sessID string) {
	key := fmt.Sprintf("rdpg/%s/tasks/backups/%s/lock", globals.ClusterID, dbname)
	client, _ := consulapi.NewClient(consulapi.DefaultConfig())
	//This Get part is for the purpose of making the check that only the holder of the lock is releasing it.
	// This part could be pruned if this test becomes obsolete.
	result, _, err := client.KV().Get(key, nil)
	if err != nil {
		log.Error(fmt.Sprintf("tasks.backups.releaseBackupLock() Error retrieving lock Key %s ! %s", key, err))
		return
	}
	if result.Session != sessID {
		log.Error(fmt.Sprintf("tasks.backups.releaseBackupLock() Can't release another session's lock (%s ~= %s) ! %s", result.Session, sessID, err))
		return
	}
	//At this point, we're cleared to release the lock
	_, _, err = client.KV().Release(&consulapi.KVPair{Key: key, Value: []byte("unlocked"), Session: sessID}, nil)
	if err != nil {
		log.Error(fmt.Sprintf("tasks.backups.releaseBackupLock() Error Releasing Backup Key %s ! %s", key, err))
	}
	return
}

//BackupAllDatabases - Use pg_dumpall on a SOLO cluster to perform a full backup of everything postgres related
func (t *Task) BackupAllDatabases() (err error) {
	b := backupParams{}

	b.databaseName = "postgres"
	b.pgDumpPath, err = config.GetValue(`pgDumpBinaryLocation`)
	if err != nil {
		return err
	}
	b.pgPort, err = config.GetValue(`BackupPort`)
	if err != nil {
		return err
	}
	b.basePath, err = config.GetValue(`BackupsPath`)
	if err != nil {
		return err
	}
	b.node, err = rdpgconsul.GetNode()
	if err != nil {
		return err
	}
	b.baseFileName = getBaseFileName() //Use this to keep schema and data file names the same

	err = createTargetFolder(b.basePath + `/` + b.databaseName)
	if err != nil {
		log.Error(fmt.Sprintf("tasks.BackupAllDatabases() Could not create target folder %s ! %s", b.basePath, err))
		return err
	}

	createDumpAllFileHistory, err := createDumpAllFile(b)
	if err != nil {
		log.Error(fmt.Sprintf("tasks.BackupAllDatabases() Could not create pg_dumpall file for database %s ! %s", b.databaseName, err))
		createDumpAllFileHistory.Status = `error`
	}
	err = history.InsertBackupHistoryDumpAll(createDumpAllFileHistory)

	return
}
