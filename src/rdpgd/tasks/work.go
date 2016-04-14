package tasks

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/starkandwayne/rdpgd/globals"

	consulapi "github.com/hashicorp/consul/api"
	"github.com/jmoiron/sqlx"

	"github.com/starkandwayne/rdpgd/log"
	"github.com/starkandwayne/rdpgd/pg"
	"github.com/starkandwayne/rdpgd/utils/rdpgconsul"
)

var (
	workLock   *consulapi.Lock
	workLockCh <-chan struct{}
	workDB     *sqlx.DB
)

//Work - Select a task from the queue for this server
func Work() {
	err := OpenWorkDB()
	if err != nil {
		log.Error(fmt.Sprintf("tasks.Work() OpenWorkDB() %s", err))
		proc, _ := os.FindProcess(os.Getpid())
		proc.Signal(syscall.SIGTERM)
	}
	defer CloseWorkDB()

	for {
		tasks := []Task{}
		err = WorkLock()
		if err != nil {
			time.Sleep(10 * time.Second)
			continue
		}

		nodeType := `read`
		if rdpgconsul.IsWriteNode(globals.MyIP) {
			nodeType = `write`
		}
		sq := fmt.Sprintf(`SELECT id,cluster_id,node,role,action,data,ttl,node_type,cluster_service FROM tasks.tasks WHERE locked_by IS NULL AND role IN ('all','%s') AND node IN ('*','%s') AND node_type IN ('any','%s') ORDER BY created_at DESC LIMIT 1`, globals.ServiceRole, globals.MyIP, nodeType)

		log.Trace(fmt.Sprintf(`tasks.Work() > %s`, sq))
		err = workDB.Select(&tasks, sq)
		if err != nil {
			WorkUnlock()
			if err == sql.ErrNoRows {
				log.Trace(`tasks.Work() No tasks found.`)
			} else {
				log.Error(fmt.Sprintf(`tasks.Work() Selecting Task ! %s`, err))
			}
			time.Sleep(5 * time.Second)
			continue
		}
		if len(tasks) == 0 {
			WorkUnlock()
			time.Sleep(5 * time.Second)
			continue
		}
		task := tasks[0]
		err = task.Dequeue()
		if err != nil {
			log.Error(fmt.Sprintf(`tasks.Work() Task<%d>#Dequeue() ! %s`, task.ID, err))
			continue
		}
		WorkUnlock()

		// TODO: Come back and have a cleanup routine for tasks that were locked
		// but never finished past the TTL, perhaps a health check or such.
		err = task.Work()
		if err != nil {
			log.Error(fmt.Sprintf(`tasks.Task<%d>#Work() ! %s`, task.ID, err))

			sq = fmt.Sprintf(`UPDATE tasks.tasks SET locked_by=NULL, processing_at=NULL WHERE id=%d`, task.ID)
			log.Trace(fmt.Sprintf(`tasks#Work() > %s`, sq))
			_, err = workDB.Exec(sq)
			if err != nil {
				log.Error(fmt.Sprintf(`tasks.Work() Updating Task %d processing_at ! %s`, task.ID, err))
			}
			continue
		} else {
			// TODO: (t *Task) Delete()
			sq = fmt.Sprintf(`DELETE FROM tasks.tasks WHERE id=%d`, task.ID)
			log.Trace(fmt.Sprintf(`tasks#Work() > %s`, sq))
			_, err = workDB.Exec(sq)
			if err != nil {
				log.Error(fmt.Sprintf(`tasks.Work() Deleting Task %d ! %s`, task.ID, err))
				continue
			}
			log.Trace(fmt.Sprintf(`tasks.Work() Task Completed! > %+v`, task))
		}
	}
}

// WorkLock - Acquire consul for cluster to aquire right to schedule tasks.
func WorkLock() (err error) {
	clusterID := os.Getenv("RDPGD_CLUSTER")
	if clusterID == "" {
		matrixName := os.Getenv(`RDPGD_MATRIX`)
		matrixNameSplit := strings.SplitAfterN(matrixName, `-`, -1)
		matrixColumn := os.Getenv(`RDPGD_MATRIX_COLUMN`)
		for i := 0; i < len(matrixNameSplit)-1; i++ {
			clusterID = clusterID + matrixNameSplit[i]
		}
		clusterID = clusterID + "c" + matrixColumn
	}

	key := fmt.Sprintf("rdpg/%s/tasks/work/lock", clusterID)
	client, _ := consulapi.NewClient(consulapi.DefaultConfig())
	workLock, err = client.LockKey(key)
	if err != nil {
		log.Error(fmt.Sprintf("tasks.WorkLock() Error Locking Work Key %s ! %s", key, err))
		return
	}

	workLockCh, err = workLock.Lock(nil) // Acquire Consul K/V Lock
	if err != nil {
		log.Error(fmt.Sprintf("tasks.WorkLock() Error Acquiring Work Key lock %s ! %s", key, err))
		return
	}

	if workLockCh == nil {
		err = fmt.Errorf(`tasks.WorkLock() Work Lock not acquired`)
	}

	return
}

//WorkUnlock - Release the work lock
func WorkUnlock() (err error) {
	if workLock != nil {
		err = workLock.Unlock()
		if err != nil {
			log.Error(fmt.Sprintf("tasks.WorkUnlock() Error Unlocking Work ! %s", err))
		}
	}
	return
}

//Work - Entry point for the type of action for a particular task
func (t *Task) Work() (err error) {
	// TODO: Add in TTL Logic with error logging.
	switch t.Action {
	case "Vacuum":
		go t.Vacuum()
	case "PrecreateDatabases":
		go t.PrecreateDatabases()
	case "ReconcileAvailableDatabases":
		go t.ReconcileAvailableDatabases()
	case "ReconcileAllDatabases":
		go t.ReconcileAllDatabases()
	case "DecommissionDatabase":
		go t.DecommissionDatabase()
	case "DecommissionDatabases":
		go t.DecommissionDatabases()
	case "Reconfigure":
		go t.Reconfigure()
	case "ScheduleNewDatabaseBackups":
		go t.ScheduleNewDatabaseBackups()
	case "BackupDatabase": // Role: read
		go t.BackupDatabase()
	case "DeleteBackupHistory":
		go t.DeleteBackupHistory()
	case "FindFilesToCopyToS3":
		go t.FindFilesToCopyToS3()
	case "EnforceFileRetention":
		go t.EnforceFileRetention()
	case "EnforceRemoteFileRetention":
		go t.EnforceRemoteFileRetention()
	case "CopyFileToS3":
		go t.CopyFileToS3()
	case "DeleteFile":
		go t.DeleteFile()
	case "RestoreDatabaseFromFile":
		go t.RestoreDatabaseFromFile()
	case "CreateTestDB":
		go t.CreateTestDB()
	case "BackupAllDatabases":
		go t.BackupAllDatabases()
	case "ClearStuckTasks":
		go t.ClearStuckTasks()
	default:
		err = fmt.Errorf(`tasks.Work() BUG!!! Unknown Task Action %s`, t.Action)
		log.Error(fmt.Sprintf(`tasks.Work() Task %+v ! %s`, t, err))
	}
	sq := fmt.Sprintf(`DELETE FROM tasks.tasks WHERE id=%d`, t.ID)
	_, err = workDB.Exec(sq)
	if err != nil {
		log.Error(fmt.Sprintf(`tasks.Work() Error deleting completed task %d ! %s`, t.ID, err))
	}
	return
}

//OpenWorkDB - Connect to the rdpg system database and connect to the tasks.tasks table
func OpenWorkDB() (err error) {
	if workDB == nil {
		p := pg.NewPG(`127.0.0.1`, pbPort, `rdpg`, `rdpg`, pgPass)
		err := p.WaitForRegClass("tasks.tasks")
		if err != nil {
			log.Error(fmt.Sprintf("tasks.Work() Failed connecting to %s err: %s", p.URI, err))
			return err
		}

		workDB, err = p.Connect()
		if err != nil {
			log.Error(fmt.Sprintf("tasks.Work() Failed connecting to %s err: %s", p.URI, err))
			return err
		}
	}
	return
}

//CloseWorkDB - Close connection to the rdpg system database
func CloseWorkDB() (err error) {
	if workDB != nil {
		workDB.Close()
	}
	return
}
