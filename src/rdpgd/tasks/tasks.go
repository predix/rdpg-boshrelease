package tasks

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/starkandwayne/rdpgd/globals"
	"github.com/starkandwayne/rdpgd/log"
)

var (
	myIP string
	// ClusterID represents the RDPG cluster ID
	ClusterID string
	pbPort    string
	pgPass    string
	pgPort    string
	poolSize  int
	// MatrixName represents the name of the cluster node within the deployment.
	MatrixName string
	// MatrixColumn represents the column of the cluster node within the deployment.
	MatrixColumn string
)

// MatrixNameSplit contains the split matrix name.
var MatrixNameSplit []string

/*
Task struct is used to represent tasks.
*/
type Task struct {
	ID             int64  `db:"id" json:"id"`
	ClusterID      string `db:"cluster_id" json:"cluster_id"`
	ClusterService string `db:"cluster_service" json:"cluster_service"`
	Node           string `db:"node" json:"node"`
	Role           string `db:"role" json:"role"`
	Action         string `db:"action" json:"action"`
	Data           string `db:"data" json:"data"`
	TTL            int64  `db:"ttl" json:"ttl"`
	NodeType       string `db:"node_type" json:"node_type"`
}

func init() {
	MatrixName = os.Getenv(`RDPGD_MATRIX`)
	MatrixNameSplit = strings.SplitAfterN(MatrixName, `-`, -1)
	MatrixColumn = os.Getenv(`RDPGD_MATRIX_COLUMN`)
	ClusterID = os.Getenv(`RDPGD_CLUSTER`)
	if ClusterID == "" {
		for i := 0; i < len(MatrixNameSplit)-1; i++ {
			ClusterID = ClusterID + MatrixNameSplit[i]
		}
		ClusterID = ClusterID + "c" + MatrixColumn
	}
	if ClusterID == "" {
		log.Error(`tasks.Scheduler() RDPGD_CLUSTER not found in environment!!!`)
	}
	pbPort = os.Getenv(`RDPGD_PB_PORT`)
	if pbPort == `` {
		pbPort = `6432`
	}
	pgPass = os.Getenv(`RDPGD_PG_PASS`)
	pgPort = os.Getenv(`RDPGD_PG_PORT`)
	ps := os.Getenv(`RDPGD_POOL_SIZE`)
	if ps == "" {
		poolSize = 10
	} else {
		p, err := strconv.Atoi(ps)
		if err != nil {
			poolSize = 10
		} else {
			poolSize = p
		}
	}
}

/*
NewTask returns a new Task struct object.
*/
func NewTask() *Task {
	return &Task{Node: "*"}
}

/*
Enqueue enqueue's a given task to the database's rdpg.tasks table.
*/
func (t *Task) Enqueue() (err error) {
	sq := fmt.Sprintf(`INSERT INTO tasks.tasks (cluster_id,node,role,action,data,ttl,node_type,cluster_service) VALUES ('%s','%s','%s','%s','%s',%d,'%s','%s')`, t.ClusterID, t.Node, t.Role, t.Action, t.Data, t.TTL, t.NodeType, t.ClusterService)
	log.Trace(fmt.Sprintf(`tasks.Task#Enqueue() > %s`, sq))
	for {
		OpenWorkDB()
		_, err = workDB.Exec(sq)
		if err != nil {
			re := regexp.MustCompile(`tasks_pkey`)
			if re.MatchString(err.Error()) {
				continue
			} else {
				log.Error(fmt.Sprintf(`tasks.Task#Enqueue() Insert Task %+v ! %s`, t, err))
				return
			}
		}
		break
	}
	log.Trace(fmt.Sprintf(`tasks.Task#Enqueue() Task Enqueued > %+v`, t))
	return
}

/*
Dequeue dequeue's a given task from the database's rdpg.tasks table.
*/
func (t *Task) Dequeue() (err error) {
	tasks := []Task{}
	sq := fmt.Sprintf(`SELECT id,node,cluster_id,role,action,data,ttl,node_type,cluster_service FROM tasks.tasks WHERE id=%d LIMIT 1`, t.ID)
	log.Trace(fmt.Sprintf(`tasks.Task<%d>#Dequeue() > %s`, t.ID, sq))
	OpenWorkDB()
	err = workDB.Select(&tasks, sq)
	if err != nil {
		log.Error(fmt.Sprintf(`tasks.Task<%d>#Dequeue() Selecting Task %+v ! %s`, t.ID, t, err.Error()))
		return
	}
	if len(tasks) == 0 {
		log.Error(fmt.Sprintf(`tasks.Task<%d>#Dequeue() No rows returned for task %+v`, t.ID, t))
		return
	}
	t = &tasks[0]
	// TODO: Add the information for who has this task locked using IP
	sq = fmt.Sprintf(`UPDATE tasks.tasks SET locked_by='%s', processing_at=CURRENT_TIMESTAMP WHERE id=%d`, myIP, t.ID)
	log.Trace(fmt.Sprintf(`tasks.Task<%d>#Dequeue() > %s`, t.ID, sq))
	_, err = workDB.Exec(sq)
	if err != nil {
		log.Error(fmt.Sprintf(`tasks.Task<%d>#Dequeue() Updating Task processing_at ! %s`, t.ID, err))
		return
	}
	log.Trace(fmt.Sprintf(`tasks.Task<%d>#Dequeue() Task Dequeued > %+v`, t.ID, t))
	return
}

//ClearStuckTasks - Clear any stuck tasks
func (t *Task) ClearStuckTasks() (err error) {

	sq := fmt.Sprintf(`DELETE FROM tasks.tasks WHERE created_at < (CURRENT_TIMESTAMP - '%s'::interval)`, globals.StuckDuration)

	log.Trace(fmt.Sprintf(`tasks#Work() > %s`, sq))
	OpenWorkDB()
	_, err = workDB.Exec(sq)
	if err != nil {
		log.Error(fmt.Sprintf(`tasks.Work() Error Deleting Stuck Task ! %s`, err))
	}
	return
}
