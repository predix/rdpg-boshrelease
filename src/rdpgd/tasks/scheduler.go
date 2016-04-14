package tasks

import (
	"fmt"
	"os"
	"syscall"
	"time"

	consulapi "github.com/hashicorp/consul/api"
	"github.com/jmoiron/sqlx"

	"github.com/starkandwayne/rdpgd/globals"
	"github.com/starkandwayne/rdpgd/log"
	"github.com/starkandwayne/rdpgd/pg"
)

var (
	scheduleLock   *consulapi.Lock
	scheduleLockCh <-chan struct{}
	scheduleDB     *sqlx.DB
)

//Schedule - holds one row object from tasks.schedules
type Schedule struct {
	ID             int64  `db:"id" json:"id"`
	ClusterID      string `db:"cluster_id" json:"cluster_id"`
	ClusterService string `db:"cluster_service" json:"cluster_service"`
	Role           string `db:"role" json:"role"`
	Action         string `db:"action" json:"action"`
	Data           string `db:"data" json:"data"`
	TTL            int64  `db:"ttl" json:"ttl"`
	NodeType       string `db:"node_type" json:"node_type"`
	Frequency      string `db:"frequency" json:"frequency"`
	Enabled        bool   `db:"enabled" json:"enabled"`
}

//Add - Insert a new schedule into tasks.schedules
func (s *Schedule) Add() (err error) {
	p := pg.NewPG(`127.0.0.1`, pbPort, `rdpg`, `rdpg`, pgPass)
	p.Set(`database`, `rdpg`)

	scheduleDB, err := p.Connect()
	if err != nil {
		log.Error(fmt.Sprintf(`tasks.Schedule.Add() Could not open connection ! %s`, err))
	}

	defer scheduleDB.Close()

	sq := fmt.Sprintf(`INSERT INTO tasks.schedules (cluster_id,role,action,data,frequency,enabled,node_type,cluster_service) SELECT '%s','%s','%s','%s','%s'::interval, %t, '%s', '%s' WHERE NOT EXISTS (SELECT id FROM tasks.schedules WHERE action = '%s' AND node_type = '%s' AND data = '%s') `, s.ClusterID, s.Role, s.Action, s.Data, s.Frequency, s.Enabled, s.NodeType, s.ClusterService, s.Action, s.NodeType, s.Data)
	log.Trace(fmt.Sprintf(`tasks.Schedule.Add(): %s`, sq))
	_, err = scheduleDB.Exec(sq)
	if err != nil {
		log.Error(fmt.Sprintf(`tasks.Schedule.Add():  %s`, err))
	}
	return
}

/*
Task Scheduler TODO's
- Task TTL: "Task type X should take no more than this long"
- accounting history stored in database.
- TTL based cleanup of task Queue for workers that may have imploded.
*/

//Scheduler - Entry point for executing the long running tasks scheduler
func Scheduler() {
	p := pg.NewPG(`127.0.0.1`, pbPort, `rdpg`, `rdpg`, pgPass)
	p.Set(`database`, `rdpg`)

	err := p.WaitForRegClass("tasks.schedules")
	if err != nil {
		log.Error(fmt.Sprintf(`tasks.Scheduler() p.WaitForRegClass() ! %s`, err))
	}

	scheduleDB, err := p.Connect()
	if err != nil {
		log.Error(fmt.Sprintf(`tasks.Scheduler() p.Connect() Failed connecting to %s ! %s`, p.URI, err))
		proc, _ := os.FindProcess(os.Getpid())
		proc.Signal(syscall.SIGTERM)
	}
	defer scheduleDB.Close()

	for {
		err = SchedulerLock()
		if err != nil {
			time.Sleep(10 * time.Second)
			continue
		}
		schedules := []Schedule{}
		sq := fmt.Sprintf(`SELECT id,cluster_id, role, action, data, ttl, node_type, cluster_service FROM tasks.schedules WHERE enabled = true AND CURRENT_TIMESTAMP >= (last_scheduled_at + frequency::interval) AND role IN ('all','%s')`, globals.ServiceRole)
		log.Trace(fmt.Sprintf(`tasks#Scheduler() Selecting Schedules > %s`, sq))
		err = scheduleDB.Select(&schedules, sq)
		if err != nil {
			log.Error(fmt.Sprintf(`tasks.Scheduler() Selecting Schedules ! %s`, err))
			SchedulerUnlock()
			time.Sleep(10 * time.Second)
			continue
		}
		for index := range schedules {
			sq = fmt.Sprintf(`UPDATE tasks.schedules SET last_scheduled_at = CURRENT_TIMESTAMP WHERE id=%d`, schedules[index].ID)
			log.Trace(fmt.Sprintf(`tasks#Scheduler() %+v > %s`, schedules[index], sq))
			_, err = scheduleDB.Exec(sq)
			if err != nil {
				log.Error(fmt.Sprintf(`tasks.Scheduler() Schedule: %+v ! %s`, schedules[index], err))
				continue
			}
			task := NewTask()
			task.ClusterID = schedules[index].ClusterID
			task.ClusterService = schedules[index].ClusterService
			task.Role = schedules[index].Role
			task.Action = schedules[index].Action
			task.Data = schedules[index].Data
			task.TTL = schedules[index].TTL
			task.NodeType = schedules[index].NodeType
			err = task.Enqueue()
			if err != nil {
				log.Error(fmt.Sprintf(`tasks.Scheduler() Task.Enqueue() %+v ! %s`, task, err))
			}
		}
		SchedulerUnlock()
		time.Sleep(10 * time.Second)
	}
}

//NewSchedule - default empty constructor
func NewSchedule() (s *Schedule) {
	return &Schedule{}
}

// SchedulerLock - Acquire consul schedulerLock for cluster to aquire right to schedule tasks.
func SchedulerLock() (err error) {

	key := fmt.Sprintf("rdpg/%s/tasks/scheduler/lock", ClusterID)
	client, _ := consulapi.NewClient(consulapi.DefaultConfig())
	scheduleLock, err = client.LockKey(key)
	if err != nil {
		log.Error(fmt.Sprintf("tasks.SchedulerLock() Error Locking Scheduler Key %s ! %s", key, err))
		return
	}
	scheduleLockCh, err = scheduleLock.Lock(nil)
	if err != nil {
		log.Error(fmt.Sprintf("tasks.SchedulerLock() Error Aquiring Scheduler Key lock %s ! %s", key, err))
		return
	}

	if scheduleLockCh == nil {
		err = fmt.Errorf(`tasks.SchedulerLock() Scheduler Lock not aquired`)
	}

	return
}

// SchedulerUnlock - Release consul schedulerLock for the current cluster
func SchedulerUnlock() (err error) {
	if scheduleLock != nil {
		err = scheduleLock.Unlock()
		if err != nil {
			log.Error(fmt.Sprintf("tasks.SchedulerUnlock() Error Unlocking Scheduler ! %s", err))
		}
	}
	return
}
