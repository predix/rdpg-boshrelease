package admin

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/starkandwayne/rdpgd/log"
)

// StatsHandler handle http request
type StatsHandler struct {
	stats Stats
}

// Stats interface used to aid in testing
type Stats interface {
	GetStats() interface{}
}

// AgentStats actual struct to get data
type AgentStats struct {
	QueueDepth        int `json:"task_queue_depth"`
	NumBoundDB        int `json:"num_bound_db"`
	NumFreeDB         int `json:"num_free_db"`
	NumReplSlots      int `json:"num_replication_slots"`
	NumDBBackupDisk   int `json:"num_db_backup_files_on_disk"`
	NumUserDatabases  int `json:"num_user_databases"`
	NumLimitDatabases int `json:"num_limit_databases"`
}

// MockStats used for testing mock data
type MockStats struct {
	Foo string
}

//GetStats return stats for agent
func (a *AgentStats) GetStats() interface{} {
	a.QueueDepth = getQueueDepth()
	a.NumBoundDB = getNumberOfBoundDatabases()
	a.NumFreeDB = getNumberOfFreeDatabases()
	a.NumReplSlots = getNumberOfReplicationSlots()
	a.NumDBBackupDisk = getNumberOfDatabaseBackupOnDisk()
	a.NumUserDatabases = getNumberOfUserDatabases()
	a.NumLimitDatabases = getMaxLimitNumberOfDatabases()
	return a
}

//GetStats get mock data
func (m *MockStats) GetStats() interface{} {
	m.Foo = "123"
	return m
}

//NewStatsHandler create new stat handler
func NewStatsHandler(stats Stats) StatsHandler {
	return StatsHandler{
		stats: stats,
	}
}

func LocksHandler(w http.ResponseWriter, request *http.Request) {
	vars := mux.Vars(request)
	dbname := vars["database"]

	count, err := getLockCountByDatabase(dbname)
	if err != nil {
		log.Error(fmt.Sprintf("admin.LocksHandler.ServeHTTP ! erred : %s", err.Error()))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	msg, err := json.Marshal(count)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(msg)
}

//ServeHTTP serves http request
func (s *StatsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	msg, err := json.Marshal(s.stats.GetStats())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(msg)
}
