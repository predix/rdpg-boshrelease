package tasks

import (
	"fmt"

	"github.com/starkandwayne/rdpgd/history"
	"github.com/starkandwayne/rdpgd/log"
)

//DeleteBackupHistory - Responsible for deleting records from backups.file_history
//older than the value in rdpg.config.key = defaultDaysToKeepFileHistory
func (t *Task) DeleteBackupHistory() (err error) {
	log.Trace("tasks.DeleteBackupHistory Starting...")
	err = history.DeleteBackupHistory()
	if err != nil {
		log.Error(fmt.Sprintf("tasks.DeleteBackupHistory ! utils/backup.DeleteBackupHistory erred : %s", err.Error()))
	}
	return
}
