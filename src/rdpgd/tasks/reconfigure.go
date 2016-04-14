package tasks

import (
	"fmt"

	"github.com/starkandwayne/rdpgd/log"
	"github.com/starkandwayne/rdpgd/services"
)

func (t *Task) Reconfigure() (err error) {
	service, err := services.NewService("pgbouncer")
	if err != nil {
		log.Error(fmt.Sprintf(`tasks.Task#Reconfigure(%s) services.NewService(pgbouncer) ! %s`, t.ClusterID, err))
	}
	err = service.Configure()
	if err != nil {
		log.Error(fmt.Sprintf(`tasks.TaskReconfigure(%s) service.Configure() ! %s`, t.ClusterID, err))
	}
	return
}
