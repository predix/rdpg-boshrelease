package tasks

import (
	"fmt"

	"github.com/starkandwayne/rdpgd/log"
	"github.com/starkandwayne/rdpgd/pg"
)

func (t *Task) Vacuum() (err error) {
	p := pg.NewPG(`127.0.0.1`, pbPort, `rdpg`, `rdpg`, pgPass)
	db, err := p.Connect()
	if err != nil {
		log.Error(fmt.Sprintf("tasks.Work() Failed connecting to %s err: %s", p.URI, err))
		return err
	}
	defer db.Close()

	log.Trace(fmt.Sprintf(`tasks.Vacuum(%s)...`, t.Data))
	sq := fmt.Sprintf(`VACUUM FULL %s`, t.Data)
	log.Trace(fmt.Sprintf(`tasks.Vacuum() > %s`, sq))
	_, err = db.Exec(sq)
	if err != nil {
		log.Error(fmt.Sprintf(`tasks.Vacuum() ! %s`, err))
	}
	return
}
