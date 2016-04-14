package instances

import (
	"fmt"

	"github.com/starkandwayne/rdpgd/log"
	"github.com/starkandwayne/rdpgd/pg"
)

// Register - This is called on the management cluster when the service cluster has created
// a new database and is registering it's avialabilityh with the management cluster.
func (i *Instance) Register() (err error) {
	p := pg.NewPG(`127.0.0.1`, pbPort, `rdpg`, `rdpg`, pgPass)
	db, err := p.Connect()
	if err != nil {
		log.Error(fmt.Sprintf("instances.Register() p.Connect(%s) ! %s", p.URI, err))
		return err
	}
	defer db.Close()

	err = i.Lock()
	if err != nil {
		log.Error(fmt.Sprintf("instances.Instance#Register(%s) Failed Locking instance %s ! %s", i.Database, i.Database, err))
		return
	}
	sq := fmt.Sprintf(`INSERT INTO cfsb.instances (cluster_id,dbname, dbuser, dbpass,effective_at, cluster_service) VALUES ('%s','%s','%s','%s',CURRENT_TIMESTAMP, '%s')`, i.ClusterID, i.Database, i.User, i.Pass, i.ClusterService)
	log.Trace(fmt.Sprintf(`instances.Instance#Register(%s) > %s`, i.Database, sq))
	_, err = db.Exec(sq)
	if err != nil {
		log.Error(fmt.Sprintf("instances.Instance#Register(%s) ! %s", i.Database, err))
	}
	err = i.Unlock()
	if err != nil {
		log.Error(fmt.Sprintf(`instances.Instance#Register(%s) Unlocking ! %s`, i.InstanceID, err))
	}

	return
}
