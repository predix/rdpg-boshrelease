package instances

import (
	"fmt"

	"github.com/starkandwayne/rdpgd/log"
	"github.com/starkandwayne/rdpgd/pg"
)

// This is called on the management cluster when it is running the scheduled task
// which reconciles the databases comparing against the service clusters lists.
func (i *Instance) Reconcile() (err error) {
	p := pg.NewPG(`127.0.0.1`, pbPort, `rdpg`, `rdpg`, pgPass)
	db, err := p.Connect()
	if err != nil {
		log.Error(fmt.Sprintf("instances.Register() p.Connect(%s) ! %s", p.URI, err))
		return err
	}
	defer db.Close()
	err = i.Lock()
	if err != nil {
		log.Error(fmt.Sprintf("instances.Instance#Reconcile(%s) Failed Locking instance %s ! %s", i.Database, i.Database, err))
		return
	}
	ei, err := FindByDatabase(i.Database)
	if err != nil {
		log.Error(fmt.Sprintf("instances.Instance#Reconcile() ! %s", err))
	} else if ei == nil {
		log.Trace(fmt.Sprintf(`instances.Instance#Reconcile() Reconciling database %s for cluster %s`, i.Database, i.ClusterID))
		sq := fmt.Sprintf(`INSERT INTO cfsb.instances (cluster_id,service_id ,plan_id ,instance_id ,organization_id ,space_id,dbname, dbuser, dbpass,effective_at,cluster_service) VALUES ('%s', '%s', '%s', '%s', '%s','%s','%s','%s','%s',CURRENT_TIMESTAMP, '%s')`, i.ClusterID, i.ServiceID, i.PlanID, i.InstanceID, i.OrganizationID, i.SpaceID, i.Database, i.User, i.Pass, i.ClusterService)
		log.Trace(fmt.Sprintf(`instances.Instance#Reconcile(%s) > %s`, i.Database, sq))
		_, err = db.Exec(sq)
		if err != nil {
			log.Error(fmt.Sprintf("instances.Instance#Reconcile(%s) ! %s", i.Database, err))
		}
	}
	err = i.Unlock()
	if err != nil {
		log.Error(fmt.Sprintf(`instances.Instance#Reconcile(%s) Unlocking ! %s`, i.InstanceID, err))
	}
	return
}
