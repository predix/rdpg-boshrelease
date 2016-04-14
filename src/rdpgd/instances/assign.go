package instances

import (
	"fmt"

	"github.com/starkandwayne/rdpgd/log"
	"github.com/starkandwayne/rdpgd/pg"
)

// Assign() is called when the master cluster tells the service cluster about
// an assignment.
func (i *Instance) Assign() (err error) {
	p := pg.NewPG(`127.0.0.1`, pbPort, `rdpg`, `rdpg`, pgPass)
	db, err := p.Connect()
	if err != nil {
		log.Error(fmt.Sprintf("instances.Assign() p.Connect(%s) ! %s", p.URI, err))
		return err
	}
	defer db.Close()

	for {
		err = i.Lock()
		if err != nil {
			log.Error(fmt.Sprintf("instances.Instance#Assign(%s) Failed Locking instance ! %s", i.Database, err))
			continue
		}
		sq := fmt.Sprintf(`UPDATE cfsb.instances SET service_id='%s',plan_id='%s',instance_id='%s',organization_id='%s',space_id='%s' WHERE dbname='%s'`, i.ServiceID, i.PlanID, i.InstanceID, i.OrganizationID, i.SpaceID, i.Database)
		log.Trace(fmt.Sprintf(`instances.Instance#Assign(%s) > %s`, i.Database, sq))
		_, err = db.Exec(sq)
		if err != nil {
			log.Error(fmt.Sprintf("instances.Instance#Assign(%s) ! %s", i.Database, err))
			err = i.Unlock()
			if err != nil {
				log.Error(fmt.Sprintf(`instances.Instance#Assign(%s) Unlocking ! %s`, i.InstanceID, err))
			}
			continue
		} else {
			err = i.Unlock()
			if err != nil {
				log.Error(fmt.Sprintf(`instances.Instance#Assign(%s) Unlocking ! %s`, i.InstanceID, err))
			}
			break
		}
	}

	ips, err := i.ClusterIPs()
	if err != nil {
		log.Error(fmt.Sprintf(`instances.Instance#Assign(%s) i.ClusterIPs() ! %s`, i.InstanceID, err))
		return
	}
	for _, ip := range ips {
		// TODO: tasks.Task{ClusterID: ,Node: ,Role: ,Action:, Data: }.Enqueue()
		// Question is how to do this without an import cycle? Smoe tasks require instances.
		sq := fmt.Sprintf(`INSERT INTO tasks.tasks (cluster_id,node,role,action,data,node_type, cluster_service) VALUES ('%s','%s','service','Reconfigure','pgbouncer','any','%s')`, ClusterID, ip, i.ClusterService)
		log.Trace(fmt.Sprintf(`instances.Instance#Assign(%s) Enqueue Reconfigure of pgbouncer > %s`, i.InstanceID, sq))
		_, err = db.Exec(sq)
		if err != nil {
			log.Error(fmt.Sprintf(`instances.Instance#Assign(%s) Inserting into tasks: %s`, i.InstanceID, sq))
		}
	}
	return
}
