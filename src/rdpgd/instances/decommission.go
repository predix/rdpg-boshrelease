package instances

import (
	"fmt"

	"github.com/starkandwayne/rdpgd/log"
	"github.com/starkandwayne/rdpgd/pg"
)

func (i *Instance) Decommission() (err error) {
	p := pg.NewPG(`127.0.0.1`, pbPort, `rdpg`, `rdpg`, pgPass)
	db, err := p.Connect()
	if err != nil {
		log.Error(fmt.Sprintf("instances.Decommission() p.Connect(%s) ! %s", p.URI, err))
		return
	}
	defer db.Close()

	// TODO: i.SetIneffective()
	sq := fmt.Sprintf(`UPDATE cfsb.instances SET ineffective_at=CURRENT_TIMESTAMP WHERE dbname='%s'`, i.Database)
	log.Trace(fmt.Sprintf(`instances.Instance<%s>#Decommission() SQL > %s`, i.InstanceID, sq))
	_, err = db.Exec(sq)
	if err != nil {
		log.Error(fmt.Sprintf("Instance#Decommission(%s) setting inefective_at ! %s", i.InstanceID, err))
		return
	}

	// TODO: tasks.Task{ClusterID: ,Node: ,Role: ,Action:, Data: }.Enqueue()
	// Question is how to do this without an import cycle? Some tasks require instances.
	sq = fmt.Sprintf(`INSERT INTO tasks.tasks (cluster_id,role,action,data, cluster_service) VALUES ('%s','all','DecommissionDatabase','%s', '%s')`, i.ClusterID, i.Database, i.ClusterService)
	log.Trace(fmt.Sprintf(`instances.Instance#Decommission(%s) Scheduling Instance Removal > %s`, i.InstanceID, sq))
	_, err = db.Exec(sq)
	if err != nil {
		log.Error(fmt.Sprintf(`instances.Instance#Decommission(%s) ! %s`, i.InstanceID, err))
	}
	return
}

// DecommissionedAt() is called when the service cluster tells the master cluster
// that a given instance has been deprovisioned at a certain timestamp
func (i *Instance) DecommissionedAt(timestamp string) (err error) {
	p := pg.NewPG(`127.0.0.1`, pbPort, `rdpg`, `rdpg`, pgPass)
	db, err := p.Connect()
	if err != nil {
		log.Error(fmt.Sprintf("instances.DecommissionedAt() p.Connect(%s) ! %s", p.URI, err))
		return err
	}
	defer db.Close()

	sq := fmt.Sprintf(`UPDATE cfsb.instances SET decommissioned_at='%s'::timestamp WHERE id='%s'`, timestamp, i.ID)
	log.Trace(fmt.Sprintf(`instances.Instance#DecommissionedAt(%s) SQL > %s`, i.Database, sq))
	_, err = db.Exec(sq)
	if err != nil {
		log.Error(fmt.Sprintf("instances.Instance#DecommissionedAt(%s) decommissioned_at ! %s", i.Database, err))
	}

	return
}
