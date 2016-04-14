package gpb

import (
	"fmt"

	"github.com/starkandwayne/rdpgd/log"
	"github.com/starkandwayne/rdpgd/pg"
)

//	consulapi "github.com/hashicorp/consul/api"

//Instance - represents 1 row in the cfsb.instances table
type Instance struct {
	ClusterID      string `db:"cluster_id" json:"cluster_id"`
	ClusterService string `db:"cluster_service" json:"cluster_service"`
	Database       string `db:"dbname" json:"dbname"`
	User           string `db:"dbuser" json:"uname"`
	Pass           string `db:"dbpass" json:"pass"`
}

//Active - return a list of all user databases from the MC
func active() (si []Instance, err error) {

	p := pg.NewPG(mcIP, mcPort, mcUser, `rdpg`, mcPass)
	db, err := p.Connect()
	if err != nil {
		log.Error(fmt.Sprintf("gpb#instances.active() p.Connect(%s) ! %s", p.URI, err))
		return
	}
	defer db.Close()

	si = []Instance{}
	sq := `SELECT cluster_id, cluster_service, dbname, dbuser, 'md5'||md5(cfsb.instances.dbpass||dbuser) as dbpass FROM cfsb.instances WHERE ineffective_at IS NULL `
	err = db.Select(&si, sq)
	if err != nil {
		log.Error(fmt.Sprintf("gpb#instances.active() ! %s", err))
	}
	return
}
