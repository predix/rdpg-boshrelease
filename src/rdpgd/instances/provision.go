package instances

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"

	consulapi "github.com/hashicorp/consul/api"

	"github.com/starkandwayne/rdpgd/log"
	"github.com/starkandwayne/rdpgd/pg"
)

// Provision is called by cfsb when a new service instance is requested.
func (i *Instance) Provision() (err error) {
	p := pg.NewPG(`127.0.0.1`, pbPort, `rdpg`, `rdpg`, pgPass)
	db, err := p.Connect()
	if err != nil {
		log.Error(fmt.Sprintf("instances.Instance#Provision() p.Connect(%s) ! %s", p.URI, err))
		return
	}
	defer db.Close()

	//maxCapacityString := os.Getenv(`RDPGD_INSTANCE_ALLOWED`)
	//	maxCapacity, err := strconv.Atoi(maxCapacityString)
	//if err != nil {
	//log.Error(fmt.Sprintf("instances.Instance#Provision() Getting max instance capacity ! %s", err))
	//}

	attempts := 0
	var dbname string
	for { // In case we need to wait for a precreated database on the cluster...
		// TODO: Compute which cluster the database will be assigned to based on
		//      min(# assigned for each cluster), then targeting this cluster:
		// TODO: Take into account plan with the above calculation, eg. dedicated vs shared
		// TODO: Group By minimum assigned on cluster when possible.
		// TODO: really switch this up, prefer a PostgreSQL pl/pgsql function.
		sq := fmt.Sprintf(`SELECT a.dbname AS dbname FROM cfsb.instances a, cfsb.plans b WHERE a.instance_id IS NULL AND a.effective_at IS NOT NULL AND a.ineffective_at IS NULL and a.decommissioned_at IS NULL AND a.cluster_service = b.cluster_service AND b.plan_id = '%s' ORDER BY a.created_at ASC LIMIT 1`, i.PlanID)
		log.Trace(fmt.Sprintf(`instances.Instance#Provision() > %s`, sq))
		err = db.Get(&dbname, sq)
		if err != nil {
			if err == sql.ErrNoRows {
				instanceNum := 0
				sq := fmt.Sprintf(`SELECT count(*) FROM cfsb.instances a, cfsb.plans b WHERE a.instance_id IS NULL AND a.effective_at IS NOT NULL AND a.ineffective_at IS NULL and a.decommissioned_at IS NULL AND a.cluster_service = b.cluster_service AND b.plan_id = '%s' `, i.PlanID)
				log.Trace(fmt.Sprintf(`instances.Instance#Provision() > %s`, sq))
				err = db.Get(&instanceNum, sq)
				if err != nil {
					log.Error(fmt.Sprintf("instances.Instance#Provision(%s): Fail to query instance number for plan: %s > %s ! %s", i.InstanceID, i.PlanID, sq, err))
				}
				instanceCapacity, err := ClusterCapacity()
				log.Trace(fmt.Sprintf("!!!!!!!instance num: %d, capacity: %d", instanceNum, instanceCapacity))
				if err != nil {
					log.Error(fmt.Sprintf("instances.Instance#Provision(): Fail to get instance capacity ! %s", err))
				}
				if instanceNum >= instanceCapacity {
					return errors.New(`Postgres service provisioning failed. The postgres cluster is out of capacity.  Please notify operations to adjust the database limits.`)
				}
				if attempts < 3 {
					log.Error(fmt.Sprintf("instances.Instance#Provision() ! Out of Capacity, attempt %d, retrying in 10s.", attempts))
					attempts += 1
					time.Sleep(10 * time.Second)
					continue
				} else {
					return errors.New(`Provisioning failed, temporarily out of capacity. Please wait a few minutes and try again. If the problem persists beyond 10 minutes please notify the operations team.`)
				}
			} else {
				log.Error(fmt.Sprintf("instances.Instance#Provision(%s) ! %s", i.InstanceID, err))
				return err
			}
		}
		log.Trace(fmt.Sprintf(`cfsb.Instance#Provision(%s) > Attempting to lock instance %s.`, i.Database, dbname))
		ni, err := FindByDatabase(string(dbname))
		if err != nil {
			log.Error(fmt.Sprintf("instances.Instance#Provision(%s) ! %s", i.InstanceID, err))
			return err
		}
		i.ClusterID = ni.ClusterID
		i.Database = ni.Database
		i.User = ni.User
		i.Pass = ni.Pass

		err = i.Lock()
		if err != nil {
			log.Error(fmt.Sprintf("instances.Instance#Provision(%s) Failed Locking instance %s ! %s", i.InstanceID, dbname, err))
			continue
		}
		sq = fmt.Sprintf(`UPDATE cfsb.instances SET instance_id='%s', service_id='%s', plan_id='%s', organization_id='%s', space_id='%s' WHERE dbname='%s'`, i.InstanceID, i.ServiceID, i.PlanID, i.OrganizationID, i.SpaceID, i.Database)
		log.Trace(fmt.Sprintf(`instances.Instance#Provision(%s) > %s`, i.InstanceID, sq))
		_, err = db.Exec(sq)
		if err != nil {
			log.Error(fmt.Sprintf(`instances.Instance#Provision(%s) ! %s`, i.InstanceID, err))
			return err
		}
		err = i.Unlock()
		if err != nil {
			log.Error(fmt.Sprintf(`instances.Instance#Provision(%s) Unlocking ! %s`, i.InstanceID, err))
		}
		// Tell the service cluster about the assignment.
		// TODO: This can be in a function.
		client, err := consulapi.NewClient(consulapi.DefaultConfig())
		if err != nil {
			log.Error(fmt.Sprintf("instances.Instance#precreateDatabase() consulapi.NewClient() ! %s", err))
			return err
		}
		catalog := client.Catalog()
		svcs, _, err := catalog.Service(i.ClusterID, "", nil)
		if err != nil {
			log.Error(fmt.Sprintf("instances.Instance#precreateDatabase() consulapi.Client.Catalog() ! %s", err))
			return err
		}
		if len(svcs) == 0 {
			log.Error(fmt.Sprintf("instances.Instance#precreateDatabase() ! No services found, no known nodes?!"))
			return err
		}
		body, err := json.Marshal(i)
		if err != nil {
			log.Error(fmt.Sprintf("instances.Instance#precreateDatabase() json.Marchal(i) ! %s", err))
			return err
		}
		url := fmt.Sprintf("http://%s:%s/%s", svcs[0].Address, os.Getenv("RDPGD_ADMIN_PORT"), `databases/assign`)
		req, err := http.NewRequest("PUT", url, bytes.NewBuffer([]byte(body)))
		log.Trace(fmt.Sprintf(`instances.Instance#precreateDatabase(%s) PUT %s`, i.Database, url))
		// req.Header.Set("Content-Type", "application/json")
		// TODO: Retrieve from configuration in database.
		req.SetBasicAuth(os.Getenv("RDPGD_ADMIN_USER"), os.Getenv("RDPGD_ADMIN_PASS"))
		httpClient := &http.Client{}
		resp, err := httpClient.Do(req)
		if err != nil {
			log.Error(fmt.Sprintf(`instances.Instance#precreateDatabase(%s) httpClient.Do() %s ! %s`, i.Database, url, err))
			return err
		}
		resp.Body.Close()
		// TODO: Trigger enqueueing of database creation on target cluster via AdminAPI.
		// TODO: Also have scheduler which enqueues if number precreated databases < 10
		break
	}
	return
}
