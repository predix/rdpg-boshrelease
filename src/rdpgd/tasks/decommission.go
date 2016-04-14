package tasks

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	consulapi "github.com/hashicorp/consul/api"
	"github.com/starkandwayne/rdpgd/bdr"
	"github.com/starkandwayne/rdpgd/globals"
	"github.com/starkandwayne/rdpgd/instances"
	"github.com/starkandwayne/rdpgd/log"
	"github.com/starkandwayne/rdpgd/pg"
)

//DecommissionDatabase - Remove targeted database specified in Data
func (t *Task) DecommissionDatabase() (err error) {
	log.Trace(fmt.Sprintf(`tasks.DecommissionDatabase(%s)...`, t.Data))

	i, err := instances.FindByDatabase(t.Data)
	if err != nil {
		log.Error(fmt.Sprintf("tasks.DecommissionDatabase(%s) instances.FindByDatabase() ! %s", i.Database, err))
		return err
	}
	//TODO: Check if i == nil; i.e. if database doesn't exist

	ips, err := i.ClusterIPs()
	if err != nil {
		log.Error(fmt.Sprintf(`tasks.Task#DecommissionDatabase(%s) i.ClusterIPs() ! %s`, i.Database, err))
		return err
	}
	if len(ips) == 0 {
		log.Error(fmt.Sprintf("tasks.Task#DecommissionDatabase(%s) ! No service cluster nodes found in Consul?!", i.Database))
		return
	}
	p := pg.NewPG(`127.0.0.1`, pbPort, `rdpg`, `rdpg`, pgPass)
	db, err := p.Connect()
	if err != nil {
		log.Error(fmt.Sprintf("tasks.Task#DecommissionDatabase(%s) p.Connect(%s) ! %s", t.Data, p.URI, err))
		return err
	}
	defer db.Close()

	switch globals.ServiceRole {
	case "manager":
		path := fmt.Sprintf(`databases/decommission/%s`, t.Data)
		url := fmt.Sprintf("http://%s:%s/%s", ips[0], os.Getenv("RDPGD_ADMIN_PORT"), path)
		req, err := http.NewRequest("DELETE", url, bytes.NewBuffer([]byte("{}")))
		log.Trace(fmt.Sprintf(`tasks.Task#Decommission() > DELETE %s`, url))
		//req.Header.Set("Content-Type", "application/json")
		// TODO: Retrieve from configuration in database.
		req.SetBasicAuth(os.Getenv("RDPGD_ADMIN_USER"), os.Getenv("RDPGD_ADMIN_PASS"))
		httpClient := &http.Client{}
		_, err = httpClient.Do(req)
		if err != nil {
			log.Error(fmt.Sprintf(`tasks.Task#DecommissionDatabase(%s) httpClient.Do() %s ! %s`, i.Database, url, err))
			return err
		}
		// TODO: Is there anything we want to do on successful request?
	case "service":
		// In here we must do everything necessary to physically delete and clean up
		// the database from all service cluster nodes.
		if err = t.BackupDatabase(); err != nil {
			log.Error(fmt.Sprintf(`tasks.Task#DecommissionDatabase(%s) t.BackupDatabase(%s) ! %s`, i.Database, err))
		} else {
			for _, ip := range ips { // Schedule pgbouncer reconfigure on each cluster node.
				newTask := Task{ClusterID: ClusterID, Node: ip, Role: "all", Action: "Reconfigure", Data: "pgbouncer", NodeType: "any"}
				err = newTask.Enqueue()
				if err != nil {
					log.Error(fmt.Sprintf(`tasks.Task#DecommissionDatabase(%s) Reconfigure PGBouncer! %s`, i.Database, err))
				}
			}
			log.Trace(fmt.Sprintf(`tasks.DecommissionDatabase(%s) TODO: Here is where we finally decommission on the service cluster...`, i.Database))

			client, err := consulapi.NewClient(consulapi.DefaultConfig())
			if err != nil {
				log.Error(fmt.Sprintf("tasks.Task#DecommissionDatabase(%s) consulapi.NewClient() ! %s", i.Database, err))
				return err
			}

			// Lock Database Deletion via Consul Lock
			key := fmt.Sprintf(`rdpg/%s/database/existance/lock`, t.ClusterID)
			lo := &consulapi.LockOptions{
				Key:         key,
				SessionName: fmt.Sprintf(`rdpg/%s/databases/existance`, t.ClusterID),
			}
			log.Trace(fmt.Sprintf(`tasks.Task<%s>#DecommissionDatabase() Attempting to acquire database existance lock %s...`, t.ClusterID, key))
			databaseCreateLock, err := client.LockOpts(lo)
			if err != nil {
				log.Error(fmt.Sprintf(`tasks.Task<%s>#DecommissionDatabase() LockKey() database/existance Lock Key %s ! %s`, t.ClusterID, key, err))
				return err
			}
			databaseCreateLockCh, err := databaseCreateLock.Lock(nil)
			if err != nil {
				log.Error(fmt.Sprintf(`tasks.Task<%s>#DecommissionDatabase() Lock() database/existance lock %s ! %s`, t.ClusterID, key, err))
				return err
			}
			if databaseCreateLockCh == nil {
				err := fmt.Errorf(`tasks.Task<%s>#DecommissionDatabase() database/existance Lock not aquired, halting Decommission!!!`, t.ClusterID)
				log.Error(err.Error())
				return err
			}
			defer databaseCreateLock.Unlock()

			p := pg.NewPG(`127.0.0.1`, pbPort, `rdpg`, `rdpg`, pgPass)
			db, err := p.Connect()
			if err != nil {
				log.Error(fmt.Sprintf("instances.Decommission() p.Connect(%s) ! %s", p.URI, err))
				return err
			}
			defer db.Close()

			sq := fmt.Sprintf(`DELETE FROM tasks.tasks WHERE action='BackupDatabase' AND data='%s'`, i.Database)
			log.Trace(fmt.Sprintf(`tasks.Task#DecommissionDatabase(%s) SQL > %s`, i.Database, sq))
			_, err = db.Exec(sq)
			if err != nil {
				log.Error(fmt.Sprintf("tasks.Task#DecommissionDatabase(%s) ! %s", i.Database, err))
			}
			sq = fmt.Sprintf(`UPDATE tasks.schedules SET enabled = false WHERE action='BackupDatabase' AND data='%s'`, i.Database)
			log.Trace(fmt.Sprintf(`tasks.Task#DecommissionDatabase(%s) SQL > %s`, i.Database, sq))
			_, err = db.Exec(sq)
			if err != nil {
				log.Error(fmt.Sprintf("tasks.Task#DecommissionDatabase(%s) ! %s", i.Database, err))
			}

			if t.ClusterService == "pgbdr" {
				b := bdr.NewBDR(ClusterID, client)
				b.DropDatabase(i.Database)

				dbuser := ""
				sq = fmt.Sprintf(`SELECT dbuser FROM cfsb.instances WHERE dbname='%s' LIMIT 1`, i.Database)
				log.Trace(fmt.Sprintf(`tasks.Task#DecommissionDatabase(%s) SQL > %s`, i.Database, sq))
				err = db.Get(&dbuser, sq)
				if err != nil {
					log.Error(fmt.Sprintf("tasks.Task#DecommissionDatabase(%s) ! %s", i.Database, err))
				}
				b.DropUser(dbuser)

				sq = fmt.Sprintf(`UPDATE cfsb.instances SET decommissioned_at=CURRENT_TIMESTAMP WHERE dbname='%s'`, i.Database)
				log.Trace(fmt.Sprintf(`tasks.Task#DecommissionDatabase(%s) SQL > %s`, i.Database, sq))
				_, err = db.Exec(sq)
				if err != nil {
					log.Error(fmt.Sprintf("tasks.Task#DecommissionDatabase(%s) ! %s", i.Database, err))
				}

			} else {
				p.DisableDatabase(i.Database)
				p.DropDatabase(i.Database)

				dbuser := ""
				sq = fmt.Sprintf(`SELECT dbuser FROM cfsb.instances WHERE dbname='%s' LIMIT 1`, i.Database)
				log.Trace(fmt.Sprintf(`tasks.Task#DecommissionDatabase(%s) SQL > %s`, i.Database, sq))
				err = db.Get(&dbuser, sq)
				if err != nil {
					log.Error(fmt.Sprintf("tasks.Task#DecommissionDatabase(%s) ! %s", i.Database, err))
				}
				p.DropUser(dbuser)

				sq = fmt.Sprintf(`UPDATE cfsb.instances SET decommissioned_at=CURRENT_TIMESTAMP WHERE dbname='%s'`, i.Database)
				log.Trace(fmt.Sprintf(`tasks.Task#DecommissionDatabase(%s) SQL > %s`, i.Database, sq))
				_, err = db.Exec(sq)
				if err != nil {
					log.Error(fmt.Sprintf("tasks.Task#DecommissionDatabase(%s) ! %s", i.Database, err))
				}

			}

			// Notify management cluster that the instance has been decommissioned
			// Find management cluster API address
			catalog := client.Catalog()
			svcs, _, err := catalog.Service(`rdpgmc`, "", nil)
			if err != nil {
				log.Error(fmt.Sprintf("tasks.Task#DecommissionDatabase(%s) consulapi.Client.Catalog() ! %s", i.Database, err))
				return err
			}
			if len(svcs) == 0 {
				log.Error(fmt.Sprintf("tasks.Task#DecommissionDatabase(%s) ! No services found, no known nodes?!", i.Database))
				return err
			}
			mgtAPIIPAddress := svcs[0].Address

			// Query the database for the decommissioned_at timestamp set
			timestamp := ""
			sq = fmt.Sprintf(`SELECT decommissioned_at::text FROM cfsb.instances WHERE dbname='%s' LIMIT 1;`, i.Database)
			db.Get(&timestamp, sq)

			type decomm struct {
				Database  string `json:"database"`
				Timestamp string `json:"timestamp"`
			}
			dc := decomm{Database: i.Database, Timestamp: timestamp}
			// Tell the management cluster (via admin api) about the timestamp.
			url := fmt.Sprintf("http://%s:%s/%s", mgtAPIIPAddress, os.Getenv("RDPGD_ADMIN_PORT"), `databases/decommissioned`)
			body, err := json.Marshal(dc)
			if err != nil {
				log.Error(fmt.Sprintf("tasks.Task#DecommissionDatabase(%s) json.Marchal(i) ! %s", i.Database, err))
				return err
			}
			req, err := http.NewRequest("PUT", url, bytes.NewBuffer([]byte(body)))
			log.Trace(fmt.Sprintf(`tasks.Task#DecommissionDatabase(%s) PUT %s body: %s`, i.Database, url, body))
			req.SetBasicAuth(os.Getenv("RDPGD_ADMIN_USER"), os.Getenv("RDPGD_ADMIN_PASS"))
			httpClient := &http.Client{}
			resp, err := httpClient.Do(req)
			if err != nil {
				log.Error(fmt.Sprintf(`tasks.Task#DecommissionDatabase(%s) httpClient.Do() PUT %s ! %s`, i.Database, url, err))
				return err
			}
			resp.Body.Close()
		}
		return nil
	default:
		log.Error(fmt.Sprintf(`tasks.Task#DecommissionDatabase(%s) ! Unknown work role: '%s' -> BUG!!!`, i.Database, globals.ServiceRole))
		return nil
	}
	return
}

// DecommissionDatabases - Scheduled task to find and decommission databases that
// require decommissioning eg. Clean up databases which users have declared they
// no longer need/love
func (t *Task) DecommissionDatabases() (err error) {
	// eg. Look for databases that that should have been decommissioned and instert
	// a DecommissionDatabase task to target each database found.
	log.Trace(fmt.Sprintf(`tasks.DecommissionDatabases(%s) TODO: Regularly scheduled maintenance removal of databases...`, t.Data))
	return
}
