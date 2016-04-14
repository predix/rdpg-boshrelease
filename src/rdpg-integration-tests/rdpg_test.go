package integration_test

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	// "github.com/starkandwayne/rdpgd/cfsb"
	consulapi "github.com/hashicorp/consul/api"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/starkandwayne/rdpgd/cfsb"
	"github.com/starkandwayne/rdpgd/instances"
	"github.com/starkandwayne/rdpgd/pg"
	"github.com/starkandwayne/rdpgd/uuid"
)

func TestRDPGSystemIntegration(t *testing.T) {
	return
	config := consulapi.DefaultConfig()
	config.Address = `10.244.2.2:8500`
	consulClient, _ := consulapi.NewClient(config)
	kv := consulClient.KV()
	key := fmt.Sprintf(`rdpg/sc-pgbdr-m0-c0/capacity/instances/allowed`)
	kvp, _, _ := kv.Get(key, nil)
	capacityAllowedString := string(kvp.Value)
	capacityAllowed, _ := strconv.Atoi(capacityAllowedString)

	SkipConvey(`RDPG System, given four service clusters`, t, func() {
		Convey(`When (Capacity Size -1 (rdpg)) * 4 + 1 databases are assigned`, func() {

			pSize := os.Getenv(`RDPGD_POOL_SIZE`)
			fmt.Printf("pool size is :%s", pSize)
			poolSize, err := strconv.Atoi(pSize)
			So(err, ShouldBeNil)
			So(poolSize, ShouldEqual, 10)

			So(capacityAllowed, ShouldEqual, 12)

			numberInstance := 0

			organizationID := uuid.NewUUID().String()
			spaceID := uuid.NewUUID().String()

			req, err := http.NewRequest("GET", cfsbAPIURL(`/v2/catalog`), nil)
			So(err, ShouldBeNil)
			req.SetBasicAuth("cfadmin", "cfadmin")
			httpClient := &http.Client{}
			resp, err := httpClient.Do(req)
			So(err, ShouldBeNil)
			So(resp.StatusCode, ShouldEqual, http.StatusOK)
			fmt.Printf(`Getting Catalog`)

			decoder := json.NewDecoder(resp.Body)
			var c cfsb.Catalog
			err = decoder.Decode(&c)
			So(err, ShouldBeNil)

			serviceID := c.Services[0].ServiceID
			planID := c.Services[0].Plans[0].PlanID

			type InstanceBody struct {
				ServiceID      string `json:"service_id"`
				PlanID         string `json:"plan_id"`
				OrganizationID string `json:"organization_guid"`
				SpaceID        string `json:"space_guid"`
			}

			ins := &InstanceBody{
				ServiceID:      serviceID,
				PlanID:         planID,
				OrganizationID: organizationID,
				SpaceID:        spaceID,
			}

			scpgbdrm0c2Count := 0
			scpgbdrm0c3Count := 0
			scpgbdrm0c0Count := 0
			scpgbdrm0c1Count := 0

			time.Sleep(10 * time.Second) // Wait for precreated databases
			Convey("assigning Capacity Size * 4 +1  database, oldest available selected at each iteration", func() {
				for ps := 0; ps < capacityAllowed*4; ps++ {

					p := pg.NewPG(`10.244.2.2`, `7432`, `rdpg`, `rdpg`, `admin`)
					db, err := p.Connect()
					sq := fmt.Sprintf(`SELECT id,cluster_id,dbname FROM cfsb.instances WHERE instance_id IS NULL ORDER BY created_at ASC LIMIT 1`)
					iOldestAvailable := instances.Instance{}
					if ps < capacityAllowed*4 {
						for {
							err = db.Get(&iOldestAvailable, sq)
							if err == sql.ErrNoRows {
								time.Sleep(1 * time.Second) // Wait for a pre-created database to be ready.
								continue
							} else {
								break
							}
						}
					} else {
						err = db.Get(&iOldestAvailable, sq)
					}

					instanceID := uuid.NewUUID().String()
					insbody, err := json.Marshal(ins)
					So(err, ShouldBeNil)

					url := cfsbAPIURL("/v2/service_instances/" + instanceID)
					req, err := http.NewRequest("PUT", url, bytes.NewBuffer(insbody))
					req.SetBasicAuth("cfadmin", "cfadmin")

					fmt.Printf(`Assigning database # %d`, ps)
					httpClient := &http.Client{}
					resp, err := httpClient.Do(req)

					if ps == capacityAllowed*4 {
						fmt.Printf(`Assigning one more database than capacity, should says no reach capacity`)

						Convey("Trying to asign one more database than total capacity", func() {
							So(err, ShouldBeNil)
							So(resp.StatusCode, ShouldEqual, http.StatusInternalServerError)
							So(resp.Body, ShouldEqual, "status: 500,description: Provisioning failed, out of capacity. Need to increase capacity setting for service cluster through admin API or redeploy to scale out, please notify the operations team.")
						})
						Convey(`The number of instances should be equal with the instance capacity.`, func() {
							sq := fmt.Sprintf(`SELECT count(id) FROM cfsb.instances`)
							err = db.Get(numberInstance, sq)
							So(numberInstance, ShouldEqual, capacityAllowed)
						})
					}

					if ps < capacityAllowed*4 {
						So(err, ShouldBeNil)
						So(resp.StatusCode, ShouldEqual, http.StatusOK)
						fmt.Printf(`Assigning database!!!! # %d`, ps)

						time.Sleep(500 * time.Millisecond) // Wait a second for the transaction commit...

						i := instances.Instance{}
						sq := fmt.Sprintf(`SELECT id, cluster_id,instance_id, service_id, plan_id, organization_id, space_id, dbname, dbuser, dbpass FROM cfsb.instances WHERE instance_id=lower('%s') LIMIT 1`, instanceID)
						err = db.Get(&i, sq)

						So(i.ID, ShouldEqual, iOldestAvailable.ID)
						So(i.ClusterID, ShouldEqual, iOldestAvailable.ClusterID)
						So(i.Database, ShouldEqual, iOldestAvailable.Database)
						if i.ClusterID == "sc-pgbdr-m0-c2" {
							scpgbdrm0c2Count += 1
						}
						if i.ClusterID == "sc-pgbdr-m0-c3" {
							scpgbdrm0c3Count += 1
						}
						if i.ClusterID == "sc-pgbdr-m0-c0" {
							scpgbdrm0c0Count += 1
						}
						if i.ClusterID == "sc-pgbdr-m0-c1" {
							scpgbdrm0c1Count += 1
						}

						if ps == poolSize*3 {
							Convey(`Poolsize*3 +1 Databases should be assigned to all the service cluster.`, func() {
								// TODO: count the # assigned on each service cluster and
								// Both should be > 0
								So(scpgbdrm0c2Count, ShouldBeGreaterThan, 0)
								So(scpgbdrm0c3Count, ShouldBeGreaterThan, 0)
								So(scpgbdrm0c0Count, ShouldBeGreaterThan, 0)
								So(scpgbdrm0c1Count, ShouldBeGreaterThan, 0)
							})
						}

					}
				} // capacityAllowed for looooop
			})
		})
	})

}
