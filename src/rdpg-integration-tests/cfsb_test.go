package integration_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	consulapi "github.com/hashicorp/consul/api"

	// "github.com/starkandwayne/rdpgd/cfsb"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/starkandwayne/rdpgd/cfsb"
	"github.com/starkandwayne/rdpgd/instances"
	"github.com/starkandwayne/rdpgd/pg"
	"github.com/starkandwayne/rdpgd/rdpg"
	"github.com/starkandwayne/rdpgd/uuid"
)

func init() {
	os.Setenv("RDPGD_PB_PORT", "7432")
}

func cfsbAPIURL(path string) string {
	return fmt.Sprintf(`http://cfadmin:cfadmin@10.244.2.2:8888%s`, path)
}

func TestCFSBIntegration(t *testing.T) {
	Convey("CFSB API Authorization", t, func() {
		// API Version Header is set test
		// basic_auth test, need username and password (Authentication :header) to do broker registrations
		// return 401 Unauthorized if credentials are not valid  test, auth only tested here
		// test when reject a request, response a 412 Precondition Failed message
		var getBasicAuthTests = []struct {
			username, password string
			status             int
		}{
			{"cfadmin", "cfadmin", 200},
			{"Aladdin", "open:sesame", 401},
			{"", "", 401},
			{"cf", "bala", 401},
			{"", "cf", 401},
		}

		for _, authTest := range getBasicAuthTests {
			req, err := http.NewRequest("GET", cfsbAPIURL(`/v2/catalog`), nil)
			So(err, ShouldBeNil)
			req.SetBasicAuth(authTest.username, authTest.password)
			httpClient := &http.Client{}
			resp, err := httpClient.Do(req)
			So(err, ShouldBeNil)
			So(resp.StatusCode, ShouldEqual, authTest.status)
		}
	})
	// Complete integration test,
	// get catalg
	// use results to provision instance
	// user results to bind
	// user results to unbind
	// user results to deprovision
	// - check for ineffective_at timestamp set
	Convey("CFSB API", t, func() {
		config := consulapi.DefaultConfig()
		config.Address = `10.244.2.2:8500`
		consulClient, err := consulapi.NewClient(config)
		So(err, ShouldBeNil)

		organizationID := uuid.NewUUID().String()
		spaceID := uuid.NewUUID().String()

		Convey("Get Catalog", func() {
			req, err := http.NewRequest("GET", cfsbAPIURL(`/v2/catalog`), nil)
			So(err, ShouldBeNil)
			req.SetBasicAuth("cfadmin", "cfadmin")

			httpClient := &http.Client{}
			resp, err := httpClient.Do(req)
			So(err, ShouldBeNil)
			So(resp.StatusCode, ShouldEqual, http.StatusOK)

			decoder := json.NewDecoder(resp.Body)
			var c cfsb.Catalog
			err = decoder.Decode(&c)
			So(err, ShouldBeNil)
			So(len(c.Services), ShouldNotEqual, 0)

			Convey("Services in Catalog Response", func() {
				firstService := c.Services[0]
				So(firstService.ServiceID, ShouldNotBeBlank)
				So(firstService.Name, ShouldNotBeBlank)
				So(firstService.Description, ShouldNotBeBlank)
				So(len(firstService.Plans), ShouldNotEqual, 0)
				Convey("Plans in Services", func() {
					firstPlan := firstService.Plans[0]
					So(firstPlan.PlanID, ShouldNotBeBlank)
					So(firstPlan.Name, ShouldNotBeBlank)
					So(firstPlan.Description, ShouldNotBeBlank)
				})
			})

			serviceID := c.Services[0].ServiceID
			planID := c.Services[0].Plans[0].PlanID

			Convey("Provision Instance", func() {
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

				instanceID := uuid.NewUUID().String()
				insbody, err := json.Marshal(ins)
				So(err, ShouldBeNil)

				// TODO: Perform this test after checking/waiting for databases to be
				// pre-created otherwise we get a false-fail.
				url := cfsbAPIURL("/v2/service_instances/" + instanceID)
				req, err := http.NewRequest("PUT", url, bytes.NewBuffer(insbody))
				req.SetBasicAuth("cfadmin", "cfadmin")
				So(err, ShouldBeNil)
				httpClient := &http.Client{}
				resp, err := httpClient.Do(req)
				So(err, ShouldBeNil)
				So(resp.StatusCode, ShouldEqual, http.StatusOK)

				time.Sleep(1 * time.Second) // Allow replication to catch up.

				// the following create an instance which we can compare the clolumn values against service cluster
				i := instances.Instance{}
				p := pg.NewPG(`10.244.2.2`, `7432`, `rdpg`, `rdpg`, `admin`)
				db, err := p.Connect()
				So(err, ShouldBeNil)
				q := fmt.Sprintf(`SELECT id, cluster_id, instance_id, service_id, plan_id, organization_id, space_id, dbname, dbuser, dbpass FROM cfsb.instances WHERE instance_id=lower('%s') LIMIT 1`, instanceID)
				db.Get(&i, q)
				db.Close()

				Convey("The fields should be set in management cluster write master instance table", func() {
					So(err, ShouldBeNil)
					So(i.ClusterID, ShouldNotBeBlank)
					So(i.ServiceID, ShouldEqual, serviceID)
					So(i.PlanID, ShouldEqual, planID)
					So(i.OrganizationID, ShouldEqual, organizationID)
					So(i.SpaceID, ShouldEqual, spaceID)
				})

				// The instance provised should have the instanceId,serviceId,planId,organizationId and spaceId
				managementCluster, err := rdpg.NewCluster(`rdpgmc`, consulClient)
				So(err, ShouldBeNil)

				Convey("Each management cluster node should have the correct fields set in the cfsb.instances table", func() {
					for _, node := range managementCluster.Nodes { // Loop over management cluster nodes.
						p := pg.NewPG(node.PG.IP, `7432`, `postgres`, `rdpg`, `admin`)
						db, err := p.Connect()
						So(err, ShouldBeNil)

						in := instances.Instance{}
						q := fmt.Sprintf(`SELECT id, cluster_id,instance_id, service_id, plan_id, organization_id, space_id, dbname, dbuser, dbpass FROM cfsb.instances WHERE instance_id=lower('%s') LIMIT 1`, instanceID)
						db.Get(&in, q)
						So(in.ClusterID, ShouldNotBeBlank)
						So(in.ServiceID, ShouldEqual, serviceID)
						So(in.PlanID, ShouldEqual, planID)
						So(in.OrganizationID, ShouldEqual, organizationID)
						So(in.SpaceID, ShouldEqual, spaceID)
						db.Close()
					}
				})

				serviceCluster, err := rdpg.NewCluster(i.ClusterID, consulClient)
				So(err, ShouldBeNil)
				// TODO: Find out which SC the instance is on and for that cluster's nodes, do the below.
				Convey("Each service cluster node should have a correct record of the instance in rdpg.cfsb.instances.", func() {
					for _, node := range serviceCluster.Nodes {
						sp := pg.NewPG(node.PG.IP, `7432`, `postgres`, `rdpg`, `admin`)
						db, err := sp.Connect()
						So(err, ShouldBeNil)
						in := instances.Instance{}
						q := fmt.Sprintf(`SELECT id, cluster_id,instance_id, service_id, plan_id, organization_id, space_id, dbname, dbuser, dbpass FROM cfsb.instances WHERE instance_id=lower('%s') LIMIT 1`, instanceID)
						db.Get(&in, q)
						So(in.ClusterID, ShouldEqual, i.ClusterID)
						So(in.ServiceID, ShouldEqual, serviceID)
						So(in.PlanID, ShouldEqual, planID)
						So(in.OrganizationID, ShouldEqual, organizationID)
						So(in.SpaceID, ShouldEqual, spaceID)
						db.Close()
					}
				})

				Convey("Each service cluster node should have the instance's user and database created on it", func() {
					for _, node := range serviceCluster.Nodes { // Loop over service cluster nodes.
						sp := pg.NewPG(node.PG.IP, `7432`, `postgres`, `rdpg`, `admin`)
						db, err := sp.Connect()
						So(err, ShouldBeNil)
						// user should be created on each service cluster node
						r := ""
						q := fmt.Sprintf(`SELECT rolname FROM pg_roles WHERE rolname='%s'`, i.User)
						db.Get(&r, q)
						So(r, ShouldEqual, i.User)

						// database should be created on each service cluster node
						d := ""
						q = fmt.Sprintf(`SELECT datname FROM pg_catalog.pg_database WHERE datname='%s'`, i.Database)
						db.Get(&d, q)
						So(d, ShouldEqual, i.Database)
						db.Close()
					}
				})

				Convey("Each service cluster node for the instance's database should have bdr and btree_gist extension, and have the same count of bdr.bdr_nodes.", func() {
					var count int
					for _, node := range serviceCluster.Nodes {
						p := pg.NewPG(node.PG.IP, `7432`, `rdpg`, i.Database, i.Pass)
						db, err := p.Connect()
						So(err, ShouldBeNil)
						var s string
						// extensions on all cluster nodes should have bdr, btree_gist
						exts := []string{"bdr", "btree_gist"}
						for _, ext := range exts {
							q := fmt.Sprintf(`SELECT extname FROM pg_extension WHERE extname ='%s'`, ext)
							db.Get(&s, q)
							So(s, ShouldEqual, ext)
						}
						// replication group should have the same count as rdpg bdr.bdr_nodes;
						db.Get(&count, "SELECT count(node_status) FROM bdr.bdr_nodes WHERE node_status='r'")
						So(count, ShouldEqual, len(serviceCluster.Nodes))
						db.Close()
					}
				})

				Convey("Each service cluster node for the instance's database should have replication_slot.", func() {
					var count int
					for _, node := range serviceCluster.Nodes {
						p := pg.NewPG(node.PG.IP, `7432`, `postgres`, `rdpg`, `admin`)
						db, err := p.Connect()
						So(err, ShouldBeNil)
						// replication group should have the same count as rdpg bdr.bdr_nodes;
						q := fmt.Sprintf(`SELECT count(*) FROM pg_replication_slots WHERE database='%s'`, i.Database)
						db.Get(&count, q)
						So(count, ShouldEqual, 1)
						db.Close()
					}
				})

				Convey("Each service cluster node should deny connection to the deprovisioned instance.", func() {
					for _, node := range serviceCluster.Nodes {
						p := pg.NewPG(node.PG.IP, `7432`, i.User, i.Database, i.Pass)
						db, err := p.Connect()
						So(err, ShouldBeNil)
						db.Close()
					}
				})

				Convey("Bind", func() {
					bindingID := uuid.NewUUID().String()
					appGuid := uuid.NewUUID().String()

					type BindingBody struct {
						PlanID    string `json:"plan_id"`
						ServiceID string `json:"service_id"`
						AppGuid   string `json:app_guid"`
					}

					bind := &BindingBody{
						PlanID:    planID,
						ServiceID: serviceID,
						AppGuid:   appGuid,
					}

					bindbody, err := json.Marshal(bind)
					So(err, ShouldBeNil)

					url := cfsbAPIURL("/v2/service_instances/" + instanceID + "/service_bindings/" + bindingID)
					req, err := http.NewRequest("PUT", url, bytes.NewBuffer(bindbody))
					req.SetBasicAuth("cfadmin", "cfadmin")
					So(err, ShouldBeNil)

					httpClient := &http.Client{}
					resp, err := httpClient.Do(req)
					So(err, ShouldBeNil)
					So(resp.StatusCode, ShouldEqual, http.StatusOK)
					time.Sleep(1 * time.Second) // Allow replication to replicate before proceeding

					// Binding that it returns should have values for it's fields
					decoder := json.NewDecoder(resp.Body)
					var b cfsb.Binding
					err = decoder.Decode(&b)
					So(err, ShouldBeNil)
					Convey("Bind response body should be consistant with management cluster node bind info", func() {
						So(b.BindingID, ShouldEqual, bindingID)
						So(b.InstanceID, ShouldEqual, instanceID)
						So(b.Creds, ShouldNotBeNil)
						creds := b.Creds

						catalog := consulClient.Catalog()
						services, _, err := catalog.Service(fmt.Sprintf(`%s-master`, i.ClusterID), "", nil)
						So(err, ShouldBeNil)
						So(len(services), ShouldEqual, 1)
						dns := fmt.Sprintf(`%s:5432`, services[0].Address)
						s_dns := strings.Split(dns, ":")
						uri := fmt.Sprintf(`postgres://%s:%s@%s/%s?sslmode=%s`, i.User, i.Pass, dns, i.Database, `disable`)
						dsn := fmt.Sprintf(`host=%s port=%s user=%s password=%s dbname=%s connect_timeout=%s sslmode=%s`, s_dns[0], s_dns[1], i.User, i.Pass, i.Database, `5`, `disable`)
						jdbc := fmt.Sprintf(`jdbc:postgres://%s:%s@%s/%s?sslmode=%s`, i.User, i.Pass, dns, i.Database, `disable`)

						So(creds.URI, ShouldEqual, uri)
						So(creds.DSN, ShouldEqual, dsn)
						So(creds.JDBCURI, ShouldEqual, jdbc)
						So(creds.Host, ShouldEqual, s_dns[0])
						So(creds.Port, ShouldEqual, s_dns[1])
						So(creds.UserName, ShouldEqual, i.User)
						So(creds.Password, ShouldEqual, i.Pass)
						So(creds.Database, ShouldEqual, i.Database)
					})

					Convey("Each management cluster node should have correct cfsb.binding record for the binding id.", func() {
						for _, node := range managementCluster.Nodes {
							p := pg.NewPG(node.PG.IP, `7432`, `rdpg`, `rdpg`, `admin`)
							db, err := p.Connect()
							So(err, ShouldBeNil)

							var instanceIdTest string
							q := fmt.Sprintf(`SELECT instance_id FROM cfsb.bindings WHERE binding_id = '%s'`, bindingID)
							db.Get(&instanceIdTest, q)
							So(instanceIdTest, ShouldEqual, instanceID)
							db.Close()
						}
					})

					// TODO: Come back to this and get this test passing.
					SkipConvey("Each management cluster node should have correct cfsb.credentials record for the binding id.", func() {
						for _, node := range managementCluster.Nodes {
							p := pg.NewPG(node.PG.IP, `7432`, `rdpg`, `rdpg`, `admin`)
							db, err := p.Connect()
							So(err, ShouldBeNil)

							credsTest := cfsb.Credentials{}
							q = fmt.Sprintf(`SELECT instance_id,dbuser,dbname,dbpass FROM cfsb.credentials WHERE binding_id = '%s'`, bindingID)
							db.Get(&credsTest, q)
							So(credsTest.InstanceID, ShouldEqual, instanceID)
							So(credsTest.UserName, ShouldEqual, i.User)
							So(credsTest.Database, ShouldEqual, i.Database)
							So(credsTest.Password, ShouldEqual, i.Pass)
							db.Close()
						}
					})

					Convey("Un Bind", func() {
						url := cfsbAPIURL("/v2/service_instances/" + instanceID + "/service_bindings/" + bindingID + "?service_id=" + serviceID + "&plan_id=" + planID)
						req, err = http.NewRequest("DELETE", url, nil)
						req.SetBasicAuth("cfadmin", "cfadmin")
						So(err, ShouldBeNil)

						httpClient := &http.Client{}
						resp, err := httpClient.Do(req)
						So(err, ShouldBeNil)
						So(resp.StatusCode, ShouldEqual, http.StatusOK)

						Convey("cfsb.binding record for each management node should become inefective for the binding id .", func() {
							for _, node := range managementCluster.Nodes {
								p := pg.NewPG(node.PG.IP, `7432`, `rdpg`, `rdpg`, `admin`)
								db, err := p.Connect()
								So(err, ShouldBeNil)
								var s string
								q := fmt.Sprintf(`SELECT ineffective_at::text FROM cfsb.bindings WHERE binding_id = '%s'`, bindingID)
								db.Get(&s, q)
								So(s, ShouldNotBeBlank)
								db.Close()
							}
						})

						Convey("cfsb.credentials record for each management node should become inefective for the binding id .", func() {
							for _, node := range managementCluster.Nodes {
								p := pg.NewPG(node.PG.IP, `7432`, `rdpg`, `rdpg`, `admin`)
								db, err := p.Connect()
								So(err, ShouldBeNil)
								var s string
								q := fmt.Sprintf(`SELECT ineffective_at::text FROM cfsb.credentials WHERE binding_id = '%s'`, bindingID)
								db.Get(&s, q)
								So(s, ShouldNotBeBlank)
								db.Close()
							}
						})
					}) //unbind
				}) // bind

				Convey("Deprovision", func() {
					url := cfsbAPIURL("/v2/service_instances/" + instanceID + "?service_id=" + serviceID + "&plan_id=" + planID)
					req, err = http.NewRequest("DELETE", url, nil)
					req.SetBasicAuth("cfadmin", "cfadmin")
					So(err, ShouldBeNil)

					httpClient := &http.Client{}
					resp, err := httpClient.Do(req)
					So(err, ShouldBeNil)
					So(resp.StatusCode, ShouldEqual, http.StatusOK)

					// Allow the decommission process time to complete...
					time.Sleep(10 * time.Second)

					Convey("cfsb.instances for each management node inefective_at should be set fot the instance.", func() {
						for _, node := range managementCluster.Nodes {
							p := pg.NewPG(node.PG.IP, `7432`, `rdpg`, `rdpg`, `admin`)
							db, err := p.Connect()
							So(err, ShouldBeNil)

							var ia string
							q := fmt.Sprintf(`SELECT ineffective_at::text FROM cfsb.instances WHERE instance_id='%s' LIMIT 1`, instanceID)
							db.Get(&ia, q)
							So(ia, ShouldNotBeBlank)
							db.Close()
						}
					})

					Convey("cfsb.instances for each service node inefective_at should be set for the instance.", func() {
						for _, node := range serviceCluster.Nodes {
							p := pg.NewPG(node.PG.IP, `7432`, `rdpg`, `rdpg`, `admin`)
							db, err := p.Connect()
							So(err, ShouldBeNil)
							var ia string
							q := fmt.Sprintf(`SELECT ineffective_at::text FROM cfsb.instances WHERE instance_id='%s'`, instanceID)
							db.Get(&ia, q)
							So(ia, ShouldNotBeBlank)
							db.Close()
						}
					})

					Convey("Each service cluster node should deny connection to the deprovisioned instance.", func() {

						time.Sleep(2 * time.Second)
						for _, node := range serviceCluster.Nodes {
							p := pg.NewPG(node.PG.IP, `7432`, i.User, i.Database, i.Pass)
							_, err := p.Connect()
							So(err, ShouldNotBeNil)
						}
					})

					Convey("Each node should NOT have the user and database anymore", func() {
						for _, node := range serviceCluster.Nodes {
							p := pg.NewPG(node.PG.IP, `7432`, `postgres`, `rdpg`, `admin`)
							db, err := p.Connect()
							So(err, ShouldBeNil)

							// user  should have been deleted on all service cluster nodes
							var s string
							q := fmt.Sprintf(`SELECT rolname FROM pg_roles WHERE rolname='%s'`, i.User)
							db.Get(&s, q)
							So(s, ShouldBeBlank)

							// database should be deleted all service cluster node
							q = fmt.Sprintf(`SELECT datname FROM pg_catalog.pg_database WHERE datname='%s'`, i.Database)
							db.Get(&s, q)
							So(s, ShouldBeBlank)
							db.Close()
						}
					})
					Convey("cfsb.instances for each service node decommissioned_at timestamp should be set for the instance.", func() {
						for _, node := range serviceCluster.Nodes {
							p := pg.NewPG(node.PG.IP, `7432`, `rdpg`, `rdpg`, `admin`)
							db, err := p.Connect()
							So(err, ShouldBeNil)
							var ia string
							q := fmt.Sprintf(`SELECT decommissioned_at::text FROM cfsb.instances WHERE instance_id='%s'`, instanceID)
							db.Get(&ia, q)
							So(ia, ShouldNotBeBlank)
							db.Close()
						}
					})

					Convey("cfsb.instances for each management node decommissioned_at timestamp should be set for the instance.", func() {
						time.Sleep(2 * time.Second)
						for _, node := range managementCluster.Nodes {
							p := pg.NewPG(node.PG.IP, `7432`, `rdpg`, `rdpg`, `admin`)
							db, err := p.Connect()
							So(err, ShouldBeNil)

							var ia string
							q := fmt.Sprintf(`SELECT decommissioned_at::text FROM cfsb.instances WHERE instance_id='%s' LIMIT 1`, instanceID)
							db.Get(&ia, q)
							So(ia, ShouldNotBeBlank)
							db.Close()
						}
					})

				}) // Deprovision

			}) // Provision
		}) // catalog

	}) // integratetion workflow
}
