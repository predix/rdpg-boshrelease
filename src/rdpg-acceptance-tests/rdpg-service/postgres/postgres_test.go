package postgres_test

import (
	"fmt"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/starkandwayne/rdpg-acceptance-tests/rdpg-service/helper-functions"
)

var _ = Describe("RDPG Postgres Testing...", func() {

	It("Check Type of Service Node is set", func() {

		allNodes := GetAllNodes()

		//Check all nodes
		for i := 0; i < len(allNodes); i++ {
			serviceName := allNodes[i].ServiceName
			clusterService := GetClusterServiceType(serviceName)
			fmt.Printf("ServiceName: '%s', ClusterService: '%s'\n", serviceName, clusterService)
			Expect(len(clusterService)).To(BeNumerically(">", 0))
		}
	})

	It("Check Schemas Exist", func() {

		allNodes := GetAllNodes()

		for i := 0; i < len(allNodes); i++ {
			address := allNodes[i].Address
			clusterService := GetClusterServiceType(allNodes[i].ServiceName)
			sq := ` SELECT count(schema_name) as rowCount FROM information_schema.schemata WHERE schema_name IN ('bdr', 'rdpg', 'cfsb', 'tasks', 'backups', 'metrics', 'audit'); `
			rowCount, err := GetRowCount(address, sq)
			fmt.Printf("%s: Found %d schemas in rdpg database...\n", allNodes[i].Node, rowCount)
			Expect(err).NotTo(HaveOccurred())
			if clusterService == `pgbdr` {
				Expect(rowCount).To(Equal(7))
			} else {
				Expect(rowCount).To(Equal(6))
			}
		}
	})

	It("Check cfsb Tables Exist", func() {

		allNodes := GetAllNodes()

		for i := 0; i < len(allNodes); i++ {
			address := allNodes[i].Address
			sq := ` SELECT count(table_name) as rowCount FROM information_schema.tables WHERE table_schema = 'cfsb' and table_name IN ('services','plans','instances','bindings','credentials'); `
			rowCount, err := GetRowCount(address, sq)
			fmt.Printf("%s: Found %d tables in schema cfsb...\n", allNodes[i].Node, rowCount)
			Expect(err).NotTo(HaveOccurred())
			Expect(rowCount).To(Equal(5))
		}
	})

	It("Check rdpg Tables Exist", func() {

		allNodes := GetAllNodes()

		for i := 0; i < len(allNodes); i++ {
			address := allNodes[i].Address
			sq := ` SELECT count(table_name) as rowCount FROM information_schema.tables WHERE table_schema = 'rdpg' and table_name IN ('config', 'consul_watch_notifications', 'events'); `
			rowCount, err := GetRowCount(address, sq)
			fmt.Printf("%s: Found %d tables in schema rdpg...\n", allNodes[i].Node, rowCount)
			Expect(err).NotTo(HaveOccurred())
			Expect(rowCount).To(Equal(3))
		}

	})

	It("Check tasks Tables Exist", func() {
		allNodes := GetAllNodes()

		for i := 0; i < len(allNodes); i++ {
			address := allNodes[i].Address
			sq := ` SELECT count(table_name) as rowCount FROM information_schema.tables WHERE table_schema = 'tasks' and table_name IN ('tasks','schedules'); `
			rowCount, err := GetRowCount(address, sq)
			fmt.Printf("%s: Found %d tables in schema tasks...\n", allNodes[i].Node, rowCount)
			Expect(err).NotTo(HaveOccurred())
			Expect(rowCount).To(Equal(2))
		}

	})

	It("Check Instance Counts", func() {

		//Note to future self: stop trying to optimize this one, each of the service
		//service clusters can have a different number of active databases, so you
		//can't just lump them all together for the test...

		scInstanceCount := 0
		mcInstanceCount := 0
		allClusterNames := GetAllClusterNames()

		for _, key := range allClusterNames {
			fmt.Printf("service cluster name: %s...\n", key)
			tempClusterNodes := GetNodesByClusterName(key)
			var perclusterInstanceCount []int
			for i := 0; i < len(tempClusterNodes); i++ {
				address := tempClusterNodes[i].Address
				sq := ` SELECT count(*) as instance_count FROM cfsb.instances WHERE effective_at IS NOT NULL AND decommissioned_at IS NULL; `
				rowCount, err := GetRowCount(address, sq)
				perclusterInstanceCount = append(perclusterInstanceCount, rowCount)
				fmt.Printf("%s: Found %d instances...\n", tempClusterNodes[i].Node, rowCount)
				Expect(err).NotTo(HaveOccurred())
			}
			//Verify each database also sees the same number of records (bdr sanity check)
			for i := 1; i < len(perclusterInstanceCount); i++ {
				Expect(perclusterInstanceCount[0]).To(Equal(perclusterInstanceCount[i]))
			}
			if key == "rdpgmc" {
				mcInstanceCount = perclusterInstanceCount[0]
			} else {
				scInstanceCount = scInstanceCount + perclusterInstanceCount[0]
			}
		}
		fmt.Printf("scInstanceCount: %d, mcInstanceCount: %d", scInstanceCount, mcInstanceCount)
		//Verify that the number of instances seen in the Management Cluster is the
		//sum of the instances from the service ClusterIPs
		fmt.Printf("Total Management Cluster Instance Count: %d\n", mcInstanceCount)
		fmt.Printf("Total Service Cluster Instance Count: %d\n", scInstanceCount)
		Expect(mcInstanceCount).To(BeNumerically(">=", 3))
		Expect(mcInstanceCount).To(Equal(scInstanceCount))
	})

	It("Check Scheduled Tasks Exist", func() {

		allClusterNames := GetAllClusterNames()

		for _, key := range allClusterNames {
			tempClusterNodes := GetNodesByClusterName(key)
			if key == "rdpgmc" {
				var mcRowCount []int
				for i := 0; i < len(tempClusterNodes); i++ {
					address := tempClusterNodes[i].Address
					sq := ` SELECT count(*) AS rowCount FROM tasks.schedules WHERE role IN ('all', 'manager') AND action IN ('Vacuum','ReconcileAllDatabases','ReconcileAvailableDatabases'); `
					rowCount, err := GetRowCount(address, sq)
					mcRowCount = append(mcRowCount, rowCount)
					fmt.Printf("%s: Found %d scheduled tasks...\n", tempClusterNodes[i].Node, rowCount)
					Expect(err).NotTo(HaveOccurred())
				}
				//Verify each database also sees the same number of records (bdr sanity check)
				for i := 1; i < len(mcRowCount); i++ {
					Expect(mcRowCount[0]).To(Equal(mcRowCount[i]))
				}

				Expect(mcRowCount[0]).To(Equal(4)) //Note Vacuum is scheduled for both nodes

			} else {

				for i := 0; i < len(tempClusterNodes); i++ {
					address := tempClusterNodes[i].Address
					clusterService := GetClusterServiceType(tempClusterNodes[i].ServiceName)
					sq := ` SELECT count(*) AS rowCount FROM tasks.schedules WHERE role IN ('all', 'service') AND action IN ('Vacuum', 'DecommissionDatabases', 'PrecreateDatabases','ScheduleNewDatabaseBackups','DeleteBackupHistory'); `
					rowCount, err := GetRowCount(address, sq)
					fmt.Printf("%s: Found %d scheduled tasks...\n", tempClusterNodes[i].Node, rowCount)
					Expect(err).NotTo(HaveOccurred())
					if clusterService == `pgbdr` {
						Expect(rowCount).To(Equal(6))
					} else {
						Expect(rowCount).To(Equal(5))
					}
				}

			}
		}
	})

	It("Check For Missed Scheduled Tasks", func() {

		//Looks for any active scheduled tasks which have not been scheduled in two
		//frequency cycles.  This also validates the backups.

		allNodes := GetAllNodes()

		for i := 0; i < len(allNodes); i++ {
			address := allNodes[i].Address
			sq := ` SELECT count(*) as rowCount FROM tasks.schedules WHERE last_scheduled_at + (2 * frequency) < CURRENT_TIMESTAMP AND enabled=true; `
			rowCount, err := GetRowCount(address, sq)
			fmt.Printf("%s: Found %d missed scheduled tasks...\n", allNodes[i].Node, rowCount)
			Expect(err).NotTo(HaveOccurred())
			Expect(rowCount).To(Equal(0))
		}
	})

	It("Check for databases known to cfsb.instances but don't exist", func() {

		allNodes := GetServiceNodes()

		for i := 0; i < len(allNodes); i++ {
			address := allNodes[i].Address
			sq := `SELECT count(name) AS rowCount FROM ( (SELECT dbname AS name FROM cfsb.instances WHERE ineffective_at IS NULL) EXCEPT (SELECT datname AS name FROM pg_database WHERE datname LIKE 'd%') ) AS instances_missing_database;`
			rowCount, err := GetRowCount(address, sq)
			fmt.Printf("%s: Found %d databases known to cfsb.instances but don't exist...\n", allNodes[i].Node, rowCount)
			Expect(err).NotTo(HaveOccurred())
			Expect(rowCount).To(Equal(0))
		}

	})

	It("Check for databases which exist and aren't known to cfsb.instances", func() {

		allNodes := GetServiceNodes()

		for i := 0; i < len(allNodes); i++ {
			address := allNodes[i].Address
			sq := `SELECT count(name) as rowCount FROM ( (SELECT datname AS name FROM pg_database WHERE datname LIKE 'd%') EXCEPT (SELECT dbname AS name FROM cfsb.instances)) AS databases_missing_instances; `
			rowCount, err := GetRowCount(address, sq)
			fmt.Printf("%s: Found %d databases in pg not in cfsb.instances...\n", allNodes[i].Node, rowCount)
			Expect(err).NotTo(HaveOccurred())
			Expect(rowCount).To(Equal(0))
		}

	})

	It("Check For Stuck Tasks", func() {

		allNodes := GetAllNodes()

		for i := 0; i < len(allNodes); i++ {
			address := allNodes[i].Address
			sq := ` SELECT COUNT(*) as rowCount FROM tasks.tasks WHERE created_at < (CURRENT_TIMESTAMP - '6 hours'::interval); `
			rowCount, err := GetRowCount(address, sq)
			fmt.Printf("%s: Found %d stuck tasks...\n", allNodes[i].Node, rowCount)
			Expect(err).NotTo(HaveOccurred())
			Expect(rowCount).To(Equal(0))
		}

	})

	It("Check For Tasks with no node_type", func() {

		allNodes := GetAllNodes()

		for i := 0; i < len(allNodes); i++ {
			address := allNodes[i].Address
			sq := ` SELECT COUNT(*) as rowCount FROM tasks.tasks WHERE node_type IS NULL OR node_type = ''; `
			rowCount, err := GetRowCount(address, sq)
			fmt.Printf("%s: Found %d tasks with bad node_type value...\n", allNodes[i].Node, rowCount)

			Expect(rowCount).To(Equal(0))
			Expect(err).NotTo(HaveOccurred())
		}

	})

	It("Check rdpg system database is configured for bdr replication", func() {

		allClusterNames := GetAllClusterNames()

		for _, key := range allClusterNames {
			tempClusterNodes := GetNodesByClusterName(key)
			if key == "rdpgmc" {
				for i := 0; i < len(tempClusterNodes); i++ {
					address := tempClusterNodes[i].Address
					sq := ` SELECT COUNT(*) as rowCount FROM bdr.bdr_nodes; `
					rowCount, err := GetRowCount(address, sq)
					fmt.Printf("%s: Found %d bdr.bdr_node entries...\n", tempClusterNodes[i].Node, rowCount)
					Expect(rowCount).To(Equal(3))
					Expect(err).NotTo(HaveOccurred())
				}

			} else {

				for i := 0; i < len(tempClusterNodes); i++ {
					address := tempClusterNodes[i].Address
					clusterService := GetClusterServiceType(tempClusterNodes[i].ServiceName)

					if clusterService == `pgbdr` {
						sq := ` SELECT COUNT(*) as rowCount FROM bdr.bdr_nodes; `
						rowCount, err := GetRowCount(address, sq)
						fmt.Printf("%s: Found %d bdr.bdr_node entries...\n", tempClusterNodes[i].Node, rowCount)
						Expect(rowCount).To(Equal(2))
						Expect(err).NotTo(HaveOccurred())
					} else {
						fmt.Printf("Cluster %s is of type %s, skipped counting bdr.bdr_nodes entries \n", tempClusterNodes[i].Node, clusterService)
					}

				}
			}
		}

	})
})
