package user_databases_test

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/starkandwayne/rdpg-acceptance-tests/rdpg-service/helper-functions"
)

var _ = Describe("Testing of individual databases...", func() {

	It("Check all user databases have bdr pairs", func() {

		allNodes := GetServiceNodes()

		for i := 0; i < len(allNodes); i++ {
			address := allNodes[i].Address
			clusterService := GetClusterServiceType(allNodes[i].ServiceName)
			if clusterService == `pgbdr` {
				sq := `SELECT dbname AS name FROM cfsb.instances WHERE effective_at IS NOT NULL AND ineffective_at IS NULL AND decommissioned_at IS NULL; `

				//Get the list of user databases
				listDatabases, err := GetList(address, sq)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(listDatabases)).NotTo(Equal(0)) //Might snag on this for newly created clusters if PrecreateDatabases hasn't run yet

				//Connect to each db and check the number of nodes
				for _, databaseName := range listDatabases {
					sq = ` SELECT COUNT(*) as rowCount FROM bdr.bdr_nodes; `
					rowCount, err := GetRowCountUserDB(address, sq, databaseName)
					if rowCount != 2 {
						fmt.Printf("%s: Found %d bdr.bdr_nodes entries for database %s...\n", allNodes[i].Node, rowCount, databaseName)
					}
					Expect(err).NotTo(HaveOccurred())
					Expect(rowCount).To(Equal(2))
				}
			} else {
				fmt.Printf("Cluster %s is of type %s, skipped counting bdr.bdr_nodes entries \n", allNodes[i].Node, clusterService)
			}
		}

	})

	It("Check all user databases have the default extensions", func() {

		allNodes := GetServiceNodes()

		for i := 0; i < len(allNodes); i++ {
			address := allNodes[i].Address
			clusterService := GetClusterServiceType(allNodes[i].ServiceName)

			sq := `SELECT dbname AS name FROM cfsb.instances WHERE effective_at IS NOT NULL AND ineffective_at IS NULL AND decommissioned_at IS NULL; `

			//Get the list of user databases
			listDatabases, err := GetList(address, sq)
			Expect(err).NotTo(HaveOccurred())
			Expect(len(listDatabases)).NotTo(Equal(0)) //Might snag on this for newly created clusters if PrecreateDatabases hasn't run yet

			//Use this block if you want to roll out an extension to all existing databases
			/*
				if clusterService == `postgresql` {
					for _, databaseName := range listDatabases {
						sq = ` SELECT COUNT(*) as rowCount FROM pg_extension WHERE extname IN ('postgis'); `
						rowCount, err := GetRowCountUserDB(address, sq, databaseName)
						Expect(err).NotTo(HaveOccurred())
						if rowCount == 0 {
							sq = ` CREATE EXTENSION postgis; `
							err = ExecQueryUserDB(address, sq, databaseName)
							fmt.Printf("%s: Added extension pg_trgm to database %s...\n", allNodes[i].Node, databaseName)
							Expect(err).NotTo(HaveOccurred())
							time.Sleep(1 * time.Second)
						}
					}
				}
			*/

			for _, databaseName := range listDatabases {
				sq = ` SELECT COUNT(*) as rowCount FROM pg_extension WHERE extname IN ('pgcrypto'); `
				rowCount, err := GetRowCountUserDB(address, sq, databaseName)
				Expect(err).NotTo(HaveOccurred())
				if rowCount == 0 {
					sq = ` CREATE EXTENSION pgcrypto; `
					err = ExecQueryUserDB(address, sq, databaseName)
					fmt.Printf("%s: Added extension pgcrypto to database %s...\n", allNodes[i].Node, databaseName)
					Expect(err).NotTo(HaveOccurred())
					time.Sleep(1 * time.Second)
				}
			}

			expectedCount := 0
			if clusterService == `pgbdr` {
				sq = ` SELECT COUNT(*) as rowCount FROM pg_extension WHERE extname IN ('btree_gist', 'bdr', 'pg_stat_statements', 'uuid-ossp', 'hstore', 'pg_trgm', 'pgcrypto'); `
				expectedCount = 7
			} else if clusterService == `postgresql` {
				sq = ` SELECT COUNT(*) as rowCount FROM pg_extension WHERE extname IN('btree_gist', 'pg_stat_statements', 'uuid-ossp', 'hstore', 'pg_trgm', 'pgcrypto'); `
				expectedCount = 6
			}

			//Connect to each db and check the number of nodes
			for _, databaseName := range listDatabases {
				rowCount, err := GetRowCountUserDB(address, sq, databaseName)
				if rowCount != expectedCount {
					fmt.Printf("%s: Found %d of %d extensions for database %s...\n", allNodes[i].Node, rowCount, expectedCount, databaseName)
				}
				Expect(err).NotTo(HaveOccurred())
				Expect(rowCount).To(Equal(expectedCount))
			}

		}

	})

})
