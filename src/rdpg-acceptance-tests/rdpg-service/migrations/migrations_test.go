package migrations_test

import (
	"fmt"
	"os"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/starkandwayne/rdpgd/pg"

	. "github.com/starkandwayne/rdpg-acceptance-tests/rdpg-service/helper-functions"
)

type Schedule struct {
	ID             int64  `db:"id" json:"id"`
	ClusterID      string `db:"cluster_id" json:"cluster_id"`
	ClusterService string `db:"cluster_service" json:"cluster_service"`
	Role           string `db:"role" json:"role"`
	Action         string `db:"action" json:"action"`
	Data           string `db:"data" json:"data"`
	TTL            int64  `db:"ttl" json:"ttl"`
	NodeType       string `db:"node_type" json:"node_type"`
	Frequency      string `db:"frequency" json:"frequency"`
	Enabled        bool   `db:"enabled" json:"enabled"`
}

//Add - Insert a new schedule into tasks.schedules
func (s *Schedule) Add(address string) (err error) {
	p := pg.NewPG(address, "7432", `rdpg`, `rdpg`, "admin")
	p.Set(`database`, `rdpg`)

	scheduleDB, err := p.Connect()
	if err != nil {
		fmt.Printf(`tasks.Schedule.Add() Could not open connection ! %s`, err)
	}

	defer scheduleDB.Close()

	sq := fmt.Sprintf(`INSERT INTO tasks.schedules (cluster_id,role,action,data,frequency,enabled,node_type,cluster_service) SELECT '%s','%s','%s','%s','%s'::interval, %t, '%s', '%s' WHERE NOT EXISTS (SELECT id FROM tasks.schedules WHERE action = '%s' AND node_type = '%s' AND data = '%s') `, s.ClusterID, s.Role, s.Action, s.Data, s.Frequency, s.Enabled, s.NodeType, s.ClusterService, s.Action, s.NodeType, s.Data)

	_, err = scheduleDB.Exec(sq)
	if err != nil {
		fmt.Printf(`tasks.Schedule.Add():  %s`, err)
	}
	return
}

var _ = Describe("RDPG Database Migrations...", func() {

	It("Check cluster_service column in cfsb.plans table exists, otherwise create", func() {

		allNodes := GetAllNodes()

		tableSchema := `tasks`
		tableName := `schedules`

		//If something wiped the tasks.tasks table, rebuild the rows that should be in the table
		for i := 0; i < len(allNodes); i++ {
			address := allNodes[i].Address
			sq := fmt.Sprintf(` SELECT count(*) as rowCount FROM %s.%s `, tableSchema, tableName)
			rowCount, err := GetRowCount(address, sq)
			clusterService := GetClusterServiceType(allNodes[i].ServiceName)
			ClusterID := allNodes[i].ServiceName

			if rowCount == 0 {
				schedules := []Schedule{}
				if clusterService == "pgbdr" {
					schedules = append(schedules, Schedule{ClusterID: ClusterID, ClusterService: clusterService, Role: `all`, Action: `Vacuum`, Data: `tasks.tasks`, NodeType: `read`, Frequency: `5 minutes`, Enabled: true})
					schedules = append(schedules, Schedule{ClusterID: ClusterID, ClusterService: clusterService, Role: `all`, Action: `Vacuum`, Data: `tasks.tasks`, NodeType: `write`, Frequency: `5 minutes`, Enabled: true})
					schedules = append(schedules, Schedule{ClusterID: ClusterID, ClusterService: clusterService, Role: `all`, Action: `DeleteBackupHistory`, Data: ``, NodeType: `read`, Frequency: `1 hour`, Enabled: true})
					schedules = append(schedules, Schedule{ClusterID: ClusterID, ClusterService: clusterService, Role: `all`, Action: `BackupDatabase`, Data: `rdpg`, NodeType: `read`, Frequency: `1 hour`, Enabled: true})
					schedules = append(schedules, Schedule{ClusterID: ClusterID, ClusterService: clusterService, Role: `all`, Action: `EnforceFileRetention`, Data: ``, NodeType: `read`, Frequency: `1 hour`, Enabled: true})
					schedules = append(schedules, Schedule{ClusterID: ClusterID, ClusterService: clusterService, Role: `all`, Action: `EnforceFileRetention`, Data: ``, NodeType: `write`, Frequency: `1 hour`, Enabled: true})
					schedules = append(schedules, Schedule{ClusterID: ClusterID, ClusterService: clusterService, Role: `all`, Action: `ClearStuckTasks`, Data: ``, NodeType: `read`, Frequency: `1 hour`, Enabled: true})
					schedules = append(schedules, Schedule{ClusterID: ClusterID, ClusterService: clusterService, Role: `all`, Action: `ClearStuckTasks`, Data: ``, NodeType: `write`, Frequency: `1 hour`, Enabled: true})

					if os.Getenv(`RDPGD_S3_BACKUPS`) == "ENABLED" {
						schedules = append(schedules, Schedule{ClusterID: ClusterID, ClusterService: clusterService, Role: `all`, Action: `FindFilesToCopyToS3`, Data: ``, NodeType: `read`, Frequency: `5 minutes`, Enabled: true})
						schedules = append(schedules, Schedule{ClusterID: ClusterID, ClusterService: clusterService, Role: `all`, Action: `FindFilesToCopyToS3`, Data: ``, NodeType: `write`, Frequency: `5 minutes`, Enabled: true})
						schedules = append(schedules, Schedule{ClusterID: ClusterID, ClusterService: clusterService, Role: `all`, Action: `EnforceRemoteFileRetention`, Data: ``, NodeType: `read`, Frequency: `1 hour`, Enabled: true})
						schedules = append(schedules, Schedule{ClusterID: ClusterID, ClusterService: clusterService, Role: `all`, Action: `EnforceRemoteFileRetention`, Data: ``, NodeType: `write`, Frequency: `1 hour`, Enabled: true})
					} else {
						schedules = append(schedules, Schedule{ClusterID: ClusterID, ClusterService: clusterService, Role: `all`, Action: `FindFilesToCopyToS3`, Data: ``, NodeType: `read`, Frequency: `5 minutes`, Enabled: false})
						schedules = append(schedules, Schedule{ClusterID: ClusterID, ClusterService: clusterService, Role: `all`, Action: `FindFilesToCopyToS3`, Data: ``, NodeType: `write`, Frequency: `5 minutes`, Enabled: false})
						schedules = append(schedules, Schedule{ClusterID: ClusterID, ClusterService: clusterService, Role: `all`, Action: `EnforceRemoteFileRetention`, Data: ``, NodeType: `read`, Frequency: `1 hour`, Enabled: false})
						schedules = append(schedules, Schedule{ClusterID: ClusterID, ClusterService: clusterService, Role: `all`, Action: `EnforceRemoteFileRetention`, Data: ``, NodeType: `write`, Frequency: `1 hour`, Enabled: false})

					}

					if allNodes[i].ServiceName == "rdpgmc" {
						schedules = append(schedules, Schedule{ClusterID: ClusterID, ClusterService: clusterService, Role: `manager`, Action: `ReconcileAvailableDatabases`, Data: ``, NodeType: `read`, Frequency: `1 minute`, Enabled: true})
						schedules = append(schedules, Schedule{ClusterID: ClusterID, ClusterService: clusterService, Role: `manager`, Action: `ReconcileAllDatabases`, Data: ``, NodeType: `read`, Frequency: `5 minutes`, Enabled: true})
					} else {
						schedules = append(schedules, Schedule{ClusterID: ClusterID, ClusterService: clusterService, Role: `service`, Action: `ScheduleNewDatabaseBackups`, Data: ``, NodeType: `write`, Frequency: `1 minute`, Enabled: true})
						schedules = append(schedules, Schedule{ClusterID: ClusterID, ClusterService: clusterService, Role: `service`, Action: `PrecreateDatabases`, Data: ``, NodeType: `write`, Frequency: `1 minute`, Enabled: true})
						schedules = append(schedules, Schedule{ClusterID: ClusterID, ClusterService: clusterService, Role: `service`, Action: `DecommissionDatabases`, Data: ``, NodeType: `write`, Frequency: `15 minutes`, Enabled: true})
						schedules = append(schedules, Schedule{ClusterID: ClusterID, ClusterService: clusterService, Role: `service`, Action: `Reconfigure`, Data: `pgbouncer`, NodeType: `read`, Frequency: `1 hour`, Enabled: true})
						schedules = append(schedules, Schedule{ClusterID: ClusterID, ClusterService: clusterService, Role: `service`, Action: `Reconfigure`, Data: `pgbouncer`, NodeType: `write`, Frequency: `1 hour`, Enabled: true})

					}

				} else { // Currently else is specifically postgresql only... we'll have to move this to a switch statement later :)
					schedules = append(schedules, Schedule{ClusterID: ClusterID, ClusterService: clusterService, Role: `all`, Action: `Vacuum`, Data: `tasks.tasks`, NodeType: `write`, Frequency: `5 minutes`, Enabled: true})
					schedules = append(schedules, Schedule{ClusterID: ClusterID, ClusterService: clusterService, Role: `all`, Action: `DeleteBackupHistory`, Data: ``, NodeType: `write`, Frequency: `1 hour`, Enabled: true})
					schedules = append(schedules, Schedule{ClusterID: ClusterID, ClusterService: clusterService, Role: `all`, Action: `BackupDatabase`, Data: `rdpg`, NodeType: `write`, Frequency: `1 hour`, Enabled: true})
					schedules = append(schedules, Schedule{ClusterID: ClusterID, ClusterService: clusterService, Role: `all`, Action: `EnforceFileRetention`, Data: ``, NodeType: `write`, Frequency: `1 hour`, Enabled: true})
					schedules = append(schedules, Schedule{ClusterID: ClusterID, ClusterService: clusterService, Role: `all`, Action: `BackupAllDatabases`, Data: ``, NodeType: `write`, Frequency: `1 hour`, Enabled: true})
					schedules = append(schedules, Schedule{ClusterID: ClusterID, ClusterService: clusterService, Role: `all`, Action: `ClearStuckTasks`, Data: ``, NodeType: `write`, Frequency: `1 hour`, Enabled: true})

					if os.Getenv(`RDPGD_S3_BACKUPS`) == "ENABLED" {
						schedules = append(schedules, Schedule{ClusterID: ClusterID, ClusterService: clusterService, Role: `all`, Action: `FindFilesToCopyToS3`, Data: ``, NodeType: `write`, Frequency: `5 minutes`, Enabled: true})
						schedules = append(schedules, Schedule{ClusterID: ClusterID, ClusterService: clusterService, Role: `all`, Action: `EnforceRemoteFileRetention`, Data: ``, NodeType: `write`, Frequency: `1 hour`, Enabled: true})
					} else {
						schedules = append(schedules, Schedule{ClusterID: ClusterID, ClusterService: clusterService, Role: `all`, Action: `FindFilesToCopyToS3`, Data: ``, NodeType: `write`, Frequency: `5 minutes`, Enabled: false})
						schedules = append(schedules, Schedule{ClusterID: ClusterID, ClusterService: clusterService, Role: `all`, Action: `EnforceRemoteFileRetention`, Data: ``, NodeType: `write`, Frequency: `1 hour`, Enabled: false})
					}

					if allNodes[i].ServiceName == "rdpgmc" {
						schedules = append(schedules, Schedule{ClusterID: ClusterID, ClusterService: clusterService, Role: `manager`, Action: `ReconcileAvailableDatabases`, Data: ``, NodeType: `write`, Frequency: `1 minute`, Enabled: true})
						schedules = append(schedules, Schedule{ClusterID: ClusterID, ClusterService: clusterService, Role: `manager`, Action: `ReconcileAllDatabases`, Data: ``, NodeType: `write`, Frequency: `5 minutes`, Enabled: true})
					} else {
						schedules = append(schedules, Schedule{ClusterID: ClusterID, ClusterService: clusterService, Role: `service`, Action: `ScheduleNewDatabaseBackups`, Data: ``, NodeType: `write`, Frequency: `1 minute`, Enabled: true})
						schedules = append(schedules, Schedule{ClusterID: ClusterID, ClusterService: clusterService, Role: `service`, Action: `PrecreateDatabases`, Data: ``, NodeType: `write`, Frequency: `1 minute`, Enabled: true})
						schedules = append(schedules, Schedule{ClusterID: ClusterID, ClusterService: clusterService, Role: `service`, Action: `DecommissionDatabases`, Data: ``, NodeType: `write`, Frequency: `15 minutes`, Enabled: true})
						schedules = append(schedules, Schedule{ClusterID: ClusterID, ClusterService: clusterService, Role: `service`, Action: `Reconfigure`, Data: `pgbouncer`, NodeType: `write`, Frequency: `1 hour`, Enabled: true})

					}
				}

				for index := range schedules {
					err = schedules[index].Add(address)
					if err != nil {
						Expect(err).NotTo(HaveOccurred())
						continue
					}
				}
				fmt.Printf("%s: Populated table %s.%s...\n", allNodes[i].Node, tableSchema, tableName)

			}

		}

	})

	/*	It("Check backups.file_history table exists, otherwise create", func() {

				allNodes := GetAllNodes()

				//Check all nodes
				var nodeRowCount []int
				for i := 0; i < len(allNodes); i++ {
					address := allNodes[i].Address
					sq := ` SELECT count(table_name) as rowCount FROM information_schema.tables WHERE table_schema = 'backups' and table_name IN ('file_history'); `
					tableCount, err := GetRowCount(address, sq)

					if tableCount == 0 {
						//Table doesn't exist, create it
						sq = `CREATE TABLE IF NOT EXISTS backups.file_history (
						  id               BIGSERIAL PRIMARY KEY NOT NULL,
							cluster_id        TEXT NOT NULL,
						  dbname            TEXT NOT NULL,
							node							TEXT NOT NULL,
							file_name					TEXT NOT NULL,
							action						TEXT NOT NULL,
							status						TEXT NOT NULL,
							params            json DEFAULT '{}'::json,
							created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
							duration          INT,
							removed_at        TIMESTAMP
						);`
						err = execQuery(address, sq)
						fmt.Printf("%s: Had to create backups.file_history table...\n", allNodes[i].Node)
						Expect(err).NotTo(HaveOccurred())
					}

					//Now rerun and verify the table was created
					sq = ` SELECT count(table_name) as rowCount FROM information_schema.tables WHERE table_schema = 'backups' and table_name IN ('file_history'); `
					rowCount, err := GetRowCount(address, sq)
					nodeRowCount = append(nodeRowCount, rowCount)
					fmt.Printf("%s: Found %d tables in schema 'backups'...\n", allNodes[i].Node, rowCount)
					Expect(err).NotTo(HaveOccurred())
				}

				//Verify each database also sees the same number of records (bdr sanity check)
				for i := 1; i < len(nodeRowCount); i++ {
					Expect(nodeRowCount[0]).To(Equal(nodeRowCount[i]))
				}

				Expect(len(nodeRowCount)).NotTo(Equal(0))
				Expect(nodeRowCount[0]).To(Equal(1))
			})

		It("Check node_type column in tasks.tasks table exists, otherwise create", func() {

			allNodes := GetAllNodes()
			tableSchema := `tasks`
			tableName := `tasks`
			columnName := `node_type`
			defaultValue := `any`

			//Check all nodes
			var nodeRowCount []int
			for i := 0; i < len(allNodes); i++ {
				address := allNodes[i].Address
				sq := fmt.Sprintf(` SELECT count(table_name) as rowCount FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s' AND column_name = '%s' `, tableSchema, tableName, columnName)
				columnCount, err := GetRowCount(address, sq)

				if columnCount == 0 {
					//Table doesn't exist, create it

					sq := fmt.Sprintf(`ALTER TABLE %s.%s ADD COLUMN %s text;`, tableSchema, tableName, columnName)
					err = execQuery(address, sq)
					fmt.Printf("%s: Had to create '%s' column in %s.%s...\n", allNodes[i].Node, columnName, tableSchema, tableName)
					Expect(err).NotTo(HaveOccurred())

					sq = fmt.Sprintf(`ALTER TABLE %s.%s ALTER COLUMN %s SET DEFAULT '%s';`, tableSchema, tableName, columnName, defaultValue)
					err = execQuery(address, sq)
					fmt.Printf("%s: Had to create '%s' column in %s.%s, setting default value to '%s'...\n", allNodes[i].Node, columnName, tableSchema, tableName, defaultValue)
					Expect(err).NotTo(HaveOccurred())

				}
				//Now rerun and verify the column was created
				sq = fmt.Sprintf(` SELECT count(table_name) as rowCount FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s' AND column_name = '%s' `, tableSchema, tableName, columnName)
				rowCount, err := GetRowCount(address, sq)
				nodeRowCount = append(nodeRowCount, rowCount)
				Expect(err).NotTo(HaveOccurred())
				fmt.Printf("%s: Found %d '%s' columns in table '%s.%s'...\n", allNodes[i].Node, rowCount, columnName, tableSchema, tableName)
			}

			//Verify each database also sees the same number of records (bdr sanity check)
			for i := 1; i < len(nodeRowCount); i++ {
				Expect(nodeRowCount[0]).To(Equal(nodeRowCount[i]))
			}

			Expect(len(nodeRowCount)).NotTo(Equal(0))
			Expect(nodeRowCount[0]).To(Equal(1))
		})

		It("Check node_type column in tasks.schedules table exists, otherwise create", func() {

			allNodes := GetAllNodes()
			tableSchema := `tasks`
			tableName := `schedules`
			columnName := `node_type`
			defaultValue := `any`

			//Check all nodes
			var nodeRowCount []int
			for i := 0; i < len(allNodes); i++ {
				address := allNodes[i].Address
				sq := fmt.Sprintf(` SELECT count(table_name) as rowCount FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s' AND column_name = '%s' `, tableSchema, tableName, columnName)
				columnCount, err := GetRowCount(address, sq)

				if columnCount == 0 {
					//Table doesn't exist, create it

					sq := fmt.Sprintf(`ALTER TABLE %s.%s ADD COLUMN %s text;`, tableSchema, tableName, columnName)
					err = execQuery(address, sq)
					fmt.Printf("%s: Had to create '%s' column in %s.%s...\n", allNodes[i].Node, columnName, tableSchema, tableName)
					Expect(err).NotTo(HaveOccurred())

					sq = fmt.Sprintf(`ALTER TABLE %s.%s ALTER COLUMN %s SET DEFAULT '%s';`, tableSchema, tableName, columnName, defaultValue)
					err = execQuery(address, sq)
					fmt.Printf("%s: Had to create '%s' column in %s.%s, setting default value to '%s'...\n", allNodes[i].Node, columnName, tableSchema, tableName, defaultValue)
					Expect(err).NotTo(HaveOccurred())

				}
				//Now rerun and verify the column was created
				sq = fmt.Sprintf(` SELECT count(table_name) as rowCount FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s' AND column_name = '%s' `, tableSchema, tableName, columnName)
				rowCount, err := GetRowCount(address, sq)
				nodeRowCount = append(nodeRowCount, rowCount)
				Expect(err).NotTo(HaveOccurred())
				fmt.Printf("%s: Found %d '%s' columns in table '%s.%s'...\n", allNodes[i].Node, rowCount, columnName, tableSchema, tableName)
			}

			//Verify each database also sees the same number of records (bdr sanity check)
			for i := 1; i < len(nodeRowCount); i++ {
				Expect(nodeRowCount[0]).To(Equal(nodeRowCount[i]))
			}

			Expect(len(nodeRowCount)).NotTo(Equal(0))
			Expect(nodeRowCount[0]).To(Equal(1))
		})

		It("Check default for defaultDaysToKeepFileHistory added rdpg.config", func() {

			allNodes := GetAllNodes()
			configKey := `defaultDaysToKeepFileHistory`
			configValue := `180`

			var nodeRowCount []int
			for i := 0; i < len(allNodes); i++ {
				address := allNodes[i].Address

				sq := fmt.Sprintf(`SELECT count(key) as rowCount FROM rdpg.config WHERE key IN ('%s') ;  `, configKey)
				configCount, err := GetRowCount(address, sq)

				if configCount == 0 {
					//Table entry doesn't exist, create it
					sq = fmt.Sprintf(`INSERT INTO rdpg.config (key,cluster_id,value) VALUES ('%s', '%s', '%s')`, configKey, allNodes[i].ServiceName, configValue)
					err = execQuery(address, sq)
					fmt.Printf("%s: Had to insert key %s with value %s into 'rdpg.config'...\n", allNodes[i].Node, configKey, configValue)
					Expect(err).NotTo(HaveOccurred())
					time.Sleep(1 * time.Second)
				}

				sq = fmt.Sprintf(`SELECT count(key) as rowCount FROM rdpg.config WHERE key IN ('%s') ;  `, configKey)
				rowCount, err := GetRowCount(address, sq)
				nodeRowCount = append(nodeRowCount, rowCount)
				Expect(err).NotTo(HaveOccurred())
				fmt.Printf("%s: Found %d default values for key %s in rdpg.config...\n", allNodes[i].Node, rowCount, configKey)
			}
			//Verify each database also sees the same number of records (bdr sanity check)
			for i := 1; i < len(nodeRowCount); i++ {
				Expect(nodeRowCount[0]).To(Equal(nodeRowCount[i]))
			}
			Expect(len(nodeRowCount)).NotTo(Equal(0))
			Expect(nodeRowCount[0]).To(Equal(1))

		})

		It("Check default for BackupPort added rdpg.config", func() {

			allNodes := GetAllNodes()
			configKey := `BackupPort`
			configValue := `7432`

			var nodeRowCount []int
			for i := 0; i < len(allNodes); i++ {
				address := allNodes[i].Address

				sq := fmt.Sprintf(`SELECT count(key) as rowCount FROM rdpg.config WHERE key IN ('%s') ;  `, configKey)
				configCount, err := GetRowCount(address, sq)

				if configCount == 0 {
					//Table entry doesn't exist, create it
					sq = fmt.Sprintf(`INSERT INTO rdpg.config (key,cluster_id,value) VALUES ('%s', '%s', '%s')`, configKey, allNodes[i].ServiceName, configValue)
					err = execQuery(address, sq)
					fmt.Printf("%s: Had to insert key %s with value %s into 'rdpg.config'...\n", allNodes[i].Node, configKey, configValue)
					Expect(err).NotTo(HaveOccurred())
					time.Sleep(1 * time.Second)
				}

				sq = fmt.Sprintf(`SELECT count(key) as rowCount FROM rdpg.config WHERE key IN ('%s') ;  `, configKey)
				rowCount, err := GetRowCount(address, sq)
				nodeRowCount = append(nodeRowCount, rowCount)
				Expect(err).NotTo(HaveOccurred())
				fmt.Printf("%s: Found %d default values for key %s in rdpg.config...\n", allNodes[i].Node, rowCount, configKey)
			}
			//Verify each database also sees the same number of records (bdr sanity check)
			for i := 1; i < len(nodeRowCount); i++ {
				Expect(nodeRowCount[0]).To(Equal(nodeRowCount[i]))
			}
			Expect(len(nodeRowCount)).NotTo(Equal(0))
			Expect(nodeRowCount[0]).To(Equal(1))

		})

		It("Check default for BackupsPath added rdpg.config", func() {

			allNodes := GetAllNodes()
			configKey := `BackupsPath`
			configValue := `/var/vcap/store/pgbdr/backups`

			var nodeRowCount []int
			for i := 0; i < len(allNodes); i++ {
				address := allNodes[i].Address

				sq := fmt.Sprintf(`SELECT count(key) as rowCount FROM rdpg.config WHERE key IN ('%s') ;  `, configKey)
				configCount, err := GetRowCount(address, sq)

				if configCount == 0 {
					//Table entry doesn't exist, create it
					sq = fmt.Sprintf(`INSERT INTO rdpg.config (key,cluster_id,value) VALUES ('%s', '%s', '%s')`, configKey, allNodes[i].ServiceName, configValue)
					err = execQuery(address, sq)
					fmt.Printf("%s: Had to insert key %s with value %s into 'rdpg.config'...\n", allNodes[i].Node, configKey, configValue)
					Expect(err).NotTo(HaveOccurred())
					time.Sleep(1 * time.Second)
				}

				sq = fmt.Sprintf(`SELECT count(key) as rowCount FROM rdpg.config WHERE key IN ('%s') ;  `, configKey)
				rowCount, err := GetRowCount(address, sq)
				nodeRowCount = append(nodeRowCount, rowCount)
				Expect(err).NotTo(HaveOccurred())
				fmt.Printf("%s: Found %d default values for key %s in rdpg.config...\n", allNodes[i].Node, rowCount, configKey)
			}
			//Verify each database also sees the same number of records (bdr sanity check)
			for i := 1; i < len(nodeRowCount); i++ {
				Expect(nodeRowCount[0]).To(Equal(nodeRowCount[i]))
			}
			Expect(len(nodeRowCount)).NotTo(Equal(0))
			Expect(nodeRowCount[0]).To(Equal(1))

		})

		It("Check default for pgDumpBinaryLocation added rdpg.config", func() {

			allNodes := GetAllNodes()
			configKey := `pgDumpBinaryLocation`
			configValue := `/var/vcap/packages/pgbdr/bin/pg_dump`

			var nodeRowCount []int
			for i := 0; i < len(allNodes); i++ {
				address := allNodes[i].Address

				sq := fmt.Sprintf(`SELECT count(key) as rowCount FROM rdpg.config WHERE key IN ('%s') ;  `, configKey)
				configCount, err := GetRowCount(address, sq)

				if configCount == 0 {
					//Table entry doesn't exist, create it
					sq = fmt.Sprintf(`INSERT INTO rdpg.config (key,cluster_id,value) VALUES ('%s', '%s', '%s')`, configKey, allNodes[i].ServiceName, configValue)
					err = execQuery(address, sq)
					fmt.Printf("%s: Had to insert key %s with value %s into 'rdpg.config'...\n", allNodes[i].Node, configKey, configValue)
					Expect(err).NotTo(HaveOccurred())
					time.Sleep(1 * time.Second)
				}

				sq = fmt.Sprintf(`SELECT count(key) as rowCount FROM rdpg.config WHERE key IN ('%s') ;  `, configKey)
				rowCount, err := GetRowCount(address, sq)
				nodeRowCount = append(nodeRowCount, rowCount)
				Expect(err).NotTo(HaveOccurred())
				fmt.Printf("%s: Found %d default values for key %s in rdpg.config...\n", allNodes[i].Node, rowCount, configKey)
			}
			//Verify each database also sees the same number of records (bdr sanity check)
			for i := 1; i < len(nodeRowCount); i++ {
				Expect(nodeRowCount[0]).To(Equal(nodeRowCount[i]))
			}
			Expect(len(nodeRowCount)).NotTo(Equal(0))
			Expect(nodeRowCount[0]).To(Equal(1))

		})

		It("Check default for pgDumpBinaryLocation added rdpg.config", func() {

			allNodes := GetAllNodes()
			configKey := `pgDumpBinaryLocation`
			configValue := `/var/vcap/packages/pgbdr/bin/pg_dump`

			var nodeRowCount []int
			for i := 0; i < len(allNodes); i++ {
				address := allNodes[i].Address

				sq := fmt.Sprintf(`SELECT count(key) as rowCount FROM rdpg.config WHERE key IN ('%s') ;  `, configKey)
				configCount, err := GetRowCount(address, sq)

				if configCount == 0 {
					//Table entry doesn't exist, create it
					sq = fmt.Sprintf(`INSERT INTO rdpg.config (key,cluster_id,value) VALUES ('%s', '%s', '%s')`, configKey, allNodes[i].ServiceName, configValue)
					err = execQuery(address, sq)
					fmt.Printf("%s: Had to insert key %s with value %s into 'rdpg.config'...\n", allNodes[i].Node, configKey, configValue)
					Expect(err).NotTo(HaveOccurred())
					time.Sleep(1 * time.Second)
				}

				sq = fmt.Sprintf(`SELECT count(key) as rowCount FROM rdpg.config WHERE key IN ('%s') ;  `, configKey)
				rowCount, err := GetRowCount(address, sq)
				nodeRowCount = append(nodeRowCount, rowCount)
				Expect(err).NotTo(HaveOccurred())
				fmt.Printf("%s: Found %d default values for key %s in rdpg.config...\n", allNodes[i].Node, rowCount, configKey)
			}
			//Verify each database also sees the same number of records (bdr sanity check)
			for i := 1; i < len(nodeRowCount); i++ {
				Expect(nodeRowCount[0]).To(Equal(nodeRowCount[i]))
			}
			Expect(len(nodeRowCount)).NotTo(Equal(0))
			Expect(nodeRowCount[0]).To(Equal(1))

		})

		It("Check DeleteBackupHistory job exists in tasks.schedules", func() {

			allNodes := GetAllNodes()

			var nodeRowCount []int
			for i := 0; i < len(allNodes); i++ {
				address := allNodes[i].Address

				sq := `SELECT count(action) as rowCount FROM tasks.schedules WHERE action = 'DeleteBackupHistory'; `
				taskCount, err := GetRowCount(address, sq)

				fmt.Printf("%s: Found %d taskCount'...\n", allNodes[i].Node, taskCount)

				if taskCount == 0 {
					//Table entry doesn't exist, create it
					sq = fmt.Sprintf(`INSERT INTO tasks.schedules (cluster_id,role,action,data,frequency,enabled,node_type) VALUES ('%s','all','DeleteBackupHistory','','1 hour'::interval, true, 'read')`, allNodes[i].ServiceName)

					err = execQuery(address, sq)
					fmt.Printf("%s: Had to add DeleteBackupHistory into 'task.schedules'...\n", allNodes[i].Node)

					Expect(err).NotTo(HaveOccurred())
					time.Sleep(1 * time.Second)
				}

				sq = `SELECT count(action) as rowCount FROM tasks.schedules WHERE action = 'DeleteBackupHistory'; `
				rowCount, err := GetRowCount(address, sq)
				nodeRowCount = append(nodeRowCount, rowCount)
				Expect(err).NotTo(HaveOccurred())
				fmt.Printf("%s: Found %d scheduled tasks for DeleteBackupHistory in tasks.schedules...\n", allNodes[i].Node, rowCount)
			}
			//Verify each database also sees the same number of records (bdr sanity check)
			for i := 1; i < len(nodeRowCount); i++ {
				Expect(nodeRowCount[0]).To(Equal(nodeRowCount[i]))
			}
			Expect(len(nodeRowCount)).NotTo(Equal(0))
			Expect(nodeRowCount[0]).To(Equal(1))

		})

		It("Check ScheduleNewDatabaseBackups job exists in tasks.schedules", func() {

			allNodes := GetServiceNodes()

			var nodeRowCount []int
			for i := 0; i < len(allNodes); i++ {
				address := allNodes[i].Address

				sq := `SELECT count(action) as rowCount FROM tasks.schedules WHERE action = 'ScheduleNewDatabaseBackups'; `
				taskCount, err := GetRowCount(address, sq)

				fmt.Printf("%s: Found %d taskCount'...\n", allNodes[i].Node, taskCount)

				if taskCount == 0 {
					//Table entry doesn't exist, create it
					sq = fmt.Sprintf(`INSERT INTO tasks.schedules (cluster_id,role,action,data,frequency,enabled,node_type) VALUES ('%s','service','ScheduleNewDatabaseBackups','','1 minute'::interval, true, 'write')`, allNodes[i].ServiceName)

					err = execQuery(address, sq)
					fmt.Printf("%s: Had to add ScheduleNewDatabaseBackups into 'task.schedules'...\n", allNodes[i].Node)
					Expect(err).NotTo(HaveOccurred())
					time.Sleep(1 * time.Second)
				}

				sq = `SELECT count(action) as rowCount FROM tasks.schedules WHERE action = 'ScheduleNewDatabaseBackups'; `
				rowCount, err := GetRowCount(address, sq)
				nodeRowCount = append(nodeRowCount, rowCount)
				Expect(err).NotTo(HaveOccurred())
				fmt.Printf("%s: Found %d scheduled tasks for ScheduleNewDatabaseBackups in tasks.schedules...\n", allNodes[i].Node, rowCount)
			}
			//Verify each database also sees the same number of records (bdr sanity check)
			for i := 1; i < len(nodeRowCount); i++ {
				Expect(nodeRowCount[0]).To(Equal(nodeRowCount[i]))
			}
			Expect(len(nodeRowCount)).NotTo(Equal(0))
			Expect(nodeRowCount[0]).To(Equal(1))

		})

		It("Check no null values in node_type for tasks.schedules", func() {

			allNodes := GetAllNodes()

			var nodeRowCount []int
			for i := 0; i < len(allNodes); i++ {
				address := allNodes[i].Address

				sq := `SELECT count(node_type) as rowCount FROM tasks.schedules WHERE node_type IS NULL; `
				taskCount, err := GetRowCount(address, sq)

				if taskCount == 0 {
					//Table entry doesn't exist, create it
					sq = `UPDATE tasks.schedules SET node_type='write' WHERE node_type IS NULL;`

					err = execQuery(address, sq)
					fmt.Printf("%s: Had to update %d task with a default in 'task.schedules'...\n", allNodes[i].Node, taskCount)

					Expect(err).NotTo(HaveOccurred())
				}

				sq = `SELECT count(node_type) as rowCount FROM tasks.schedules WHERE node_type IS NULL; `
				rowCount, err := GetRowCount(address, sq)
				nodeRowCount = append(nodeRowCount, rowCount)
				Expect(err).NotTo(HaveOccurred())
				fmt.Printf("%s: Found %d rows with null values in node_type column of tasks.schedules...\n", allNodes[i].Node, rowCount)
			}
			//Verify each database also sees the same number of records (bdr sanity check)
			for i := 1; i < len(nodeRowCount); i++ {
				Expect(nodeRowCount[0]).To(Equal(nodeRowCount[i]))
			}
			Expect(len(nodeRowCount)).NotTo(Equal(0))
			Expect(nodeRowCount[0]).To(Equal(0))

		})

		It("Check BackupDatabase job exists in tasks.schedules for the rdpg system database", func() {

			allNodes := GetAllNodes()

			var nodeRowCount []int
			for i := 0; i < len(allNodes); i++ {
				address := allNodes[i].Address

				sq := `SELECT count(action) as rowCount FROM tasks.schedules WHERE action = 'BackupDatabase' AND data = 'rdpg'; `
				taskCount, err := GetRowCount(address, sq)

				fmt.Printf("%s: Found %d taskCount'...\n", allNodes[i].Node, taskCount)

				if taskCount == 0 {
					//Table entry doesn't exist, create it
					sq = fmt.Sprintf(`INSERT INTO tasks.schedules (cluster_id,role,action,data,frequency,enabled,node_type) VALUES ('%s','all','BackupDatabase','rdpg','1 day'::interval, true, 'read')`, allNodes[i].ServiceName)

					err = execQuery(address, sq)
					fmt.Printf("%s: Had to add BackupDatabase for 'rdpg' into 'task.schedules'...\n", allNodes[i].Node)
					Expect(err).NotTo(HaveOccurred())
					time.Sleep(1 * time.Second)
				}

				sq = `SELECT count(action) as rowCount FROM tasks.schedules WHERE action = 'BackupDatabase' AND data = 'rdpg'; `
				rowCount, err := GetRowCount(address, sq)
				nodeRowCount = append(nodeRowCount, rowCount)
				Expect(err).NotTo(HaveOccurred())
				fmt.Printf("%s: Found %d scheduled tasks for BackupDatabase for database 'rdpg' in tasks.schedules...\n", allNodes[i].Node, rowCount)
			}
			//Verify each database also sees the same number of records (bdr sanity check)
			for i := 1; i < len(nodeRowCount); i++ {
				Expect(nodeRowCount[0]).To(Equal(nodeRowCount[i]))
			}
			Expect(len(nodeRowCount)).NotTo(Equal(0))
			Expect(nodeRowCount[0]).To(Equal(1))

		})

		It("Check Vacuum job in tasks.schedules no longer is scheduled for 'any'", func() {

			allNodes := GetAllNodes()

			var nodeRowCount []int
			for i := 0; i < len(allNodes); i++ {
				address := allNodes[i].Address

				sq := `SELECT count(action) as rowCount FROM tasks.schedules WHERE action = 'Vacuum' AND node_type = 'any'; `
				taskCount, err := GetRowCount(address, sq)

				fmt.Printf("%s: Found %d taskCount'...\n", allNodes[i].Node, taskCount)

				if taskCount == 1 {
					//Table entry doesn't exist, create it
					sq = fmt.Sprintf(`UPDATE tasks.schedules SET node_type = 'read' WHERE action = 'Vacuum' AND node_type = 'any';`)

					err = execQuery(address, sq)
					fmt.Printf("%s: Had to add change Vacuum node_type to 'read' from 'any' in 'task.schedules'...\n", allNodes[i].Node)
					Expect(err).NotTo(HaveOccurred())
					time.Sleep(1 * time.Second)
				}

				sq = `SELECT count(action) as rowCount FROM tasks.schedules WHERE action = 'Vacuum' AND node_type = 'read'; `
				rowCount, err := GetRowCount(address, sq)
				nodeRowCount = append(nodeRowCount, rowCount)
				Expect(err).NotTo(HaveOccurred())
				fmt.Printf("%s: Found %d scheduled tasks for Vacuum with node_type 'read' in tasks.schedules...\n", allNodes[i].Node, rowCount)
			}
			//Verify each database also sees the same number of records (bdr sanity check)
			for i := 1; i < len(nodeRowCount); i++ {
				Expect(nodeRowCount[0]).To(Equal(nodeRowCount[i]))
			}
			Expect(len(nodeRowCount)).NotTo(Equal(0))
			Expect(nodeRowCount[0]).To(Equal(1))

		})

		It("Check Vacuum job exists in tasks.schedules is scheduled for node_type 'write'", func() {

			allNodes := GetAllNodes()

			var nodeRowCount []int
			for i := 0; i < len(allNodes); i++ {
				address := allNodes[i].Address

				sq := `SELECT count(action) as rowCount FROM tasks.schedules WHERE action = 'Vacuum' AND node_type = 'write'; `
				taskCount, err := GetRowCount(address, sq)

				fmt.Printf("%s: Found %d taskCount'...\n", allNodes[i].Node, taskCount)

				if taskCount == 0 {
					//Table entry doesn't exist, create it
					sq = fmt.Sprintf(`INSERT INTO tasks.schedules (cluster_id,role,action,data,frequency,enabled,node_type) VALUES ('%s','all','Vacuum','tasks.tasks','5 minutes'::interval, true, 'write')`, allNodes[i].ServiceName)

					err = execQuery(address, sq)
					fmt.Printf("%s: Had to add Vacuum job for the node_type 'write' into 'task.schedules'...\n", allNodes[i].Node)
					Expect(err).NotTo(HaveOccurred())
					time.Sleep(1 * time.Second)
				}

				sq = `SELECT count(action) as rowCount FROM tasks.schedules WHERE action = 'Vacuum' AND node_type = 'write'; `
				rowCount, err := GetRowCount(address, sq)
				nodeRowCount = append(nodeRowCount, rowCount)
				Expect(err).NotTo(HaveOccurred())
				fmt.Printf("%s: Found %d scheduled tasks for Vacuum with node_type 'write' in tasks.schedules...\n", allNodes[i].Node, rowCount)
			}
			//Verify each database also sees the same number of records (bdr sanity check)
			for i := 1; i < len(nodeRowCount); i++ {
				Expect(nodeRowCount[0]).To(Equal(nodeRowCount[i]))
			}
			Expect(len(nodeRowCount)).NotTo(Equal(0))
			Expect(nodeRowCount[0]).To(Equal(1))

		})

		It("Check FindFilesToCopyToS3 job exists in tasks.schedules is scheduled for node_type 'write'", func() {

			allNodes := GetAllNodes()

			var nodeRowCount []int
			for i := 0; i < len(allNodes); i++ {
				address := allNodes[i].Address

				sq := `SELECT count(action) as rowCount FROM tasks.schedules WHERE action = 'FindFilesToCopyToS3' AND node_type = 'write'; `
				taskCount, err := GetRowCount(address, sq)

				fmt.Printf("%s: Found %d taskCount'...\n", allNodes[i].Node, taskCount)

				if taskCount == 0 {
					//Table entry doesn't exist, create it
					sq = fmt.Sprintf(`INSERT INTO tasks.schedules (cluster_id,role,action,data,frequency,enabled,node_type) VALUES ('%s','all','FindFilesToCopyToS3','tasks.tasks','5 minutes'::interval, false, 'write')`, allNodes[i].ServiceName)

					err = execQuery(address, sq)
					fmt.Printf("%s: Had to add FindFilesToCopyToS3 job for the node_type 'write' into 'task.schedules'...\n", allNodes[i].Node)
					Expect(err).NotTo(HaveOccurred())
					time.Sleep(1 * time.Second)
				}

				sq = `SELECT count(action) as rowCount FROM tasks.schedules WHERE action = 'FindFilesToCopyToS3' AND node_type = 'write'; `
				rowCount, err := GetRowCount(address, sq)
				nodeRowCount = append(nodeRowCount, rowCount)
				Expect(err).NotTo(HaveOccurred())
				fmt.Printf("%s: Found %d scheduled tasks for FindFilesToCopyToS3 with node_type 'write' in tasks.schedules...\n", allNodes[i].Node, rowCount)
			}
			//Verify each database also sees the same number of records (bdr sanity check)
			for i := 1; i < len(nodeRowCount); i++ {
				Expect(nodeRowCount[0]).To(Equal(nodeRowCount[i]))
			}
			Expect(len(nodeRowCount)).NotTo(Equal(0))
			Expect(nodeRowCount[0]).To(Equal(1))

		})

		It("Check FindFilesToCopyToS3 job exists in tasks.schedules is scheduled for node_type 'read'", func() {

			allNodes := GetAllNodes()

			var nodeRowCount []int
			for i := 0; i < len(allNodes); i++ {
				address := allNodes[i].Address

				sq := `SELECT count(action) as rowCount FROM tasks.schedules WHERE action = 'FindFilesToCopyToS3' AND node_type = 'read'; `
				taskCount, err := GetRowCount(address, sq)

				fmt.Printf("%s: Found %d taskCount'...\n", allNodes[i].Node, taskCount)

				if taskCount == 0 {
					//Table entry doesn't exist, create it
					sq = fmt.Sprintf(`INSERT INTO tasks.schedules (cluster_id,role,action,data,frequency,enabled,node_type) VALUES ('%s','all','FindFilesToCopyToS3','tasks.tasks','5 minutes'::interval, false, 'read')`, allNodes[i].ServiceName)

					err = execQuery(address, sq)
					fmt.Printf("%s: Had to add FindFilesToCopyToS3 job for the node_type 'read' into 'task.schedules'...\n", allNodes[i].Node)
					Expect(err).NotTo(HaveOccurred())
					time.Sleep(1 * time.Second)
				}

				sq = `SELECT count(action) as rowCount FROM tasks.schedules WHERE action = 'FindFilesToCopyToS3' AND node_type = 'read'; `
				rowCount, err := GetRowCount(address, sq)
				nodeRowCount = append(nodeRowCount, rowCount)
				Expect(err).NotTo(HaveOccurred())
				fmt.Printf("%s: Found %d scheduled tasks for FindFilesToCopyToS3 with node_type 'read' in tasks.schedules...\n", allNodes[i].Node, rowCount)
			}
			//Verify each database also sees the same number of records (bdr sanity check)
			for i := 1; i < len(nodeRowCount); i++ {
				Expect(nodeRowCount[0]).To(Equal(nodeRowCount[i]))
			}
			Expect(len(nodeRowCount)).NotTo(Equal(0))
			Expect(nodeRowCount[0]).To(Equal(1))

		})
	*/
})
