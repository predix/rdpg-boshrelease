package backups_test

import (
	"fmt"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/starkandwayne/rdpg-acceptance-tests/rdpg-service/helper-functions"
)

var _ = Describe("RDPG Backups Testing...", func() {

	It("Check backups Tables Exist", func() {

		allNodes := GetAllNodes()

		for i := 0; i < len(allNodes); i++ {
			address := allNodes[i].Address
			sq := ` SELECT count(table_name) as rowCount FROM information_schema.tables WHERE table_schema = 'backups' and table_name IN ('file_history'); `
			rowCount, err := GetRowCount(address, sq)
			fmt.Printf("%s: Found %d tables in schema 'backups'...\n", allNodes[i].Node, rowCount)
			Expect(err).NotTo(HaveOccurred())
			Expect(rowCount).To(Equal(1))
		}

	})

	It("Check all user databases are scheduled for backups", func() {

		//Note: this one may need a sleep command in order for it to report correctly on a freshly created deployment
		allNodes := GetServiceNodes()

		for i := 0; i < len(allNodes); i++ {
			address := allNodes[i].Address
			sq := `SELECT count(name) as rowCount FROM ( (SELECT dbname AS name FROM cfsb.instances WHERE effective_at IS NOT NULL AND decommissioned_at IS NULL) EXCEPT (SELECT data AS name FROM tasks.schedules WHERE action = 'BackupDatabase' ) ) AS missing_databases;  `
			rowCount, err := GetRowCount(address, sq)
			fmt.Printf("%s: Found %d databases in cfsb.instances not scheduled for backups in tasks.schedules...\n", allNodes[i].Node, rowCount)
			Expect(err).NotTo(HaveOccurred())
			Expect(rowCount).To(Equal(0))
		}

	})

	It("Check all configuration defaults have been configured", func() {

		allNodes := GetAllNodes()

		for i := 0; i < len(allNodes); i++ {
			address := allNodes[i].Address
			sq := `SELECT count(key) as rowCount FROM rdpg.config WHERE key IN ('pgDumpBinaryLocation','BackupPort','BackupsPath','defaultDaysToKeepFileHistory') ;  `
			rowCount, err := GetRowCount(address, sq)
			fmt.Printf("%s: Found %d default values configured in rdpg.config...\n", allNodes[i].Node, rowCount)
			Expect(err).NotTo(HaveOccurred())
			Expect(rowCount).To(Equal(4))
		}
	})

	It("Check task DeleteBackupHistory is scheduled", func() {

		allNodes := GetServiceNodes()

		for i := 0; i < len(allNodes); i++ {
			address := allNodes[i].Address
			sq := `SELECT count(action) as rowCount FROM tasks.schedules WHERE action = 'DeleteBackupHistory' AND enabled=true;  `
			rowCount, err := GetRowCount(address, sq)
			fmt.Printf("%s: Found %d entry for 'DeleteBackupHistory' in tasks.schedules...\n", allNodes[i].Node, rowCount)
			Expect(err).NotTo(HaveOccurred())
			Expect(rowCount).To(Equal(1))
		}
	})

	It("Check task ScheduleNewDatabaseBackups is scheduled", func() {

		allNodes := GetServiceNodes()

		for i := 0; i < len(allNodes); i++ {
			address := allNodes[i].Address
			sq := `SELECT count(action) as rowCount FROM tasks.schedules WHERE action = 'ScheduleNewDatabaseBackups' AND enabled=true;  `
			rowCount, err := GetRowCount(address, sq)
			fmt.Printf("%s: Found %d entry for 'ScheduleNewDatabaseBackups' in tasks.schedules...\n", allNodes[i].Node, rowCount)
			Expect(err).NotTo(HaveOccurred())
			Expect(rowCount).To(Equal(1))
		}
	})

	It("Check backups.file_history truncation is working", func() {

		//daysToKeep, err := GetConfigKeyValue(`defaultDaysToKeepFileHistory`)
		daysToKeep := `181` //Default is 180, use defaultDaysToKeepFileHistory + 1 day
		allNodes := GetAllNodes()

		for i := 0; i < len(allNodes); i++ {
			address := allNodes[i].Address
			sq := fmt.Sprintf(`SELECT count(*) as rowCount FROM backups.file_history WHERE created_at < NOW() - '%s days'::interval; `, daysToKeep)
			rowCount, err := GetRowCount(address, sq)
			fmt.Printf("%s: Found %d rows in backups.file_history which should have been removed via a scheduled task...\n", allNodes[i].Node, rowCount)
			Expect(err).NotTo(HaveOccurred())
			Expect(rowCount).To(Equal(0))
		}
	})

	It("Check task BackupDatabase for rdpg system database is scheduled", func() {

		allNodes := GetAllNodes()

		for i := 0; i < len(allNodes); i++ {
			address := allNodes[i].Address
			sq := `SELECT count(action) as rowCount FROM tasks.schedules WHERE action = 'BackupDatabase' AND data = 'rdpg' AND enabled=true;  `
			rowCount, err := GetRowCount(address, sq)
			fmt.Printf("%s: Found %d entry for 'BackupDatabase' for database 'rdpg' in tasks.schedules...\n", allNodes[i].Node, rowCount)
			Expect(err).NotTo(HaveOccurred())
			Expect(rowCount).To(Equal(1))
		}

	})

	It("Check task FindFilesToCopyToS3 is scheduled for both node types", func() {

		allNodes := GetAllNodes()

		for i := 0; i < len(allNodes); i++ {
			address := allNodes[i].Address
			clusterService := GetClusterServiceType(allNodes[i].ServiceName)
			sq := `SELECT count(action) as rowCount FROM tasks.schedules WHERE action = 'FindFilesToCopyToS3' AND node_type IN ('read', 'write', 'any');  `
			rowCount, err := GetRowCount(address, sq)
			Expect(err).NotTo(HaveOccurred())
			if clusterService == `pgbdr` {
				Expect(rowCount).To(Equal(2))
			} else {
				Expect(rowCount).To(Equal(1))
			}
			fmt.Printf("%s: Found %d entry for 'FindFilesToCopyToS3' for database 'rdpg' in tasks.schedules...\n", allNodes[i].Node, rowCount)
		}
	})

})
