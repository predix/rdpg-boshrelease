package admin

import (
	"fmt"
	"os"
	"strconv"

	"github.com/starkandwayne/rdpgd/log"
	"github.com/starkandwayne/rdpgd/pg"
)

//curl http://rdpg:admin@127.0.0.1:58888/stats/locks/rdpg -v
type databaseLocks struct {
	Mode      string `db:"mode" json:"mode"`
	ModeCount int    `db:"mode_count" json:"mode_count"`
	DBName    string `db:"dbname" json:"dbname"`
}

func execQuery(address string, sq string) (queryRowCount int, err error) {
	p := pg.NewPG(address, "7432", `rdpg`, `rdpg`, pgPass)
	db, err := p.Connect()
	if err != nil {
		return -1, err
	}
	defer db.Close()
	var rowCount []int
	err = db.Select(&rowCount, sq)
	if err != nil {
		return -1, err
	}
	return rowCount[0], nil
}

func getRowCount(sq string) (rowCount int, err error) {

	address := `127.0.0.1`
	rowCount, err = execQuery(address, sq)
	return
	//Expect(nodeRowCount[0]).To(Equal(0))

}

func getQueueDepth() (rowCount int) {
	sq := `SELECT COUNT(*) FROM tasks.tasks;`
	rowCount, err := getRowCount(sq)
	if err != nil {
		log.Error(fmt.Sprintf("admin.getQueueDepth() Could not get row count running query %s ! %s", sq, err))
		return -1
	}
	return
}

func getNumberOfBoundDatabases() (rowCount int) {
	sq := `SELECT COUNT(*) FROM cfsb.instances WHERE instance_id IS NOT NULL AND effective_at IS NOT NULL AND ineffective_at IS NULL AND decommissioned_at IS NULL;`
	rowCount, err := getRowCount(sq)
	if err != nil {
		log.Error(fmt.Sprintf("admin.getNumberOfBoundDatabases() Could not get row count running query %s ! %s", sq, err))
		return -1
	}
	return
}

func getNumberOfFreeDatabases() (rowCount int) {
	sq := `SELECT COUNT(*) FROM cfsb.instances WHERE instance_id IS NULL AND effective_at IS NOT NULL AND ineffective_at IS NULL AND decommissioned_at IS NULL;`
	rowCount, err := getRowCount(sq)
	if err != nil {
		log.Error(fmt.Sprintf("admin.getNumberOfFreeDatabases() Could not get row count running query %s ! %s", sq, err))
		return -1
	}
	return
}

func getNumberOfReplicationSlots() (rowCount int) {
	sq := `SELECT COUNT(*) FROM pg_replication_slots WHERE active=true;`
	rowCount, err := getRowCount(sq)
	if err != nil {
		log.Error(fmt.Sprintf("admin.getNumberOfReplicationSlots() Could not get row count running query %s ! %s", sq, err))
		return -1
	}
	return
}

func getNumberOfDatabaseBackupOnDisk() (rowCount int) {
	sq := `SELECT COUNT(*) FROM backups.file_history WHERE action = 'CreateBackup' AND removed_at IS NULL;`
	rowCount, err := getRowCount(sq)
	if err != nil {
		log.Error(fmt.Sprintf("admin.getNumberOfDatabaseBackupOnDisk() Could not get row count running query %s ! %s", sq, err))
		return -1
	}
	return
}

func getNumberOfUserDatabases() (rowCount int) {
	sq := `SELECT count(*) FROM pg_database WHERE datname LIKE 'd%';`
	rowCount, err := getRowCount(sq)
	if err != nil {
		log.Error(fmt.Sprintf("admin.getNumberOfDatabaseBackupOnDisk() Could not get row count running query %s ! %s", sq, err))
		return -1
	}
	return
}

func getMaxLimitNumberOfDatabases() (rowCount int) {
	rowCount, err := strconv.Atoi(os.Getenv(`RDPGD_INSTANCE_ALLOWED`))
	if err != nil {
		log.Error(fmt.Sprintf("admin.getMaxLimitNumberOfDatabases() Could not get convert environment variable RDPGD_INSTANCE_ALLOWED string value %s to an integer ! %s", os.Getenv(`RDPGD_INSTANCE_ALLOWED`), err))
		return -1
	}
	return
}

func getLockCountByDatabase(dbname string) (locks []databaseLocks, err error) {
	locks = []databaseLocks{}
	sql := `SELECT mode::text as mode, count(mode) as mode_count, datname::text as dbname FROM pg_locks, pg_database WHERE database=oid`
	if dbname != "" {
		sql += fmt.Sprintf(` AND datname = '%s'`, dbname)
	}
	sql += ` GROUP BY mode, datname;`

	pgPort := `7432`
	address := `127.0.0.1`
	pgPass = os.Getenv(`RDPGD_PG_PASS`)

	p := pg.NewPG(address, pgPort, `rdpg`, `rdpg`, pgPass)
	db, err := p.Connect()
	if err != nil {
		return nil, err
	}
	defer db.Close()
	err = db.Select(&locks, sql)
	if err != nil {
		log.Error(fmt.Sprintf("admin.getLockCountByDatabase ! db.Select(&locks, %s) erred : %s", sql, err.Error()))
		return nil, err
	}
	return locks, nil
}

//func getNumberOfDatabaseBackupOnDisk() (rowCount int) {
//	sq := `SELECT datname AS Name,  pg_catalog.pg_get_userbyid(datdba) AS Owner, pg_catalog.pg_database_size(datname) AS size
//FROM pg_catalog.pg_database
//WHERE datname LIKE 'd%' or datname = 'rdpg'
//ORDER BY 3 DESC;`
//	rowCount, err := getRowCount(sq)
//	if err != nil {
//		log.Error(fmt.Sprintf("admin.getNumberOfDatabaseBackupOnDisk() Could not get row count running query %s ! %s", sq, err))
//		return -1
//	}
//	return
//}
