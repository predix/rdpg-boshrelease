package history

import (
	"fmt"

	"github.com/starkandwayne/rdpgd/config"
	"github.com/starkandwayne/rdpgd/globals"
	"github.com/starkandwayne/rdpgd/log"
	"github.com/starkandwayne/rdpgd/utils/rdpgpg"
)

type BackupFileHistory struct {
	BackupFile        string
	BackupPathAndFile string
	DBName            string
	Node              string
	Status            string
	Duration          int
}

type S3FileHistory struct {
	FileName string
	Source   string
	Target   string
	DBName   string
	Size     int64
	Node     string
	Status   string
	Duration int
	Bucket   string
}

//DeleteBackupHistory - Responsible for deleting records from backups.file_history
//older than the value in rdpg.config.key = defaultDaysToKeepFileHistory
func DeleteBackupHistory() (err error) {
	daysToKeep, err := config.GetValue(`defaultDaysToKeepFileHistory`)
	log.Trace(fmt.Sprintf("tasks.DeleteBackupHistory() Keeping %s days of file history in backups.file_history", daysToKeep))

	address := `127.0.0.1`
	sq := fmt.Sprintf(`DELETE FROM backups.file_history WHERE created_at < NOW() - '%s days'::interval; `, daysToKeep)

	err = rdpgpg.ExecQuery(address, sq)
	if err != nil {
		log.Error(fmt.Sprintf(`history.DeleteBackupHistory() Error when running query %s ! %s`, sq, err))
	}
	return
}

func InsertBackupHistory(f BackupFileHistory) (err error) {
	address := `127.0.0.1`
	sq := fmt.Sprintf(`INSERT INTO backups.file_history(cluster_id, dbname, node, file_name, action, status, duration, params) VALUES ('%s','%s','%s','%s','%s','%s',%d,'{"location":"%s","dbname":"%s","node":"%s","cluster_id":"%s"}')`, globals.ClusterID, f.DBName, f.Node, f.BackupFile, `CreateBackup`, f.Status, f.Duration, f.BackupPathAndFile, f.DBName, f.Node, globals.ClusterID)
	err = rdpgpg.ExecQuery(address, sq)
	if err != nil {
		log.Error(fmt.Sprintf("history.insertHistory() Error inserting record into backups.file_history, running query: %s ! %s", sq, err))
	}
	return
}

func InsertBackupHistoryDumpAll(f BackupFileHistory) (err error) {
	address := `127.0.0.1`
	sq := fmt.Sprintf(`INSERT INTO backups.file_history(cluster_id, dbname, node, file_name, action, status, duration, params) VALUES ('%s','%s','%s','%s','%s','%s',%d,'{"location":"%s","dbname":"%s","node":"%s","cluster_id":"%s"}')`, globals.ClusterID, f.DBName, f.Node, f.BackupFile, `CreateBackupDumpAll`, f.Status, f.Duration, f.BackupPathAndFile, f.DBName, f.Node, globals.ClusterID)
	err = rdpgpg.ExecQuery(address, sq)
	if err != nil {
		log.Error(fmt.Sprintf("history.insertHistory() Error inserting record into backups.file_history, running query: %s ! %s", sq, err))
	}
	return
}

func InsertS3History(f S3FileHistory) (err error) {
	address := `127.0.0.1`
	sq := fmt.Sprintf(`INSERT INTO backups.file_history(cluster_id, dbname, node, file_name, action, status, duration, params) VALUES ('%s','%s','%s','%s','%s','%s',%d,'{"source":"%s", "target":"%s", "size":"%d", "bucket":"%s"}')`, globals.ClusterID, f.DBName, f.Node, f.FileName, `CopyToS3`, f.Status, f.Duration, f.Source, f.Target, f.Size, f.Bucket)
	err = rdpgpg.ExecQuery(address, sq)
	if err != nil {
		log.Error(fmt.Sprintf("history.insertHistory() backup.S3FileHistory  Error inserting record into backups.file_history, running query: %s ! %s", sq, err))
	}
	return
}

func InsertS3HistoryCopyFromS3(f S3FileHistory) (err error) {
	address := `127.0.0.1`
	sq := fmt.Sprintf(`INSERT INTO backups.file_history(cluster_id, dbname, node, file_name, action, status, duration, params) VALUES ('%s','%s','%s','%s','%s','%s',%d,'{"source":"%s", "target":"%s", "size":"%d", "bucket":"%s"}')`, globals.ClusterID, f.DBName, f.Node, f.FileName, `CopyFromS3`, f.Status, f.Duration, f.Source, f.Target, f.Size, f.Bucket)
	err = rdpgpg.ExecQuery(address, sq)
	if err != nil {
		log.Error(fmt.Sprintf("history.insertHistory() backup.S3FileHistory  Error inserting record into backups.file_history, running query: %s ! %s", sq, err))
	}
	return
}

func InsertRestoreHistory(f BackupFileHistory) (err error) {
	address := `127.0.0.1`
	sq := fmt.Sprintf(`INSERT INTO backups.file_history(cluster_id, dbname, node, file_name, action, status, duration, params) VALUES ('%s','%s','%s','%s','%s','%s',%d,'{"location":"%s","dbname":"%s","node":"%s","cluster_id":"%s"}')`, globals.ClusterID, f.DBName, f.Node, f.BackupFile, `RestoreBackup`, f.Status, f.Duration, f.BackupPathAndFile, f.DBName, f.Node, globals.ClusterID)
	err = rdpgpg.ExecQuery(address, sq)
	if err != nil {
		log.Error(fmt.Sprintf("history.InsertRestoreHistory() Error inserting record into backups.file_history, running query: %s ! %s", sq, err))
	}
	return
}
