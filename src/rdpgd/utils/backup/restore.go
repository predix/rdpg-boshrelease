package backup

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/starkandwayne/rdpgd/config"
	"github.com/starkandwayne/rdpgd/globals"
	"github.com/starkandwayne/rdpgd/history"
	"github.com/starkandwayne/rdpgd/log"
	"github.com/starkandwayne/rdpgd/pg"
	"github.com/starkandwayne/rdpgd/utils/rdpgs3"
)

//Used to facilate locking the restore stage
var restoreLock sync.Mutex

func init() {
	restoreLock = sync.Mutex{}
}

// A wrapper for ImportSqlFile which handles the additional process of
// finding the backup whereever it is stored (local or remote) and putting it in
// the correct place, and then restoring it from that location.
func RestoreInPlace(dbname, basefile string) (err error) {
	if strings.Contains(basefile, "/") {
		errorMessage := fmt.Sprintf("utils/backup.RestoreInPlace ! '%s' is not a file base name.", basefile, err.Error())
		log.Error(errorMessage)
		return errors.New(errorMessage)
	}
	err = StageRestoreInPlace(dbname, basefile)
	if err != nil {
		log.Error(fmt.Sprintf("utils/backup.RestoreInPlace ! utils/backup.StageRestoreInPlace(%s, %s) erred : %s", dbname, basefile, err.Error()))
		return err
	}

	p := pg.NewPG("127.0.0.1", globals.PBPort, "rdpg", "rdpg", globals.PGPass)
	if err != nil {
		log.Error(fmt.Sprintf(`utils/backup.RestoreInPlace ! pg.NewPG("127.0.0.1", %s, "rdpg", "rdpg", %s) erred : %s`, globals.PBPort, globals.PGPass, err.Error()))
		return err
	}

	exists, err := DatabaseExists(dbname)
	if err != nil {
		log.Error(fmt.Sprintf("utils/backup.RestoreInPlace ! utils/backup.DatabaseExists(%s) erred : %s", dbname, err.Error()))
		return err
	}
	if exists {
		err = p.DisableDatabase(dbname)
		if err != nil {
			log.Error(fmt.Sprintf("utils.backup.RestoreInPlace ! pg.DisableDatabase(%s) erred : %s", dbname, err.Error()))
			return err
		}
		err = p.DropDatabase(dbname)
		if err != nil {
			log.Error(fmt.Sprintf("utils/backup.RestoreInPlace ! pg.DropDatabase(%s) erred : %s", dbname, err.Error()))
			return err
		}
	} else {
		errorMessage := fmt.Sprintf("utils/backup.RestoreInPlace ! Restoring database %s doesn't currently exist.", dbname)
		log.Warn(errorMessage)
	}

	username := "u" + strings.TrimPrefix(dbname, "d")
	err = p.CreateDatabase(dbname, username)
	if err != nil {
		log.Error(fmt.Sprintf("utils/backup.StageRestoreInPlace ! pg.CreateDatabase(%s, %s) erred : %s", dbname, username, err.Error()))
		return err
	}

	err = ImportSqlFile(dbname, RestoreLocation(dbname, basefile))
	if err != nil {
		log.Error(fmt.Sprintf("utils/backup.RestoreInPlace ! utils/backup.ImportSqlFile(%s, %s) erred : %s", dbname, RestoreLocation(dbname, basefile), err.Error()))
		return err
	}

	err = UnstageRestore(dbname, basefile)
	if err != nil {
		log.Error(fmt.Sprintf("utils/backup.RestoreInPlace ! UnstageRestore(%s, %s) erred : %s", dbname, basefile, err.Error()))
		return err
	}

	return nil
}

// Restores a database given the name of the database, and the absolute path to
// the backup file.
func ImportSqlFile(dbname, filepath string) (err error) {
	log.Trace(fmt.Sprintf("utils/backup.ImportSqlFile ! Beginning restore of database %s", dbname))
	start := time.Now()
	f := history.BackupFileHistory{}
	f.Status = "error"
	f.BackupFile = filepath
	f.BackupPathAndFile = filepath
	f.DBName = dbname
	f.Node = globals.MyIP
	defer func() {
		f.Duration = int(time.Since(start).Seconds())
		insertErr := history.InsertRestoreHistory(f)
		if insertErr != nil {
			log.Error(fmt.Sprintf("utils/backup.ImportSqlFile ! Unable to record history for BackupFileHistory: %+v: %s", f, insertErr.Error()))
		}
	}()

	//Make sure database actually exists first.
	exists, err := DatabaseExists(dbname)
	if err != nil {
		log.Error(fmt.Sprintf("utils/backup.ImportSqlFile ! utils/backup.DatabaseExists(%s) erred : %s", dbname, err.Error()))
		return err
	}
	if !exists {
		errorMessage := fmt.Sprintf("utils/backup.ImportSqlFile ! No database found with name: %s", dbname)
		log.Error(errorMessage)
		return errors.New(errorMessage)
	}
	pgPort, err := config.GetValue(`BackupPort`)
	if err != nil {
		log.Error(fmt.Sprintf("utils/backup.ImportSqlFile ! config.GetValue(`BackupPort`) erred : %s", err.Error()))
		return err
	}

	lockRestore()
	log.Trace(fmt.Sprintf("utils/backup.RestoreInPlace ! Executing %s -p %s -U vcap -d %s -f %s", globals.PSQL_PATH, pgPort, dbname, filepath))
	out, err := exec.Command(globals.PSQL_PATH, "-p", pgPort, "-U", "vcap", "-d", dbname, "-f", filepath).CombinedOutput()
	unlockRestore()
	if err != nil {
		log.Error(fmt.Sprintf(`utils/backup.ImportSqlFile ! Error running pg_dump command for: %s out: %s ! %s`, dbname, out, err))
		return err
	}

	log.Trace(fmt.Sprintf("utils/backup.ImportSqlFile ! Restored database: %s", dbname))
	f.Status = "ok"
	return
}

// Copies a backup file into the restore stage and adds it to the register for
// what is prepared to be restored. Any given database can only be staged once
// at any given time - an attempt to stage a database that is already staged
// will delete the previously staged backup and the new file will be registered
// for recovery instead.
// The data section of the task should be valid JSON containing string values
// for the keys "database_name" and "base_file_name"
func StageRestoreInPlace(dbname, filename string) (err error) {
	//BDR can't automatically restore, silly!
	if !globals.CanAutoRestore {
		log.Warn("utils/backup.StageRestoreInPlace ! Attempt made to stage a restore, but this cluster cannot automate restores")
		return errors.New("This cluster cannot perform automated restores.")
	}
	dbExists, err := DatabaseExists(dbname)
	if err != nil {
		log.Error(fmt.Sprintf("utils/backup.StageRestoreInPlace ! backup.DatabaseExists(%s) erred : %s", dbname, err.Error()))
		return err
	}
	if !dbExists {
		errorMessage := fmt.Sprintf("utils/backup.StageRestoreInPlace ! Database doesn't exist: %s", dbname)
		log.Error(errorMessage)
		return errors.New(errorMessage)
	}
	if !strings.HasPrefix(dbname, "d") || utf8.RuneCountInString(dbname) != 33 {
		log.Warn(fmt.Sprintf("utils/backup.StageRestoreInPlace ! Rejecting attempt made to restore non-user database: %s", dbname))
		return errors.New("Cannot auto-restore non-user database")
	}

	var local, remote []DatabaseBackupList = nil, nil

	//I was told to ignore .globals files... change this later if that changes.
	local, err = LocalListing(dbname, false)
	if err != nil {
		log.Error(fmt.Sprintf("utils/backup.StageRestoreInPlace ! backup.LocalListing(%s, false) erred : %s", dbname, err.Error()))
		return err
	}
	if rdpgs3.Configured {
		remote, err = RemoteListing(dbname, false)
		if err != nil {
			log.Error(fmt.Sprintf("utils/backup.StageRestoreInPlace ! backup.RemoteListing(%s, false) erred : %s", dbname, err.Error()))
			return err
		}
	}

	all := Combine(local, remote)
	if err != nil {
		log.Error(fmt.Sprintf("utils/backup.StageRestoreInPlace ! utils/backup.FullListing(%s, false) erred : %s", dbname, err.Error()))
		return err
	}

	//If no backup is specified, use the most recent backup for this database
	if dbname == "" {
		//Need to check if there are any backups before defaulting to the newest backup.
		if len(all) == 0 || len(all[0].Backups) == 0 {
			errorMessage := fmt.Sprintf("utils/backup.StageRestoreInPlace ! No backups found for this database.")
			log.Warn(errorMessage)
			return errors.New(errorMessage)
		}
		dbname = all[0].Backups[len(all[0].Backups)-1].Name
	} else if !ContainsBackup(dbname, filename, all) {
		errorMessage := fmt.Sprintf("utils/backup.StageRestoreInPlace ! No backup with name '%s' found for database '%s'", filename, dbname)
		log.Error(errorMessage)
		return errors.New(errorMessage)
	}

	//Look for this backup in the local filesystem
	isLocal := ContainsBackup(dbname, filename, local)
	//At this point, we know that the desired backup is SOMEWHERE. So if it isn't
	// local (!isLocal) then it must be in remote storage.

	//If we're at this point in the code, the backup actually exists.
	//Actually get the file from whereever it is.
	if isLocal {
		err = stageLocalInPlace(dbname, filename)
		if err != nil {
			log.Error(fmt.Sprintf("utils/backup.StageRestoreInPlace ! stageLocalInPlace erred : %s", err.Error()))
			return err
		}
	} else {
		err = stageRemoteInPlace(dbname, filename)
		if err != nil {
			log.Error(fmt.Sprintf("utils/backup.StageRestoreInPlace ! stageRemoteInPlace erred : %s", err.Error()))
			return err
		}
	}
	return nil
}

func stageLocalInPlace(dbname, filename string) (err error) {
	src := Location(dbname, filename)
	dest := RestoreLocation(dbname, filename)
	lockRestore()
	err = copyFile(src, dest)
	unlockRestore()
	if err != nil {
		log.Error(fmt.Sprintf("utils/backup.stageLocalInPlace ! copyFile(%s, %s) erred", dest, src))
		return err
	}
	return nil
}

func copyFile(src, dest string) (err error) {
	if _, err := os.Stat(src); os.IsNotExist(err) {
		errorMessage := fmt.Sprintf("Source file %s doesn't exist!", src)
		return errors.New(errorMessage)
	}
	srcFile, err := os.Open(src)
	if err != nil {
		log.Error(fmt.Sprintf("utils/backup.copyFile ! os.Open(%s) erred : %s", src, err.Error()))
		return err
	}
	defer srcFile.Close()

	pathArray := removeBlankSplit(strings.Split(dest, "/"))
	directory := ""
	if strings.HasPrefix(dest, "/") {
		directory = "/"
	}
	if len(pathArray) > 0 {
		for _, v := range pathArray[:len(pathArray)-1] {
			directory += (v + "/")
		}
	}
	err = os.MkdirAll(directory, 0777)
	if err != nil {
		log.Error(fmt.Sprintf("utils.backup.copyFile ! os.MkdirAll(%s) erred : %s", directory, err.Error()))
		return err
	}
	destFile, err := os.Create(dest)
	if err != nil {
		log.Error(fmt.Sprintf("utils/backup.copyFile ! os.Create(%s) erred : %s", dest, err.Error()))
		return err
	}
	defer destFile.Close()
	numBytes, err := io.Copy(destFile, srcFile)
	if err != nil {
		return err
	}
	log.Trace(fmt.Sprintf("utils/backup.copyFile ! %d bytes were copied", numBytes))
	return nil
}

// Gets the backup file with the given base name for the given database
// and puts it into the restore stage.
func stageRemoteInPlace(dbname, filename string) (err error) {
	destinationPath := RestoreLocation(dbname, filename)
	sourceKey := strings.TrimPrefix(Location(dbname, filename), "/")

	lockRestore()
	err = rdpgs3.CopyFileFromS3(sourceKey, destinationPath, rdpgs3.BucketName)
	unlockRestore()
	if err != nil {
		log.Error(fmt.Sprintf("utils/backup.stageRemoteInPlace ! CopyFileFromS3 erred : %s", err.Error()))
		return err
	}

	return nil
}

func UnstageRestore(database, filename string) (err error) {
	fileToRemove := RestoreLocation(database, filename)
	err = os.Remove(fileToRemove)
	if err != nil {
		log.Error(fmt.Sprintf("utils/backup.UnstageRestore ! os.Remove(%s) erred : %s", fileToRemove, err.Error()))
	}
	return err
}

func RestoreLocation(dbname, filename string) string {
	return globals.RestoreStagePath + "/" + dbname + "/" + filename
}

//Right now, this is a pretty damn coarse lock. One restore at a time.
// I eventually want to lock on a database-by-database. uh... //TODO
func lockRestore() {
	restoreLock.Lock()
}

func unlockRestore() {
	restoreLock.Unlock()
}

func removeBlankSplit(input []string) (ret []string) {
	ret = nil
	for _, v := range input {
		if v != "" {
			ret = append(ret, v)
		}
	}
	return
}
