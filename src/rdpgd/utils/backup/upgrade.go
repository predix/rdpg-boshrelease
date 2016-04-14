package backup

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/starkandwayne/rdpgd/config"
	"github.com/starkandwayne/rdpgd/globals"
	"github.com/starkandwayne/rdpgd/log"
	"github.com/starkandwayne/rdpgd/pg"
	"github.com/starkandwayne/rdpgd/utils/rdpgs3"
)

func AddBackupPathConfig(dc *config.DefaultConfig) (err error) {
	log.Trace("Entering AddBackupPathConfig")
	if dc.Key != "BackupsPath" {
		errorMessage := fmt.Sprintf("utils/backup.AddBackupPathConfig ! Key specified: %s != 'BackupsPath'", dc.Key)
		log.Error(errorMessage)
		return errors.New(errorMessage)
	}
	p := pg.NewPG(`127.0.0.1`, globals.PGPort, `rdpg`, `rdpg`, globals.PGPass)
	p.Set(`database`, `rdpg`)

	db, err := p.Connect()
	if err != nil {
		log.Error(fmt.Sprintf(`config.DefaultConfig() Could not open connection ! %s`, err))
	}
	defer db.Close()

	oldConfigs := []config.DefaultConfig{}
	sql := fmt.Sprintf("SELECT key, cluster_id, value FROM rdpg.config WHERE key = 'BackupsPath' AND cluster_id = '%s';", dc.ClusterID)
	err = db.Select(&oldConfigs, sql)
	//If there is no preexisting config, then just insert this...
	if len(oldConfigs) == 0 {
		sq := fmt.Sprintf(`INSERT INTO rdpg.config (key,cluster_id,value) SELECT '%s', '%s', '%s' WHERE NOT EXISTS (SELECT key FROM rdpg.config WHERE key = '%s' AND cluster_id = '%s')`, dc.Key, dc.ClusterID, dc.Value, dc.Key, dc.ClusterID)
		log.Trace(fmt.Sprintf(`config.DefaultConfig.Add(): %s`, sq))
		_, err = db.Exec(sq)
		if err != nil {
			log.Error(fmt.Sprintf(`config.DefaultConfig.Add():%s`, err))
			return err
		}
	} else { //Otherwise, need to check if we need to move the backup files.
		if oldConfigs[0].Value != dc.Value {
			//If the path has changed, move the files.
			sq := fmt.Sprintf(`UPDATE rdpg.config SET value = '%s' WHERE key = '%s' AND cluster_id = '%s';`, dc.Value, dc.Key, dc.ClusterID)
			log.Trace(fmt.Sprintf(`config.DefaultConfig.Add(): %s`, sq))
			_, err = db.Exec(sq)
			if err != nil {
				log.Error(fmt.Sprintf(`config.DefaultConfig.Add():%s`, err))
				return err
			}
			err = MoveBackupFiles(oldConfigs[0].Value, dc.Value)
			var localError error = nil
			if err != nil {
				log.Error(fmt.Sprintf("utils/backup.AddBackupPathConfig() ! utils/backup.MoveBackupFiles erred: %s", err.Error()))
				//Still want to try remote move. Don't just explode now. Return this error later if necessary.
				localError = err
				err = nil
			}
			if rdpgs3.Configured {
				err = MoveRemoteBackupFiles(oldConfigs[0].Value, dc.Value)
				if err != nil {
					log.Error(fmt.Sprintf("utils/backup.AddBackupPath() ! utils/backup.MoveRemoteBackupFiles erred: %s", err.Error()))
					return err
				}
			}
			return localError //is nil by default. Only returns error if MoveBackupFiles erred.
		}
	}
	return nil
}

// Walks the folders in the old backup directory, and moves them to the location of
// the backups in this new deployment
func MoveBackupFiles(oldPath, newPath string) (err error) {
	os.Stat(oldPath + "/")
	if err != nil {
		if os.IsNotExist(err) {
			//The directory isn't there. Nothing to copy over. Everything is probably fine.
			return nil
		}
		//Otherwise, we have an issue. Something like a permissions problem, maybe.
		log.Error(fmt.Sprintf("tasks.MoveBackupFiles() ! os.Stat(%s) erred : %s", oldPath+"/", err.Error()))
		return err
	}
	oldDirectory, err := ioutil.ReadDir(oldPath + "/")
	if err != nil {
		log.Error(fmt.Sprintf("tasks.MoveBackupFiles() ! ioutil.ReadDir(%s) erred : %s", oldPath+"/", err.Error()))
		return err
	}
	toMatch := GenerateFiletypeMatcher(true)
	for _, f := range oldDirectory {
		//recursively call this function until we're copying some actual got-dang files.
		if f.IsDir() {
			//Make the new directory to move the files into
			err = os.MkdirAll(newPath+"/"+f.Name(), 0777)
			if err != nil {
				log.Error(fmt.Sprintf("tasks.MoveBackupFiles ! os.MkdirAll(%s, 0777) erred : %s", newPath+"/"+f.Name(), err.Error()))
				//Should exit here because the above error will otherwise cause the following recursion to fail silently.
				return err
			}
			//Actually move the files there.
			err = MoveBackupFiles(oldPath+"/"+f.Name(), newPath+"/"+f.Name())
			if err != nil {
				log.Error(fmt.Sprintf("tasks.MoveBackupFiles ! MoveBackupFiles(%s, %s) erred : %s", oldPath+"/"+f.Name(), newPath+"/"+f.Name(), err.Error()))
				continue
			}
			//Remove the old directory.
			err = os.Remove(oldPath + "/" + f.Name())
			if err != nil {
				log.Error(fmt.Sprintf("tasks.MoveBackupFiles ! os.Remove erred(%s) erred : %s", oldPath+"/"+f.Name(), err.Error()))
				//Don't exit here. Could be the result of a singular error.
			}
		} else {
			matched, err := regexp.Match(toMatch, []byte(f.Name()))
			if err != nil {
				log.Error(fmt.Sprintf("regexp.Match(%s) erred : %s", toMatch, err.Error()))
				return err
			}
			if matched {
				log.Trace(fmt.Sprintf("tasks.MoveBackupFiles ! Calling os.Rename(%s, %s)", oldPath+"/"+f.Name(), newPath+"/"+f.Name()))
				err = os.Rename(oldPath+"/"+f.Name(), newPath+"/"+f.Name())
			}
		}
		if err != nil {
			log.Error(fmt.Sprintf("tasks.MoveBackupFiles() ! os.Rename(%s, %s) erred : %s", oldPath+"/"+f.Name(), newPath+"/"+f.Name(), err.Error()))
			//Don't quit for this... this could be an isolated error.
			//If that ends up causing weird issues... change it later
		}
	}
	return nil
}

func MoveRemoteBackupFiles(oldPath, newPath string) (err error) {
	log.Trace("tasks.MoveRemoteBackupFiles ! Beginning to move remote backup files")
	if !rdpgs3.Configured {
		errorMessage := "tasks.MoveRemoteBackupFiles !  s3 storage is not configured; exiting"
		log.Error(errorMessage)
		return errors.New(errorMessage)
	}
	//Helper functions rely on pointing to a specific place for globals.LocalBackupLocation...
	// so, I'm going to hack that for the duration of this function
	storedPath := globals.LocalBackupPath
	globals.LocalBackupPath = oldPath
	defer func() { globals.LocalBackupPath = storedPath }() //mmmm lambdas
	//Get all of the old files...
	backupList, err := RemoteListing("", true)
	if err != nil {
		log.Error(fmt.Sprintf("tasks.MoveRemoteBackupFiles ! backup.RemoteListing erred : %s", err.Error()))
		return err
	}
	//Move them over to the new location...
	err = moveS3(backupList, oldPath, newPath)
	if err != nil {
		log.Error(fmt.Sprintf("tasks.MoveRemoteBackupFiles ! tasks.moveS3 erred : %s", err.Error()))
		return err
	}
	return nil
}

//A helper for MoveRemoteBackupFiles that does the heavy lifting of copying all of the old backup
// files to the new location, and then deleting away the old versions.
func moveS3(backupList []DatabaseBackupList, oldPath, newPath string) (err error) {
	var copiedBackups []DatabaseBackupList = nil
	creds := credentials.NewStaticCredentials(rdpgs3.AWSAccessKey, rdpgs3.AWSSecretKey, rdpgs3.Token)
	config := &aws.Config{
		Region:           &rdpgs3.AWSRegion,
		Endpoint:         &rdpgs3.Endpoint,
		S3ForcePathStyle: &rdpgs3.S3ForcePathStyle,
		Credentials:      creds,
	}
	s3client := s3.New(config)
	baseSourcePath := rdpgs3.BucketName + oldPath
	baseDestPath := strings.TrimPrefix(newPath, "/")
	//Copy every backup
	for _, db := range backupList {
		dbSourcePath := baseSourcePath + "/" + db.Database
		dbDestPath := baseDestPath + "/" + db.Database
		thisDBEntry := DatabaseBackupList{Database: db.Database, Backups: nil}
		for _, thisBackup := range db.Backups {
			thisSource := dbSourcePath + "/" + thisBackup.Name
			thisDest := dbDestPath + "/" + thisBackup.Name
			input := &s3.CopyObjectInput{
				CopySource: &thisSource,
				Bucket:     &rdpgs3.BucketName,
				Key:        &thisDest,
			}
			_, err := s3client.CopyObject(input)
			if err != nil {
				log.Error(fmt.Sprintf("tasks.moveS3 ! s3.CopyObject [ %s => %s ] erred : %s", thisSource, rdpgs3.BucketName+"/"+thisDest, err.Error()))
				//on to the next one... skip adding it to the list of successfully copied items
				continue
			}
			thisDBEntry.Backups = append(thisDBEntry.Backups, thisBackup)
		}
		if len(thisDBEntry.Backups) > 0 {
			copiedBackups = append(copiedBackups, thisDBEntry)
		}
	}

	//Delete the old copies of the s3 objects
	var objectsToDelete []*s3.ObjectIdentifier = nil
	for _, dbToDelete := range copiedBackups {
		for _, backupToDelete := range dbToDelete.Backups {
			location := strings.TrimPrefix(Location(dbToDelete.Database, backupToDelete.Name), "/")
			objectsToDelete = append(objectsToDelete, &(s3.ObjectIdentifier{Key: &location}))
		}
	}

	input := s3.DeleteObjectsInput{Bucket: &rdpgs3.BucketName, Delete: &(s3.Delete{Objects: objectsToDelete})}
	_, err = s3client.DeleteObjects(&input)
	if err != nil {
		log.Error(fmt.Sprintf("tasks.EnforceRemoteFileRetention ! Error in s3.DeleteObjects : %s", err))
		return err
	}

	return
}
