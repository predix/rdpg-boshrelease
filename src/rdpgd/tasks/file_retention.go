package tasks

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/starkandwayne/rdpgd/utils/rdpgs3"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/starkandwayne/rdpgd/globals"
	"github.com/starkandwayne/rdpgd/utils/rdpgpg"

	"github.com/starkandwayne/rdpgd/utils/backup"

	"github.com/starkandwayne/rdpgd/log"
)

/*EnforceFileRetention - Responsible for adding removing files which are no longer
needed on the local file system.  For example, backup files which have been created
successfully locally and copied to S3 successfully can be deleted to preserve
local disk storage */
func (t *Task) EnforceFileRetention() (err error) {

	/*
	   If s3 copy is enabled you cannot delete files until they have been copied to s3
	   otherwise keep the most recent backups, say the last 48 hours worth and delete all others
	*/
	//Select eligible files
	var eligibleBackups []backup.DatabaseBackupList = nil
	//Get the list of backups that exist locally...
	var localList []backup.DatabaseBackupList
	//If S3 backups are enabled, then don't delete a local file unless it has been backed up already
	if isS3FileCopyEnabled() {
		//Both gets backups that are both in the local filesystem and the remote storage
		localList, err = backup.Both("", true)
		if err != nil {
			log.Error(fmt.Sprintf("tasks.Task<%d>#EnforceFileRetention() ! utils/backup#Both() : %s", t.ID, err.Error()))
			return err
		}
	} else {
		localList, err = backup.LocalListing("", true)
		if err != nil {
			log.Error(fmt.Sprintf("tasks.Task<%d>#EnforceFileRetention() ! utils/backup.LocalListing() : %s", t.ID, err.Error()))
			return err
		}
	}
	eligibleBackups, err = findExpiredFiles(localList, false)
	if err != nil {
		log.Error(fmt.Sprintf("tasks.EnforceFileRetention() ! tasks.findExpiredFiles() : %s", err.Error()))
		return err
	}
	numFilesToDelete := 0
	for _, v := range eligibleBackups {
		numFilesToDelete += len(v.Backups)
	}
	if err != nil {
		log.Error(fmt.Sprintf(`tasks.Task<%d>#EnforceFileRetention() Failed to load list of files ! %s`, t.ID, err))
		return err
	}

	log.Trace(fmt.Sprintf("tasks.EnforceFileRetention() > Found %d files to delete", numFilesToDelete))

	for _, eligibleDatabase := range eligibleBackups {
		for _, backupToDelete := range eligibleDatabase.Backups {
			fm := S3FileMetadata{
				Location:  backup.Location(eligibleDatabase.Database, backupToDelete.Name),
				DBName:    backupToDelete.Name,
				Node:      globals.MyIP,
				ClusterID: globals.ClusterID,
			}
			byteParams, err := json.Marshal(fm)
			if err != nil {
				log.Error(fmt.Sprintf("tasks.EnforceFileRetention() > Error attempting to marshal some JSON ! %+v %s", fm, err))
				return err
			}
			fileToDeleteParams := string(byteParams)
			log.Trace(fmt.Sprintf("tasks.EnforceFileRetention() > Attempting to add %s", fileToDeleteParams))
			newTask := Task{ClusterID: t.ClusterID, Node: t.Node, Role: t.Role, Action: "DeleteFile", Data: fileToDeleteParams, TTL: t.TTL, NodeType: t.NodeType}
			err = newTask.Enqueue()
			if err != nil {
				log.Error(fmt.Sprintf(`tasks.EnforceFileRetention() service task schedules ! %s`, err))
			}
		}
	}

	return
}

//DeleteFile - Delete a file from the operating system
func (t *Task) DeleteFile() (err error) {
	taskParams := []byte(t.Data)
	fm := S3FileMetadata{}
	err = json.Unmarshal(taskParams, &fm)
	if err != nil {
		log.Error(fmt.Sprintf("tasks.DeleteFile() json.Unmarshal() ! %s", err))
	}
	log.Trace(fmt.Sprintf(`tasks.DeleteFile() Attempting to delete file "%s" `, fm.Location))
	err = os.Remove(fm.Location)
	if err != nil {
		log.Error(fmt.Sprintf(`tasks.DeleteFile() Attempted to delete file "%s" ! %s`, fm.Location, err))
	} else {
		//As long as the file was deleted, update the history record
		address := `127.0.0.1`
		sq := fmt.Sprintf(`UPDATE backups.file_history SET removed_at = CURRENT_TIMESTAMP WHERE params::text = '%s'`, t.Data)
		err = rdpgpg.ExecQuery(address, sq)
		if err != nil {
			log.Error(fmt.Sprintf(`tasks.DeleteFile() Attempted to update backups.file_history using query <<<%s>>> ! %s`, sq, err))
		}

	}

	return
}

func isS3FileCopyEnabled() (isEnabled bool) {
	return strings.ToUpper(os.Getenv(`RDPGD_S3_BACKUPS`)) == "ENABLED"
}

/*EnforceRemoteFileRetention - Responsible for adding removing files which are no longer
needed on the remote storage.  For example, if S3 storage is enabled, files which are
older than the retention cutoff will be deleted in order to preserve space. This function
will produce an error if S3 Storage is not enabled or properly configured for this RDPG
deployment.
*/
func (t *Task) EnforceRemoteFileRetention() (err error) {
	//Is S3 even enabled?
	if !isS3FileCopyEnabled() {
		errorMessage := "tasks.EnforceRemoteFileRetention ! S3 Storage is not enabled for this deployment"
		log.Error(errorMessage)
		return errors.New(errorMessage)
	}
	//If S3 is enabled, is all of the necessary information at least filled in?
	if !rdpgs3.Configured {
		errorMessage := "tasks.EnforceRemoteFileRetention ! S3 storage information has missing fields"
		log.Error(errorMessage)
		return errors.New(errorMessage)
	}

	var eligibleBackups []backup.DatabaseBackupList = nil
	remoteList, err := backup.RemoteListing("", true)
	if err != nil {
		log.Error(fmt.Sprintf("tasks.EnforceRemoteFileRetention ! Error in util/backup.RemoteListing(\"\", true) : %s", err))
		return err
	}
	eligibleBackups, err = findExpiredFiles(remoteList, true)
	if err != nil {
		log.Error(fmt.Sprintf("tasks.EnforceFileRetention() ! tasks.findExpiredFiles() : %s", err.Error()))
	}

	numFilesToDelete := 0
	for _, v := range eligibleBackups {
		numFilesToDelete += len(v.Backups)
	}
	if err != nil {
		log.Error(fmt.Sprintf(`tasks.Task<%d>#EnforceRemoteFileRetention() Failed to load list of files ! %s`, t.ID, err))
	}

	log.Trace(fmt.Sprintf("tasks.EnforceRemoteFileRetention() > Found %d files to delete", numFilesToDelete))
	if numFilesToDelete == 0 {
		return nil
	}

	var objectsToDelete []*s3.ObjectIdentifier = nil
	for _, eligibleDatabase := range eligibleBackups {
		for _, backupToDelete := range eligibleDatabase.Backups {
			location := strings.TrimPrefix(backup.Location(eligibleDatabase.Database, backupToDelete.Name), "/")
			objectsToDelete = append(objectsToDelete, &(s3.ObjectIdentifier{Key: &location}))
		}
	}

	creds := credentials.NewStaticCredentials(rdpgs3.AWSAccessKey, rdpgs3.AWSSecretKey, rdpgs3.Token)
	config := &aws.Config{
		Region:           &rdpgs3.AWSRegion,
		Endpoint:         &rdpgs3.Endpoint,
		S3ForcePathStyle: &rdpgs3.S3ForcePathStyle,
		Credentials:      creds,
	}
	s3client := s3.New(config)

	input := s3.DeleteObjectsInput{Bucket: &rdpgs3.BucketName, Delete: &(s3.Delete{Objects: objectsToDelete})}
	_, err = s3client.DeleteObjects(&input)
	if err != nil {
		log.Error(fmt.Sprintf("tasks.EnforceRemoteFileRetention ! Error in s3.DeleteObjects : %s", err))
		return err
	}
	return nil
}

func findExpiredFiles(input []backup.DatabaseBackupList, remote bool) (eligible []backup.DatabaseBackupList, err error) {
	eligible = nil
	//And now grab the ones that are older than our retention cutoff
	for _, databaseWithBackups := range input {
		thisDB := backup.DatabaseBackupList{Database: databaseWithBackups.Database, Backups: []backup.DBBackup{}}
		for i, thisBackup := range databaseWithBackups.Backups {
			//If this is the most recent thisBackup...
			if i == len(databaseWithBackups.Backups)-1 {
				//Don't delete it, even if it is older than the cutoff
				continue
			}
			fnSplit := strings.Split(thisBackup.Name, ".")
			if len(fnSplit) != 2 {
				errorMessage := fmt.Sprintf("tasks.findExpiredFiles() ! Improper filename found: %s", thisBackup.Name)
				log.Error(errorMessage)
				return nil, errors.New(errorMessage)
			}
			//Filenames are timestamps of when they were made
			timestamp := fnSplit[0]
			//Files are from oldest to newest, so if we find one new enough to keep, we're done
			keep, err := backup.ShouldRetain(timestamp, databaseWithBackups.Database, remote)
			if err != nil {
				log.Error(fmt.Sprintf("tasks.findExpiredFiles ! utils/backup.ShouldRetain() erred : %s", err.Error()))
			}
			if keep {
				break
			}
			//If we're here, this is a file to delete from remote storage.
			thisDB.Backups = append(thisDB.Backups, thisBackup)
		}
		if len(thisDB.Backups) > 0 {
			eligible = append(eligible, thisDB)
		}
	}

	return
}
