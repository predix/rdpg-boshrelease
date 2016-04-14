package tasks

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/starkandwayne/rdpgd/globals"
	"github.com/starkandwayne/rdpgd/history"
	"github.com/starkandwayne/rdpgd/log"
	"github.com/starkandwayne/rdpgd/utils/backup"
	"github.com/starkandwayne/rdpgd/utils/rdpgs3"
)

type s3Credentials struct {
	awsSecretKey      string
	awsAccessKey      string
	bucketName        string
	awsRegion         string
	configured        bool
	token             string
	endpoint          string
	s3ForcePathStyle  bool
	enabledInManifest string
}

//S3FileMetadata - Basic meta data needed for all file manipulations of backup files
type S3FileMetadata struct {
	Location  string `json:"location"`
	DBName    string `json:"dbname"`
	Node      string `json:"node"`
	ClusterID string `json:"cluster_id"`
}

//S3FileDownload - Meta data needed for copying files from an s3 bucket
type S3FileDownload struct {
	Source string `json:"source"`
	// make sure the folder exists before we create the file
	Target string `json:"target"`
	Bucket string `json:"bucket"`
	DBName string `json:"dbname"`
}

//FindFilesToCopyToS3 - Responsible for copying files, such as database backups
//to S3 storage
func (t *Task) FindFilesToCopyToS3() (err error) {
	if err != nil {
		log.Error(fmt.Sprintf("tasks.FindFilesToCopyToS3() Could not retrieve S3 Credentials ! %s", err))
		return err
	}

	//If S3 creds/bucket aren't set just exit since they aren't configured
	if rdpgs3.Configured == false {
		log.Error(fmt.Sprintf("tasks.FindFilesToCopyToS3() S3 CONFIGURATION MISSING FOR THIS DEPLOYMENT ! S3 Credentials are not configured, skipping attempt to copy until configured "))
		return
	}

	//Select eligible files
	//Diff with empty string means get me the diff for ALL THE THINGS
	localDiff, _, err := backup.Diff("", true)
	if err != nil {
		log.Error(fmt.Sprintf(`tasks.Task<%d>#CopyFileToS3() Failed to load list of files ! %s`, t.ID, err))
		return
	}
	numFilesToCopy := 0
	for _, dbWithBackupsToCopy := range localDiff {
		numFilesToCopy += len(dbWithBackupsToCopy.Backups)
	}

	log.Trace(fmt.Sprintf("tasks.FindFilesToCopyToS3() > Found %d files to copy over %d unique databases", numFilesToCopy, len(localDiff)))

	//Loop and add Tasks CopyFileToS3
	for _, dbWithBackupsToCopy := range localDiff {
		for _, backupToCopy := range dbWithBackupsToCopy.Backups {
			//Gather the info necessary for uploading the file.
			fm := S3FileMetadata{}
			fm.Location = backup.Location(dbWithBackupsToCopy.Database, backupToCopy.Name)
			fm.DBName = dbWithBackupsToCopy.Database
			fm.Node = globals.MyIP
			fm.ClusterID = globals.ClusterID
			//JSONify that info
			fileToCopyParams, err := json.Marshal(fm)
			if err != nil {
				log.Error(fmt.Sprintf("tasks.FindFilesToCopyToS3() > Error attempting to marshal some JSON ! %+v %s", fm, err))
				return err
			}
			log.Trace(fmt.Sprintf("tasks.FindFilesToCopyToS3() > Attempting to add %s", fileToCopyParams))
			//Insert the task
			newTask := Task{ClusterID: t.ClusterID, Node: t.Node, Role: t.Role, Action: "CopyFileToS3", Data: string(fileToCopyParams), TTL: t.TTL, NodeType: t.NodeType}
			err = newTask.Enqueue()
			if err != nil {
				log.Error(fmt.Sprintf(`tasks.FindFilesToCopyToS3() service task schedules ! %s`, err))
			}
		}

	}
	return

}

//CopyFileToS3 - Responsible for copying a file to S3
func (t *Task) CopyFileToS3() (err error) {
	start := time.Now()

	f := history.S3FileHistory{}
	f.Status = "error" //Changed at the end of the function if successful
	f.Bucket = rdpgs3.BucketName
	defer func() {
		f.Duration = int(time.Since(start).Seconds())
		insertErr := history.InsertS3History(f)
		if insertErr != nil {
			log.Error(fmt.Sprintf("tasks.CopyFileToS3 ! insertS3History erred : %s", err.Error()))
		}
	}()

	taskParams := []byte(t.Data)
	fm := S3FileMetadata{}
	err = json.Unmarshal(taskParams, &fm)
	if err != nil {
		log.Error(fmt.Sprintf("tasks.CopyFileToS3() json.Unmarshal() ! %s", err))
		return err
	}

	//Log results to backups.file_history
	f.Source = fm.Location
	f.Target = fm.Location
	f.DBName = fm.DBName
	f.Node = fm.Node
	creds := credentials.NewStaticCredentials(rdpgs3.AWSAccessKey, rdpgs3.AWSSecretKey, rdpgs3.Token)

	config := &aws.Config{
		Region:           &rdpgs3.AWSRegion,
		Endpoint:         &rdpgs3.Endpoint,
		S3ForcePathStyle: &rdpgs3.S3ForcePathStyle,
		Credentials:      creds,
	}

	s3client := s3.New(config)

	file, err := os.Open(fm.Location)
	if err != nil {
		log.Error(fmt.Sprintf("tasks.CopyFileToS3() Error attempting to open file %s ! %s", fm.Location, err))
		return err
	}

	defer file.Close()

	fileInfo, _ := file.Stat()
	f.Size = fileInfo.Size()
	f.FileName = fileInfo.Name()
	buffer := make([]byte, f.Size)
	file.Read(buffer)
	fileBytes := bytes.NewReader(buffer) // convert to io.ReadSeeker type
	fileType := http.DetectContentType(buffer)

	s3params := &s3.PutObjectInput{
		Bucket:        aws.String(rdpgs3.BucketName), // required
		Key:           aws.String(fm.Location),       // required
		ACL:           aws.String("public-read"),     //other values: http://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html#CannedACL
		Body:          fileBytes,
		ContentLength: aws.Int64(f.Size),
		ContentType:   aws.String(fileType),
		Metadata: map[string]*string{
			"Key": aws.String("MetadataValue"), //required
		},
		// see more at http://godoc.org/github.com/aws/aws-sdk-go/service/s3#S3.PutObject
	}

	result, err := s3client.PutObject(s3params)
	log.Trace(fmt.Sprintf("tasks.CopyFileToS3() Copy file to S3 result > %s ", result))

	if err != nil {
		log.Error(fmt.Sprintf("tasks.CopyFileToS3() AWS General Error ! %s", err))
		if awsErr, ok := err.(awserr.Error); ok {
			// Generic AWS Error with Code, Message, and original error (if any)
			log.Error(fmt.Sprintf("tasks.CopyFileToS3() AWS Error %s !! %s ! %s", awsErr.Code(), awsErr.Message(), awsErr.OrigErr()))
			if reqErr, ok := err.(awserr.RequestFailure); ok {
				// A service error occurred
				log.Error(fmt.Sprintf("tasks.CopyFileToS3() AWS Service Error %s !!! %s !! %s ! %s", reqErr.Code(), reqErr.Message(), reqErr.StatusCode(), reqErr.RequestID()))
			}
		} else {
			// This case should never be hit, the SDK should always return an
			// error which satisfies the awserr.Error interface.
			log.Error(fmt.Sprintf("tasks.CopyFileToS3() General AWS Error %s ! ", err.Error()))
		}
	}

	return
}
