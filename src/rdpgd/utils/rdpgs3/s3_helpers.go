package rdpgs3

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/starkandwayne/rdpgd/globals"
	"github.com/starkandwayne/rdpgd/history"
	"github.com/starkandwayne/rdpgd/log"
)

//TODO: This code is currently duplicated from tasks.s3. It needs to be deleted
//			from there when John is done doing whatever it is he's doing in that file.
var (
	AWSSecretKey      string
	AWSAccessKey      string
	BucketName        string
	AWSRegion         string
	Configured        bool
	Token             string
	Endpoint          string
	S3ForcePathStyle  bool
	EnabledInManifest string
)

type ByKey []*s3.Object

func (k ByKey) Len() int           { return len(k) }
func (k ByKey) Swap(i, j int)      { k[i], k[j] = k[j], k[i] }
func (k ByKey) Less(i, j int) bool { return *k[i].Key < *k[j].Key }

func init() {
	setUpCreds()
}

//Call if you want to reinitialize the S3 Creds some time
// after startup has occurred.
func ReinitializeS3Credentials() (err error) {
	setUpCreds()
	return
}

func setUpCreds() {
	//Initialize values
	Configured = false
	Token = ``
	S3ForcePathStyle = true

	AWSAccessKey = os.Getenv(`RDPGD_S3_AWS_ACCESS`)
	AWSSecretKey = os.Getenv(`RDPGD_S3_AWS_SECRET`)
	BucketName = os.Getenv(`RDPGD_S3_BUCKET`)
	AWSRegion = os.Getenv(`RDPGD_S3_REGION`)
	Endpoint = os.Getenv(`RDPGD_S3_ENDPOINT`)
	EnabledInManifest = os.Getenv(`RDPGD_S3_BACKUPS`)

	if BucketName != `` && AWSAccessKey != `` && AWSSecretKey != `` && AWSRegion != `` && strings.ToUpper(EnabledInManifest) == `ENABLED` {
		Configured = true
	}
}

//Base function for getting a file from S3 and putting it somewhere on the local filesystem.
func CopyFileFromS3(source, destination, bucket string) (err error) {
	start := time.Now()
	f := history.S3FileHistory{}
	f.Status = "error" //Changed at end if successful
	spl := strings.Split(source, "/")
	basename := spl[len(spl)-1]
	//Log results to backups.file_history
	f.Source = source
	f.Target = destination
	f.Node = globals.MyIP
	f.Bucket = bucket
	f.FileName = basename
	defer func() {
		f.Duration = int(time.Since(start).Seconds())
		insertErr := history.InsertS3History(f)
		if insertErr != nil {
			log.Error(fmt.Sprintf("utils/rdpgs3.CopyFileFromS3 ! InsertS3History erred : %s", err.Error()))
		}
	}()

	if !Configured {
		errorMessage := "utils/rdpgs3.CopyFileFromS3 ! S3 Credentials not configured for this deployment!"
		log.Error(errorMessage)
		return errors.New(errorMessage)
	}

	creds := credentials.NewStaticCredentials(AWSAccessKey, AWSSecretKey, Token)

	config := &aws.Config{
		Region:           &AWSRegion,
		Endpoint:         &Endpoint,
		S3ForcePathStyle: &S3ForcePathStyle,
		Credentials:      creds,
	}

	s3client := s3.New(config)

	params := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(source),
	}
	resp, err := s3client.GetObject(params)
	if err != nil {
		// Print the error, cast err to awserr.Error to get the Code and
		// Message from an error.
		log.Error(fmt.Sprintf("tasks.StageRemoteRestore() AWS Error: ! %s", err.Error()))
		return
	}
	defer resp.Body.Close()

	pathArray := removeBlankSplit(strings.Split(destination, "/"))
	basefilename := pathArray[len(pathArray)-1]
	pathArray = pathArray[0 : len(pathArray)-1]
	if len(pathArray) == 0 {
		errorMessage := "utils/rdpgs3.CopyFileFromS3 ! Must specify destination path"
		log.Error(errorMessage)
		return errors.New(errorMessage)
	}
	folderPath := "/"
	for _, v := range pathArray {
		folderPath += (v + "/")
	}

	// make sure the folder exists before we create the file
	err = os.MkdirAll(folderPath, 0777)
	if err != nil {
		log.Error(fmt.Sprintf("utils/backup.CopyFileFromS3 ! Could not create target folder %s ! %s", folderPath, err))
		return err
	}

	downloadFile, err := os.Create(folderPath + basefilename)
	if err != nil {
		log.Error(fmt.Sprintf("utils/backup.CopyFileFromS3 ! attempting to create file error: ! %s", err))
		return err
	}

	defer downloadFile.Close()

	f.Size, err = io.Copy(downloadFile, resp.Body)

	if err != nil {
		log.Error(fmt.Sprintf("utils/backup.CopyFileFromS3 ! Failed to copy object to file ! %s", err))
		return err
	}

	f.Status = "ok"
	return err
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
