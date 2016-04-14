package backup

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/starkandwayne/rdpgd/pg"

	"github.com/starkandwayne/rdpgd/globals"

	"github.com/starkandwayne/rdpgd/log"
	"github.com/starkandwayne/rdpgd/utils/rdpgs3"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/service/s3"
)

//Struct representing a list of backups for a specific database.
type DatabaseBackupList struct {
	Database string // The name of the database for which this object lists backups.
	Backups  []DBBackup
}

//Represents a single backup object.
type DBBackup struct {
	Name string // The name of the backup file.
	Size string // The size of the backup file.
}

//Represents a row in the backup.retention_rules table of the RDPG database.
type RetentionRuleRow struct {
	DBName       string  `db:"dbname"`
	Hours        float64 `db:"hours"`
	IsRemoteRule bool    `db:"is_remote_rule"`
}

type RetentionPolicy struct {
	DBName      string
	LocalHours  float64
	RemoteHours float64
}

const backupFileSuffix string = ".sql"
const globalsFileSuffix string = ".globals"

//For sortDBList
//--ByDBName
type ByDBName []DatabaseBackupList

func (db ByDBName) Len() int           { return len(db) }
func (db ByDBName) Swap(i, j int)      { db[i], db[j] = db[j], db[i] }
func (db ByDBName) Less(i, j int) bool { return db[i].Database < db[j].Database }

//--ByFilename
type ByFilename []DBBackup

func (f ByFilename) Len() int           { return len(f) }
func (f ByFilename) Swap(i, j int)      { f[i], f[j] = f[j], f[i] }
func (f ByFilename) Less(i, j int) bool { return f[i].Name < f[j].Name }

// Returns a listing of backup files on the local filesystem for the specified
//		database.
// @param dbname
//				The name of the database on this cluster to return backups for. If
//				the dbname given is an empty string, the backups for all databases
//				this cluster will be returned.
// @return
//				A slice of DatabaseBackupList objects where each "Database" entry
//				contains the name of the database to which the backups refer, and
//				the Backups slice within contains entries with information about
//				each backup for that database.
//				The return slice is sorted ascendingly by database name, and the backups
//				within are sorted by their filenames (aka timestamps).
func LocalListing(dbname string, showGlobals bool) (backupList []DatabaseBackupList, err error) {
	backupList = []DatabaseBackupList{}
	dirListing, err := ioutil.ReadDir(localBackupLocation())
	if err != nil {
		log.Trace(fmt.Sprintf("admin.backup.handleLocalListing() No backups present on this cluster."))
		err = nil
		return
	}
	matchingString := GenerateFiletypeMatcher(showGlobals)
	for _, dir := range dirListing {
		if (dir.Name() == dbname || dbname == "") && dir.IsDir() {
			thisDatabase := DatabaseBackupList{dir.Name(), []DBBackup{}}
			backupFiles, err := ioutil.ReadDir(localBackupLocation() + dir.Name())
			// This could result from a folder getting deleted between the original search and
			// trying to access it. This shouldn't happen, as even moving backups to S3 shouldn't cause the backup
			// folder to get deleted... so treat it as a true error.
			if err != nil {
				log.Error(fmt.Sprintf("Error when attempting to open directory: %s ! %s", localBackupLocation()+dir.Name(), err))
				return backupList, errors.New("An error occurred when trying to open a backup directory")
			}
			for _, f := range backupFiles {
				matched, err := regexp.Match(matchingString, []byte(f.Name()))
				if err != nil {
					log.Error(fmt.Sprintf(`admin.backup.handleLocalListing() Error when attempting regexp: %s / %s ! %s`, matchingString, f.Name(), err))
					return backupList, errors.New("A regexp error occurred")
				}
				//The match matches on "<basename><backupFileSuffix>" e.g "asdf.sql"
				if f.Mode().IsRegular() && matched {
					thisDatabase.Backups = append(thisDatabase.Backups, DBBackup{f.Name(), strconv.FormatInt(f.Size(), 10)})
				}
			}
			if len(thisDatabase.Backups) > 0 {
				backupList = append(backupList, thisDatabase)
			}
		}
	}
	sortDBList(&backupList)
	return
}

// Returns a listing of backup files on the remote storage for the specified
//		database.
// @param dbname
//				The name of the database on this cluster to return backups for. If
//				the dbname given is an empty string, the backups for all databases
//				this cluster will be returned.
// @return
//				A slice of DatabaseBackupList objects where each "Database" entry
//				contains the name of the database to which the backups refer, and
//				the Backups slice within contains entries with information about
//				each backup for that database.
//				The return slice is sorted ascendingly by database name, and the backups
//				within are sorted by their filenames (aka timestamps).
func RemoteListing(dbname string, showGlobals bool) (backupList []DatabaseBackupList, err error) {
	log.Trace("In admin.backup.handleRemoteListing")
	err = rdpgs3.ReinitializeS3Credentials()
	if err != nil {
		log.Error("admin.backup.handleRemoteListing ! Failure in ReinitializeS3Credentials.")
		return
	}

	var ourPrefix string
	//If no database specified, look for all databases
	if dbname == "" {
		ourPrefix = strings.TrimPrefix(localBackupLocation(), "/")
	} else {
		ourPrefix = strings.TrimPrefix(localBackupLocation(), "/") + dbname + "/"
	}
	done := false
	input := &s3.ListObjectsInput{
		Bucket: &rdpgs3.BucketName,
		Prefix: &ourPrefix,
	}
	backupList = []DatabaseBackupList{}
	for !done {
		output, err := remoteHelper(&backupList, input, showGlobals)
		if err != nil {
			log.Error(fmt.Sprintf("utils/backup.RemoteListing ! %s", err.Error()))
			return nil, err
		}
		done = !(*output.IsTruncated)
		if !done {
			input = &s3.ListObjectsInput{
				Bucket: &rdpgs3.BucketName,
				Prefix: &ourPrefix,
				Marker: output.Contents[len(output.Contents)-1].Key,
			}
		}
	}

	sortDBList(&backupList)
	return
}

func remoteHelper(backupList *[]DatabaseBackupList, input *s3.ListObjectsInput, showGlobals bool) (output *s3.ListObjectsOutput, err error) {
	creds := credentials.NewStaticCredentials(rdpgs3.AWSAccessKey, rdpgs3.AWSSecretKey, rdpgs3.Token)
	config := &aws.Config{
		Region:           &rdpgs3.AWSRegion,
		Endpoint:         &rdpgs3.Endpoint,
		S3ForcePathStyle: &rdpgs3.S3ForcePathStyle,
		Credentials:      creds,
	}

	s3client := s3.New(config)

	output, err = s3client.ListObjects(input)
	if err != nil {
		log.Error(fmt.Sprintf("admin.backup.handleRemoteListing ! Error when trying to get objects from s3. ! %s", err.Error()))
		return
	}

	var thisDatabase DatabaseBackupList = DatabaseBackupList{Database: "", Backups: []DBBackup{}}

	cont := output.Contents
	//The Contents list is a list of s3 objects which
	// has a Key value which is the full name of the file
	// in the storage, including its "path"
	sort.Sort(rdpgs3.ByKey(cont))
	matchingString := GenerateFiletypeMatcher(showGlobals)

	//For each returned content in this page from S3...
	for _, c := range cont {
		keySplit := strings.Split(*c.Key, "/")
		if len(keySplit) < 2 {
			log.Warn(fmt.Sprintf("utils/backup.RemoteListing ! S3 key improper structure: %s", *c.Key))
			continue
		}
		filename := keySplit[len(keySplit)-1]
		database := keySplit[len(keySplit)-2]
		if thisDatabase.Database != database {
			//Database set to empty string on first runthrough.
			if thisDatabase.Database != "" && len(thisDatabase.Backups) > 0 {
				*backupList = append(*backupList, thisDatabase)
			}
			index := FindDatabaseBackups(database, *backupList)
			if err != nil {
				log.Error(fmt.Sprintf("utils/backup.remoteHelper ! %s", err))
				return output, err
			}
			//if the database isn't in the list already...
			if index < 0 {
				thisDatabase = DatabaseBackupList{database, []DBBackup{}}
			} else { //Add to the one already in the list
				thisDatabase = (*backupList)[index]
			}
		}

		//Make sure it's a backup file by regexping for the proper suffix.
		matched, err := regexp.Match(matchingString, []byte(filename))
		if err != nil {
			log.Error(fmt.Sprintf(`utils/backup.RemoteListing() Error when attempting regexp: %s / %s ! %s`, matchingString, filename, err))
			return output, errors.New("A regexp error occurred")
		}
		//The match matches on "<basename><backupFileSuffix>" e.g "asdf.sql"
		if matched {
			thisDatabase.Backups = append(thisDatabase.Backups, DBBackup{filename, strconv.FormatInt(*c.Size, 10)})
		}
	}
	if len(cont) > 0 {
		*backupList = append(*backupList, thisDatabase)
	}

	return
}

//Returns an array of database backup lists containing the backups found for the
// given database, regardless of whether the backup is local or remote. If dbname is the
// empty string, the backups for all databases are returned.
func FullListing(dbname string, showGlobals bool) (backupList []DatabaseBackupList, err error) {
	local, err := LocalListing(dbname, showGlobals)
	if err != nil {
		log.Error(fmt.Sprintf("utils/backup.FullListing ! utils/backup.LocalListing erred : %s", err.Error()))
		return nil, err
	}
	if rdpgs3.Configured {
		remote, err := RemoteListing(dbname, showGlobals)
		if err != nil {
			log.Error(fmt.Sprintf("utils/backup.FullListing ! utils/backup.RemoteListing erred : %s", err.Error()))
			return nil, err
		}
		backupList = Combine(local, remote)
	} else {
		backupList = local
	}
	return backupList, nil
}

// Performs a binary search over the provided slice of DatabaseBackupLists, which
// is known to be sorted in ascending order by database name, and returns the
// index at which that DatabaseBackupList is located. Returns -1 if that list is
// not found in the slice.
func FindDatabaseBackups(db string, backupList []DatabaseBackupList) int {
	cursor := len(backupList) / 2
	left := 0                //Inclusive
	right := len(backupList) //Exclusive
	for right > left {
		if backupList[cursor].Database == db {
			return cursor
		} else if backupList[cursor].Database < db { //Look higher
			left = cursor + 1
			cursor = left + ((right - left) / 2)
		} else { // Look lower
			right = cursor
			cursor = left + ((right - left) / 2)
		}
	}
	return -1
}

// Performs a binary search for the database specified, and if it exists, it then
// performs another binary search over that databases's list of backups. Returns
// true if the specified database is in the list, and its list of backups also
// contains a backup by the given name. Returns false otherwise.
func ContainsBackup(db, basefilename string, backupList []DatabaseBackupList) bool {
	index := FindDatabaseBackups(db, backupList)
	if index >= 0 {
		backups := backupList[index].Backups
		cursor := len(backups) / 2
		left := 0             //Inclusive
		right := len(backups) //Exclusive
		for right > left {
			if backups[cursor].Name == basefilename {
				return true
			} else if backups[cursor].Name < basefilename { //Look higher
				left = cursor + 1
				cursor = left + ((right - left) / 2)
			} else { // Look lower
				right = cursor
				cursor = left + ((right - left) / 2)
			}
		}
	}
	return false
}

//Assumes both inputs are sorted (they are produced that way)
func Combine(first, second []DatabaseBackupList) []DatabaseBackupList {
	var output []DatabaseBackupList = nil
	cursor1, cursor2 := 0, 0
	length1, length2 := len(first), len(second)
	for cursor1 < length1 && cursor2 < length2 {
		var thisDB DatabaseBackupList
		if first[cursor1].Database == second[cursor2].Database {
			thisDB = DatabaseBackupList{Database: first[cursor1].Database, Backups: nil}
			//Merge their values
			b1, b2 := first[cursor1].Backups, second[cursor2].Backups //Handles for arrays
			c1, c2 := 0, 0                                            //Cursors for the internal backup arrays
			l1, l2 := len(b1), len(b2)                                //Lengths for internal backup arrays
			for c1 < l1 && c2 < l2 {
				var thisBackup DBBackup
				if b1[c1].Name == b2[c2].Name {
					thisBackup = b1[c1]
					c1++
					c2++
				} else if b1[c1].Name < b2[c2].Name {
					thisBackup = b1[c1]
					c1++
				} else {
					thisBackup = b2[c2]
					c2++
				}
				thisDB.Backups = append(thisDB.Backups, thisBackup)
			}
			for ; c1 < l1; c1++ {
				thisDB.Backups = append(thisDB.Backups, b1[c1])
			}
			for ; c2 < l2; c2++ {
				thisDB.Backups = append(thisDB.Backups, b2[c2])
			}
			cursor1++
			cursor2++
		} else if first[cursor1].Database < second[cursor2].Database {
			thisDB = first[cursor1]
			cursor1++
		} else {
			thisDB = second[cursor2]
			cursor2++
		}
		output = append(output, thisDB)
	}
	for ; cursor1 < length1; cursor1++ {
		output = append(output, first[cursor1])
	}
	for ; cursor2 < length2; cursor2++ {
		output = append(output, second[cursor2])
	}
	return output
}

func GenerateFiletypeMatcher(showGlobals bool) (matchingString string) {
	if showGlobals {
		matchingString = fmt.Sprintf(".+(%s|%s)\\z", regexp.QuoteMeta(backupFileSuffix), regexp.QuoteMeta(globalsFileSuffix))
	} else {
		matchingString = fmt.Sprintf(".+%s\\z", regexp.QuoteMeta(backupFileSuffix))
	}
	return
}

func Diff(dbname string, showGlobals bool) (localDiff, remoteDiff []DatabaseBackupList, err error) {
	local, err := LocalListing(dbname, showGlobals)
	if err != nil {
		log.Error("Error encountered in utils/backup.Diff ! utils.backup.LocalListing")
		return nil, nil, err
	}
	remote, err := RemoteListing(dbname, showGlobals)
	if err != nil {
		log.Error("Error encountered in utils/backup.Diff ! utils.backup.RemoteListing")
		return nil, nil, err
	}
	localDiff = []DatabaseBackupList{}
	remoteDiff = []DatabaseBackupList{}
	for _, l := range local {
		//Find the corresponding remote backup
		index := FindDatabaseBackups(l.Database, remote)
		//If the database is not in the remote list, add all of the local backups to the local diff
		if index < 0 {
			localDiff = append(localDiff, l)
		} else { //Otherwise, we need to find specifically what to add to the diff, if anything
			r := remote[index]
			lToAdd := DatabaseBackupList{Database: l.Database, Backups: []DBBackup{}}
			rToAdd := DatabaseBackupList{Database: r.Database, Backups: []DBBackup{}}
			//Walk each array for differences
			//Note that the lists are sorted in ascending order
			lCurs, rCurs := 0, 0
			for lCurs < len(l.Backups) && rCurs < len(r.Backups) {
				if l.Backups[lCurs].Name == r.Backups[rCurs].Name {
					//Then these are corresponding backups
					lCurs++
					rCurs++
				} else {
					//We have some sort of diff
					if l.Backups[lCurs].Name < r.Backups[rCurs].Name {
						//There is a backup in local that is not in remote.
						lToAdd.Backups = append(lToAdd.Backups, l.Backups[lCurs])
						lCurs++
					} else {
						//There is a backup in remote that is not in local
						rToAdd.Backups = append(rToAdd.Backups, r.Backups[rCurs])
						rCurs++
					}
				}
			}
			//Add any remaining locals to its diff
			for ; /*<<PREDECLARED*/ lCurs < len(l.Backups); lCurs++ {
				lToAdd.Backups = append(lToAdd.Backups, l.Backups[lCurs])
			}
			//Add any remaining remotes to its diff
			for ; /*<<PREDECLARED*/ rCurs < len(r.Backups); rCurs++ {
				rToAdd.Backups = append(rToAdd.Backups, r.Backups[rCurs])
			}
			if len(lToAdd.Backups) > 0 {
				localDiff = append(localDiff, lToAdd)
			}
			if len(rToAdd.Backups) > 0 {
				remoteDiff = append(remoteDiff, rToAdd)
			}
			//Remove this entry from the remote listing, for ease of checking what's left later.
			if index < len(remote)-1 {
				remote = append(remote[:index], remote[index+1:]...)
			} else {
				remote = remote[:len(remote)-1]
			}
		}
	}
	//Whatever is left here is only on remote
	for _, r := range remote {
		remoteDiff = append(remoteDiff, r)
	}
	sortDBList(&localDiff) //This may already be sorted by nature of the algorithm...
	sortDBList(&remoteDiff)
	return
}

func Both(dbname string, showGlobals bool) (bothList []DatabaseBackupList, err error) {
	local, err := LocalListing(dbname, showGlobals)
	if err != nil {
		log.Error(fmt.Sprintf("utils/backup.Both ! utils.backup.LocalListing : %s", err))
		return nil, err
	}
	remote, err := RemoteListing(dbname, showGlobals)
	if err != nil {
		log.Error(fmt.Sprintf("utils/backup.Diff ! utils.backup.RemoteListing : %s", err))
		return nil, err
	}
	bothList = []DatabaseBackupList{}
	for _, l := range local {
		//Find the corresponding remote backup
		index := FindDatabaseBackups(l.Database, remote)
		if index >= 0 {
			r := remote[index]
			toAdd := DatabaseBackupList{Database: r.Database, Backups: []DBBackup{}}
			//Walk each array for differences
			//Note that the lists are sorted in ascending order
			lCurs, rCurs := 0, 0
			for lCurs < len(l.Backups) && rCurs < len(r.Backups) {
				if l.Backups[lCurs].Name == r.Backups[rCurs].Name {
					//Then these are corresponding backups
					toAdd.Backups = append(toAdd.Backups, l.Backups[lCurs])
					lCurs++
					rCurs++
				} else {
					//We have some sort of diff
					if l.Backups[lCurs].Name < r.Backups[rCurs].Name {
						//There is a backup in local that is not in remote.
						lCurs++
					} else {
						//There is a backup in remote that is not in local
						rCurs++
					}
				}
			}
			if len(toAdd.Backups) > 0 {
				bothList = append(bothList, toAdd)
			}
		}
	}
	sortDBList(&bothList) //This may already be sorted by nature of the algorithm...
	return
}

func sortDBList(backupList *[]DatabaseBackupList) {
	for _, v := range *backupList {
		sort.Sort(ByFilename(v.Backups))
	}
	sort.Sort(ByDBName(*backupList))
}

func DatabaseExists(dbname string) (exists bool, err error) {
	exists = false
	p := pg.NewPG(`127.0.0.1`, os.Getenv(`RDPGD_PB_PORT`), `rdpg`, `rdpg`, os.Getenv(`RDPGD_PG_PASS`))
	return p.DatabaseExists(dbname)
}

// Returns the correct path on the filesystem for where a file with a given name
// for a given database should be/go on the local filesystem
func Location(dbname, filename string) string {
	return globals.LocalBackupPath + "/" + dbname + "/" + filename
}

func localBackupLocation() string {
	return globals.LocalBackupPath + "/"
}
