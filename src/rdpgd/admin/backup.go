/* Handlers for API calls to interact with the backup system */
package admin

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"

	"github.com/starkandwayne/rdpgd/utils/backup"

	"github.com/gorilla/mux"
	"github.com/starkandwayne/rdpgd/globals"
	"github.com/starkandwayne/rdpgd/instances"
	"github.com/starkandwayne/rdpgd/log"
	"github.com/starkandwayne/rdpgd/tasks"
	"github.com/starkandwayne/rdpgd/utils/rdpgconsul"
	"github.com/starkandwayne/rdpgd/utils/rdpgs3"
	//"strings"
)

const twoYears float64 = 17520.0

/* Should contain a form value dbname which equals the database name
   e.g. curl www.hostname.com/backup/now -X POST -d "dbname=nameofdatabase"
   The {how} should be either "now" or "enqueue" */
func BackupHandler(w http.ResponseWriter, request *http.Request) {
	vars := mux.Vars(request)
	dbname := request.FormValue("dbname")
	t := tasks.NewTask()
	t.Action = "BackupDatabase"
	t.Data = dbname
	t.Node = globals.MyIP
	t.Role = globals.ServiceRole
	t.TTL = 3600
	t.ClusterService = globals.ClusterService
	t.NodeType = "read"
	if rdpgconsul.IsWriteNode(globals.MyIP) {
		t.NodeType = "write"
	}

	var err error
	if dbname != "rdpg" {
		//Using FindByDatabase to determine if the database actually exists to be backed up.
		inst, err := instances.FindByDatabase(dbname)
		if err != nil {
			log.Error(fmt.Sprintf("admin.BackupHandler() instances.FindByDatabase(%s) Error occurred when searching for database.", dbname))
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("Error encountered while searching for database"))
			return
		}
		if inst == nil {
			//...then the database doesn't exist on this cluster.
			log.Debug(fmt.Sprintf("admin.BackupHandler() Attempt to initiate backup on non-existant database with name: %s", dbname))
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte("Database not found"))
			return
		}
	}

	switch vars[`how`] {
	//Immediately calls Backup() and performs the backup
	case "now":
		err = t.BackupDatabase()
		if err != nil {
			log.Error(fmt.Sprintf(`api.BackupHandler() Task.BackupDatabase() %+v ! %s`, t, err))
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("Error encountered while trying to perform backup"))
			return
		}
		w.Write([]byte("Backup completed."))
	case "enqueue":
		// Queues up a backup to be done with a worker thread gets around to it.
		// This call returns after the queuing process is done; not after the backup is done.
		err = t.Enqueue()
		if err != nil {
			log.Error(fmt.Sprintf(`api.BackupHandler() Task.Enqueue() %+v ! %s`, t, err))
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("Error while trying to queue"))
			return
		}
		w.Write([]byte("Backup successfully queued."))
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

/* Lists all the backup files, regardless of their location. Backups are returned in json format.
   The request must be a GET.
   Form values:
     "fmt", "filename" is the only supported value at present. Defaults to "filename"
            if absent or if left blank
     "dbname": the name of the database for which to query backups of. If left blank, returns the backups for
            all databases..
	 "globals": set to the string "true" if files with the suffix ".globals" should appear in the output.*/
func BackupListAllHandler(w http.ResponseWriter, request *http.Request) {
	printFormat := "filename"
	if request.FormValue("fmt") != "" {
		printFormat = request.FormValue("fmt")
	}
	showGlobals := false
	if request.FormValue("globals") == "true" {
		showGlobals = true
	}
	// If the dbname wasn't specified of if the field is blank, then return the backups of
	// all databases.
	dbname := request.FormValue("dbname")

	backupList, err := backup.FullListing(dbname, showGlobals)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("An error occurred when querying backups"))
	}
	switch printFormat {
	case "filename":
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(formatFilename(backupList)))
	//case "timestamp": TODO
	default:
		log.Debug(fmt.Sprintf(`api.BackupListHandler() Requested unsupported format.`))
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Unsupported Format Requested"))
	}
}

/* Lists all the backup files at the desired location.
   The {where} should be "local" or "remote". The "local" option finds all the backup
   files on the local filesystem. The "remote" option will display all of the
   backup files on the remote storage, such as S3. Backups are returned in json format.
   The request must be a GET.
   Form values:
     "fmt", "filename" is the only supported value at present. Defaults to "filename"
            if absent or if left blank
     "dbname": the name of the database for which to query backups of. If left blank, returns the backups for
            all databases.
	 "globals": set to the string "true" if files with the suffix ".globals" should appear in the output.*/
func BackupListHandler(w http.ResponseWriter, request *http.Request) {
	printFormat := "filename"
	if request.FormValue("fmt") != "" {
		printFormat = request.FormValue("fmt")
	}
	showGlobals := false
	if request.FormValue("globals") == "true" {
		showGlobals = true
	}
	backupList := []backup.DatabaseBackupList{}
	// If the dbname wasn't specified of if the field is blank, then return the backups of
	// all databases.
	dbname := request.FormValue("dbname")
	// Where are we getting the files from?
	vars := mux.Vars(request)
	var err error
	switch vars["where"] {
	case "local":
		backupList, err = backup.LocalListing(dbname, showGlobals)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}
	case "remote":
		rdpgs3.ReinitializeS3Credentials()
		if !rdpgs3.Configured {
			w.WriteHeader(http.StatusGone)
			w.Write([]byte("Remote storage has not been configured"))
		}
		backupList, err = backup.RemoteListing(dbname, showGlobals)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}
	default:
		w.WriteHeader(http.StatusNotFound)
		return
	}

	switch printFormat {
	case "filename":
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(formatFilename(backupList)))
	//case "timestamp": TODO
	default:
		log.Debug(fmt.Sprintf(`api.BackupListHandler() Requested unsupported format.`))
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Unsupported Format Requested"))
	}
}

func BackupDiffHandler(w http.ResponseWriter, request *http.Request) {
	//Default printing format to print pretty timestamps. So pretty.
	printFormat := "filename"
	if request.Method == "POST" && request.FormValue("fmt") != "" {
		printFormat = request.FormValue("fmt")
	}
	showGlobals := false
	if request.FormValue("globals") == "true" {
		showGlobals = true
	}

	// If the dbname wasn't specified of if the field is blank, then return the backups of
	// all databases.
	dbname := request.FormValue("dbname")
	// Where are we getting the files from?
	vars := mux.Vars(request)

	//Do we actually have s3?

	rdpgs3.ReinitializeS3Credentials()
	if !rdpgs3.Configured {
		w.WriteHeader(http.StatusGone)
		w.Write([]byte("Remote storage has not been configured"))
	}

	var localDiff, remoteDiff []backup.DatabaseBackupList
	var err error
	//The "both" path uses a different function
	if vars["where"] != "both" {
		localDiff, remoteDiff, err = backup.Diff(dbname, showGlobals)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}
	}

	//====Begin inner function
	formatter := func(localList []backup.DatabaseBackupList, remoteList []backup.DatabaseBackupList, printFormat string) (outputString string, err error) {
		if localList == nil && remoteList == nil {
			errorMessage := fmt.Sprintf("api.BackupDiffHandler ! Can't handle formatting without input")
			log.Error(errorMessage)
			return "", errors.New(errorMessage)
		}
		switch printFormat {
		case "filename":
			if localList == nil {
				// only/remote
				outputString = formatFilename(remoteList)
			} else if remoteList == nil {
				//only/local
				outputString = formatFilename(localList)
			} else {
				//only/both
				outputString = `{ "local": ` + formatFilename(localList) + `, "remote": ` + formatFilename(remoteList) + ` }`
			}
		default:
			log.Debug(fmt.Sprintf(`api.BackupDiffHandler() Requested unsupported format.`))
			return "", errors.New(fmt.Sprintf("Unsupported Printing Format: %s. Valid format is 'filename'", printFormat))
		}
		return
	}
	//====End inner function

	var output string
	switch vars["where"] {
	case "local":
		output, err = formatter(localDiff, nil, printFormat)
	case "remote":
		output, err = formatter(nil, remoteDiff, printFormat)
	case "diff":
		output, err = formatter(localDiff, remoteDiff, printFormat)
	case "both":
		bothList, err := backup.Both(dbname, false)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}
		output, err = formatter(bothList, nil, printFormat)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(output))
}

// fmt can be specified as a url argument. i.e. fmt='list'
// If fmt is 'list', a JSON array of rules is returned.
// If fmt is 'database', the JSON will be a map of databases with custom rules,
// attached to their respective custom rules.
func CustomRetentionRulesHandler(w http.ResponseWriter, request *http.Request) {
	//Get the specified printing format. Default to 'list'
	printFormat := "list"
	if request.FormValue("fmt") != "" {
		printFormat = request.FormValue("fmt")
	}

	//Attempt to get the list of custom retention rules
	rules, err := backup.GetCustomRetentionRules()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}

	var output []byte
	switch printFormat {
	case "list":
		output, err = json.Marshal(rules)
	case "database":
		output = reorganizeCustomRules(rules)
	default:
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(fmt.Sprintf("Unsupported print format: %s. Valid formats are 'list' and 'database'", printFormat)))
	}
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(output)
}

func RetentionPolicyHandler(w http.ResponseWriter, request *http.Request) {
	dbname := request.FormValue("dbname")
	if dbname == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Missing required form value: \"dbname\""))
		return
	}
	location := mux.Vars(request)["where"]
	var output []byte
	var err error
	var code int
	switch request.Method {
	case "GET", "":
		output, code, err = getPolicy(dbname, location)
	case "PUT":
		if request.FormValue("value") == "" {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("Missing required form value: \"value\""))
			return
		}
		var value float64
		value, err = strconv.ParseFloat(request.FormValue("value"), 64)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(fmt.Sprintf("Unable to parse value: %s", string(request.FormValue("value")))))
			return
		}
		output, code, err = setPolicy(dbname, location, value)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
	if err != nil {
		w.WriteHeader(code)
		w.Write([]byte(err.Error()))
		return
	}
	w.Write(output)
}

func getPolicy(dbname, location string) (output []byte, code int, err error) {
	policy, err := backup.GetRetentionPolicy(dbname)
	if err != nil {
		exists, innerErr := backup.DatabaseExists(dbname)
		//if error is EQUAL to nil. That's not a typo
		if innerErr == nil && !exists {
			return nil, http.StatusBadRequest, errors.New(fmt.Sprintf("No such database: %s", dbname))
		}
		return nil, http.StatusInternalServerError, err
	}
	switch location {
	case "local":
		return []byte(strconv.FormatFloat(policy.LocalHours, 'f', -1, 64)), http.StatusOK, nil
	case "remote":
		return []byte(strconv.FormatFloat(policy.RemoteHours, 'f', -1, 64)), http.StatusOK, nil
	default:
		return nil, http.StatusBadRequest, errors.New(fmt.Sprintf("Unrecognized location: %s", location))
	}
}

func setPolicy(dbname, location string, value float64) (output []byte, code int, err error) {
	var remote bool
	switch location {
	case "local":
		remote = false
	case "remote":
		remote = true
	default:
		return nil, http.StatusBadRequest, errors.New(fmt.Sprintf("Unrecognized location: %s", location))
	}
	if value > twoYears || value < 0.0 {
		return nil, http.StatusBadRequest, errors.New(fmt.Sprintf("Value %f is out of bounds.", value))
	}
	row := backup.RetentionRuleRow{DBName: dbname, Hours: value, IsRemoteRule: remote}
	err = row.Put()
	if err != nil {
		exists, innerErr := backup.DatabaseExists(dbname)
		if innerErr == nil && !exists {
			return nil, http.StatusBadRequest, err
		}
		return nil, http.StatusInternalServerError, err
	}
	return []byte("OK"), http.StatusCreated, nil
}

func reorganizeCustomRules(rows []backup.RetentionRuleRow) []byte {
	//I want to put it in a map for efficiency and ease of programming...
	// but you can't iterate over a map with range in a predetermined order.
	// So, keep another sortable array of database names to simulate such a thing.
	policies := make(map[string]backup.RetentionPolicy)
	//First, restructure the information in a convenient way to parse it.
	order := []string{}
	for _, v := range rows {
		thisPolicy := policies[v.DBName]
		order = append(order, v.DBName)
		//If we haven't considered this policy yet...
		if thisPolicy.DBName == "" {
			//Initialize its values
			thisPolicy.RemoteHours = -1.0
			thisPolicy.LocalHours = -1.0
			thisPolicy.DBName = "NOTEMPTYSTRING"
		}
		if v.IsRemoteRule {
			thisPolicy.RemoteHours = v.Hours
		} else {
			thisPolicy.LocalHours = v.Hours
		}
		policies[v.DBName] = thisPolicy
	}
	sort.Strings(order)
	//Now, form the JSON from that restructured form
	currentDB := ""
	output := "{ "
	separator := ""
	for _, name := range order {
		//The order slice can actually have duplicates. Just weeding through them.
		if name == currentDB {
			continue
		}
		currentDB = name
		thisLocal := ""
		thisRemote := ""
		subSeparator := ""
		if policies[name].LocalHours >= 0 {
			thisLocal = fmt.Sprintf("\"local\": %f", policies[name].LocalHours)
		}
		if policies[name].RemoteHours >= 0 {
			thisRemote = fmt.Sprintf("\"remote\": %f", policies[name].RemoteHours)
		}
		if policies[name].LocalHours >= 0 && policies[name].RemoteHours >= 0 {
			subSeparator = ", "
		}
		output += fmt.Sprintf("%s\"%s\": { %s%s%s }", separator, name, thisLocal, subSeparator, thisRemote)
		separator = ", "
	}
	output += "}"
	return []byte(output)
}

// JSONifies the DatabaseBackupList into lists of backup filenames and sizes.
func formatFilename(backupList []backup.DatabaseBackupList) (outputString string) {
	outputString = "{ "
	separator := ""
	for _, d := range backupList {
		outputString = outputString + fmt.Sprintf("%s\"%s\": [", separator, d.Database)
		//Inner loop should have "" as first separator
		separator = ""
		for _, v := range d.Backups {
			outputString = outputString + fmt.Sprintf("%s{ \"Name\": \"%s\", \"Bytes\": \"%s\" }", separator, v.Name, v.Size)
			separator = ", "
		}
		outputString = outputString + "]"
		//After the first run, separator should be a comma
		separator = ", "
	}
	outputString = outputString + "}"
	return
}

func RemoteCopyHandler(w http.ResponseWriter, request *http.Request) {
	dbname := request.FormValue("dbname")
	filename := request.FormValue("filename")
	//Can't copy to s3 if there's no s3.
	if !rdpgs3.Configured {
		w.WriteHeader(http.StatusGone)
		w.Write([]byte("Remote storage has not been configured"))
		return
	}

	if dbname == "" && filename != "" {
		//A backup for no database doesn't make any sense
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Cannot specify filename without database"))
		return
	}

	//Select eligible files
	filesToCopy, _, err := backup.Diff(dbname, true)
	if err != nil {
		log.Error(fmt.Sprintf(`api.CopyFileHelper() ! utils/backup.Diff(\"%s\", true) erred : %s`, dbname, err.Error()))
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Error getting file information"))
		return
	}
	//Determine this node type
	nType := "read"
	if rdpgconsul.IsWriteNode(globals.MyIP) {
		nType = "write"
	}

	numFiles := 0
	for _, dbWithBackupsToCopy := range filesToCopy {
		for _, backupToCopy := range dbWithBackupsToCopy.Backups {
			if filename != "" && backupToCopy.Name != filename {
				continue
			}
			//Gather the info necessary for uploading the file.
			fm := tasks.S3FileMetadata{}
			fm.Location = backup.Location(dbWithBackupsToCopy.Database, backupToCopy.Name)
			fm.DBName = dbWithBackupsToCopy.Database
			fm.Node = globals.MyIP
			fm.ClusterID = globals.ClusterID
			//JSONify that info
			fileToCopyParams, err := json.Marshal(fm)
			if err != nil {
				log.Error(fmt.Sprintf("tasks.FindFilesToCopyToS3() > Error attempting t)o marshal some JSON ! %+v %s", fm, err))
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte("An error occurred when marshalling JSON"))
				return
			}
			log.Trace(fmt.Sprintf("api.CopyFileHelper > Attempting to copy %s", fileToCopyParams))
			//Insert the task
			newTask := tasks.Task{
				ClusterID: globals.ClusterID,
				Node:      globals.MyIP,
				Role:      globals.ServiceRole,
				Action:    "CopyFileToS3",
				Data:      string(fileToCopyParams),
				TTL:       3600,
				NodeType:  nType,
			}
			err = newTask.CopyFileToS3()
			if err != nil {
				log.Error(fmt.Sprintf(`api.CopyFileHandler ! task.CopyFileToS3 erred : %s`, err.Error()))
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte("An error occurred when copying files to remote storage"))
				return
			}
			numFiles++
		}
	}

	w.Write([]byte(fmt.Sprintf("%d files were written to S3", numFiles)))

}
