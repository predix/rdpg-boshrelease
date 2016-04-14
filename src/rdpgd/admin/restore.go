package admin

import (
	"fmt"
	"net/http"

	"github.com/starkandwayne/rdpgd/utils/backup"
)

// Responds to POST
// Expects two url-encoded arguments
// dbname: The name of the db to restore in place
// filename: The base name of the file to use for the restore. It will be located
//					by RDPG whereever it is stored. For a list of available files to use
//					for restores, GET /backup/list
func RestoreInPlaceHandler(w http.ResponseWriter, request *http.Request) {
	dbname := request.FormValue("dbname")
	filename := request.FormValue("filename")
	if dbname == "" || filename == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Please specify the url-encoded arguments 'dbname' and 'filename'"))
		return
	}
	err := backup.RestoreInPlace(dbname, filename)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	w.Write([]byte(fmt.Sprintf("Restore completed for database '%s' with backup '%s'", dbname, filename)))
	return
}
