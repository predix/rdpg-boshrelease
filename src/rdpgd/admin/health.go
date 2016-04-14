package admin

import (
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/starkandwayne/rdpgd/log"
	"github.com/starkandwayne/rdpgd/pg"
)

func Check(check string) (status int, err error) {
	p := pg.NewPG(`127.0.0.1`, pbPort, `health`, `health`, `check`)
	status = http.StatusOK
	switch check {
	case "ha_pb_pg":
		p.Set(`port`, `5432`)
		db, err := p.Connect()
		if err != nil {
			log.Error(fmt.Sprintf("admin.Check(%s) %s ! %s", check, p.URI, err))
			status = http.StatusInternalServerError
			return status, err
		}
		defer db.Close()
		_, err = db.Exec(`SELECT CURRENT_TIMESTAMP`)
		if err != nil {
			log.Error(fmt.Sprintf(`admin.Check(%s) ! %s`, check, err))
			status = http.StatusInternalServerError
			return status, err
		}
	case "pb":
		p.Set(`port`, pbPort)
		db, err := p.Connect()
		if err != nil {
			log.Error(fmt.Sprintf("admin.Check(%s) %s ! %s", check, p.URI, err))
			status = http.StatusInternalServerError
			return status, err
		}
		defer db.Close()
		_, err = db.Exec(`SELECT CURRENT_TIMESTAMP`)
		if err != nil {
			log.Error(fmt.Sprintf(`admin.Check(%s) ! %s`, check, err))
			status = http.StatusInternalServerError
			return status, err
		}
	case "pg":
		p.Set(`port`, pgPort)
		db, err := p.Connect()
		if err != nil {
			log.Error(fmt.Sprintf("admin.Check(%s) %s ! %s", check, p.URI, err))
			status = http.StatusInternalServerError
			return status, err
		}
		defer db.Close()
		_, err = db.Exec(`SELECT CURRENT_TIMESTAMP`)
		if err != nil {
			log.Error(fmt.Sprintf(`admin.Check(%s) ! %s`, check, err))
			status = http.StatusInternalServerError
			return status, err
		}
	default:
		status = http.StatusInternalServerError
		return status, err
	}
	return status, err
}

/*
(HC) GET /health/hapbpg
*/
func HealthHandler(w http.ResponseWriter, request *http.Request) {
	vars := mux.Vars(request)
	checkName := vars[`check`]
	switch request.Method {
	case `GET`:
		status, err := Check(checkName)
		if err != nil {
			msg := fmt.Sprintf(`{"status": %d, "description": "%s"}`, status, err)
			log.Error(fmt.Sprintf(`admin.HealthHandler(): Check(%s) ! %s`, checkName, msg))
			http.Error(w, msg, status)
		} else {
			msg := fmt.Sprintf(`{"status": %d, "description": "%s ok"}`, status, checkName)
			w.WriteHeader(status)
			fmt.Fprintf(w, msg)
		}
	default:
		msg := fmt.Sprintf(`{"status": %d, "description": "Method not allowed %s"}`, http.StatusMethodNotAllowed, request.Method)
		log.Error(fmt.Sprintf(`admin.HealthHandler(): Check(%s) ! %s`, checkName, msg))
		http.Error(w, msg, http.StatusMethodNotAllowed)
	}
}
