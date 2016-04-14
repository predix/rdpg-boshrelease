package admin

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/starkandwayne/rdpgd/instances"
	"github.com/starkandwayne/rdpgd/log"
)

/*
POST /databases/register
PUT /databases/assign
*/
func DatabasesHandler(w http.ResponseWriter, request *http.Request) {
	vars := mux.Vars(request)
	log.Trace(fmt.Sprintf("admin.DatabasesHandler() > %s /databases/%s %+v", request.Method, vars["action"], vars))
	switch request.Method {
	case "GET":
		switch vars["action"] {
		case "": // List All Databases
			instances, err := instances.All()
			if err != nil {
				msg := fmt.Sprintf(`{"status": %d, "description": "%s"}`+"\n", http.StatusInternalServerError, err)
				log.Error(fmt.Sprintf(`admin.DatabasesHandler(): instances.All() %s %+v ! %s`, msg, vars, err))
				http.Error(w, msg, http.StatusInternalServerError)
				return
			}
			jsonInstances, err := json.Marshal(instances)
			if err != nil {
				msg := fmt.Sprintf(`{"status": %d, "description": "%s"}`+"\n", http.StatusInternalServerError, err)
				log.Error(fmt.Sprintf(`admin.DatabasesHandler(): json.Marshal(instances) %s %+v ! %s`, msg, vars, err))
				http.Error(w, msg, http.StatusInternalServerError)
			} else {
				w.Header().Set("Content-Type", "application/json; charset=UTF-8")
				w.WriteHeader(http.StatusOK)
				w.Write(jsonInstances)
			}
		case "available": // Lists Available Databases
			instances, err := instances.Available()
			if err != nil {
				msg := fmt.Sprintf(`{"status": %d, "description": "%s"}`+"\n", http.StatusInternalServerError, err)
				log.Error(fmt.Sprintf(`admin.DatabasesHandler(): instances.Available() %s %+v ! %s`, msg, vars, err))
				http.Error(w, msg, http.StatusInternalServerError)
				return
			}
			jsonInstances, err := json.Marshal(instances)
			if err != nil {
				msg := fmt.Sprintf(`{"status": %d, "description": "%s"}`+"\n", http.StatusInternalServerError, err)
				log.Error(fmt.Sprintf(`admin.DatabasesHandler(): json.Marshal(instances) %s %+v ! %s`, msg, vars, err))
				http.Error(w, msg, http.StatusInternalServerError)
			} else {
				w.Header().Set("Content-Type", "application/json; charset=UTF-8")
				w.WriteHeader(http.StatusOK)
				w.Write(jsonInstances)
			}
		default:
			msg := fmt.Sprintf(`{"status": %d, "description": "Invalid Action %s"}`+"\n", http.StatusBadRequest, vars["action"])
			log.Error(fmt.Sprintf(`admin.DatabasesHandler(): %s %s`, msg, vars))
			http.Error(w, msg, http.StatusBadRequest)
		}
	case `POST`:
		var i instances.Instance
		decoder := json.NewDecoder(request.Body)
		err := decoder.Decode(&i)
		if err != nil {
			msg := fmt.Sprintf(`{"status": %d, "description": "%s"}`+"\n", http.StatusInternalServerError, err)
			log.Error(fmt.Sprintf(`admin.DatabasesHandler(): decoder.Decode() %s %s ! %s`, msg, vars, err))
			http.Error(w, msg, http.StatusInternalServerError)
			return
		}
		switch vars[`action`] {
		case `register`:
			err = i.Register()
			if err != nil {
				msg := fmt.Sprintf(`{"status": %d, "description": "%s"}`+"\n", http.StatusInternalServerError, err)
				log.Error(fmt.Sprintf(`admin.DatabasesHandler(): Instance#Register() %s %s ! %s`, msg, vars, err))
				http.Error(w, msg, http.StatusInternalServerError)
				return
			} else {
				w.Header().Set(`Content-Type`, `application/json; charset=UTF-8`)
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(`{}`))
				return
			}
		default:
			msg := fmt.Sprintf(`{"status": %d, "description": "Invalid Action %s"}`+"\n", http.StatusBadRequest, vars[`action`])
			log.Error(msg)
			http.Error(w, msg, http.StatusBadRequest)
		}
	case `PUT`:
		switch vars[`action`] {
		case `assign`: // updates an existing record.
			// PUT /databases/assign/database
			var i instances.Instance
			decoder := json.NewDecoder(request.Body)
			err := decoder.Decode(&i)
			if err != nil {
				msg := fmt.Sprintf(`{"status": %d, "description": "%s"}`+"\n", http.StatusInternalServerError, err)
				log.Error(fmt.Sprintf(`admin.DatabasesHandler(): decoder.Decode() assign %s %s ! %s`, msg, vars, err))
				http.Error(w, msg, http.StatusInternalServerError)
				return
			}
			err = i.Assign()
			if err != nil {
				msg := fmt.Sprintf(`{"status": %d, "description": "%s"}`+"\n", http.StatusInternalServerError, err)
				log.Error(fmt.Sprintf(`admin.DatabasesHandler(): instances.Assign() %s %s ! %s`, msg, vars, err))
				http.Error(w, msg, http.StatusInternalServerError)
				return
			} else {
				w.Header().Set(`Content-Type`, `application/json; charset=UTF-8`)
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(`{}`))
				return
			}
		case `decommissioned`: // updates an existing record to show it was deprovisioned.
			// PUT /databases/decommissioned
			// This is requested from service cluster to master cluster
			type decomm struct {
				Database  string `json:"database"`
				Timestamp string `json:"timestamp"`
			}
			dc := decomm{}
			decoder := json.NewDecoder(request.Body)
			err := decoder.Decode(&dc)
			if err != nil {
				msg := fmt.Sprintf(`{"status": %d, "description": "%s"}`+"\n", http.StatusInternalServerError, err)
				log.Error(fmt.Sprintf(`admin.DatabasesHandler(): decoder.Decode() decommissioned %s %s ! %s`, msg, vars, err))
				http.Error(w, msg, http.StatusInternalServerError)
				return
			}

			if len(dc.Timestamp) < 1 {
				err = errors.New(`Timestamp query parameter assignment is required!`)
				msg := fmt.Sprintf(`{"status": %d, "description": "%s"}`+"\n", http.StatusInternalServerError, err)
				log.Error(fmt.Sprintf(`admin.DatabasesHandler(): decommissioned %s ! %s`, msg, err))
				http.Error(w, msg, http.StatusInternalServerError)
				return
			}

			i, err := instances.FindByDatabase(dc.Database)
			if err != nil {
				msg := fmt.Sprintf(`{"status": %d, "description": "%s"}`+"\n", http.StatusInternalServerError, err)
				log.Error(fmt.Sprintf(`admin.DatabasesHandler(): instances.FindByDatabase(%s) %s ! %s`, dc.Database, msg, err))
				http.Error(w, msg, http.StatusInternalServerError)
				return
			}

			err = i.DecommissionedAt(dc.Timestamp)
			if err != nil {
				msg := fmt.Sprintf(`{"status": %d, "description": "%s"}"`+"\n", http.StatusInternalServerError, err)
				log.Error(fmt.Sprintf(`admin.DatabasesHandler(): instances.DecommissionedAt() %s %s ! %s`, msg, vars, err))
				http.Error(w, msg, http.StatusInternalServerError)
				return
			} else {
				w.Header().Set(`Content-Type`, `application/json; charset=UTF-8`)
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(`{}`))
				return
			}
		default:
			msg := fmt.Sprintf(`{"status": %d, "description": "Invalid Action %s"}`+"\n", http.StatusBadRequest, vars[`action`])
			log.Error(fmt.Sprintf(`admin.DatabasesHandler(): %s %s`, msg, vars))
			http.Error(w, msg, http.StatusBadRequest)
		}
	case `DELETE`:
		switch vars[`action`] {
		case `decommission`:
			i, err := instances.FindByDatabase(vars[`database`])
			if err != nil {
				msg := fmt.Sprintf(`{"status": %d, "description": "%s"}`+"\n", http.StatusInternalServerError, err)
				log.Error(fmt.Sprintf(`admin.DatabasesHandler(): instance.FindByDatabase() %s %s ! %s`, msg, vars, err))
				http.Error(w, msg, http.StatusInternalServerError)
				return
			} else {
				err = i.Decommission()
				if err != nil {
					msg := fmt.Sprintf(`{"status": %d, "description": "There was an error decommissioning the database (%s)"}`+"\n", http.StatusInternalServerError, err)
					log.Error(fmt.Sprintf(`admin.DatabasesHandler(): instance#Decommission() %s %s ! %s`, msg, vars, err))
					http.Error(w, msg, http.StatusInternalServerError)
				}
			}
		default:
			msg := fmt.Sprintf(`{"status": %d, "description": "Invalid Action %s"}`+"\n", http.StatusBadRequest, vars[`action`])
			log.Error(fmt.Sprintf(`admin.DatabasesHandler(): %s %s`, msg, vars))
			http.Error(w, msg, http.StatusBadRequest)
		}
	default:
		msg := fmt.Sprintf(`{"status": %d, "description": "Method not allowed %s"}`+"\n", http.StatusMethodNotAllowed, request.Method)
		log.Error(fmt.Sprintf(`admin.DatabasesHandler(): %s %s`, msg, vars))
		http.Error(w, msg, http.StatusMethodNotAllowed)
		return
	}
}
