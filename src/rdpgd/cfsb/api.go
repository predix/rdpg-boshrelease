package cfsb

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"

	"github.com/gorilla/mux"
	"github.com/starkandwayne/rdpgd/instances"
	"github.com/starkandwayne/rdpgd/log"
)

var (
	sbPort, sbUser, sbPass string
	pgPort, pbPort, pgPass string
)

//CFSB used for service broker
type CFSB struct {
}

//Response struct
type Response struct {
	Status      int
	Description string
}

func init() {
	sbPort = os.Getenv("RDPGD_SB_PORT")
	if sbPort == "" {
		sbPort = "8888"
	}
	sbUser = os.Getenv("RDPGD_SB_USER")
	if sbUser == "" {
		sbUser = "cfadmin"
	}
	sbPass = os.Getenv("RDPGD_SB_PASS")
	if sbPass == "" {
		sbPass = "cfadmin"
	}
	pbPort = os.Getenv(`RDPGD_PB_PORT`)
	if pbPort == `` {
		pbPort = `6432`
	}
	pgPass = os.Getenv(`RDPGD_PG_PASS`)
}

//API sets up API server
func API() (err error) {
	CFSBMux := http.NewServeMux()
	router := mux.NewRouter()
	router.HandleFunc("/v2/catalog", httpAuth(CatalogHandler))
	router.HandleFunc("/v2/service_instances/{instance_id}", httpAuth(InstanceHandler))
	CFSBMux.Handle("/", router)
	router.HandleFunc("/v2/service_instances/{instance_id}/service_bindings/{binding_id}", httpAuth(BindingHandler))

	http.Handle("/", router)
	err = http.ListenAndServe(":"+sbPort, CFSBMux)
	log.Error(fmt.Sprintf(`cfsbapi.API() ! %s`, err))
	return err
}

func httpAuth(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, request *http.Request) {
		if len(request.Header["Authorization"]) == 0 {
			log.Trace(fmt.Sprintf("httpAuth(): 'Authorization' Header Required"))
			http.Error(w, "Authorization Required", http.StatusUnauthorized)
			return
		}

		auth := strings.SplitN(request.Header["Authorization"][0], " ", 2)
		if len(auth) != 2 || auth[0] != "Basic" {
			log.Error(fmt.Sprintf("httpAuth(): Unhandled Authorization Type, Expected Basic"))
			http.Error(w, "Unhandled Authorization Type, Expected Basic\n", http.StatusBadRequest)
			return
		}
		payload, err := base64.StdEncoding.DecodeString(auth[1])
		if err != nil {
			log.Error(fmt.Sprintf("httpAuth(): Authorization base64.StdEncoding.DecodeString() Failed"))
			http.Error(w, "Authorization Failed\n", http.StatusUnauthorized)
			return
		}
		nv := strings.SplitN(string(payload), ":", 2)
		if (len(nv) != 2) || !isAuthorized(nv[0], nv[1]) {
			log.Error(fmt.Sprintf("httpAuth(): Authorization Failed: !isAuthorized() nv: %+v", nv))
			http.Error(w, "Authorization Failed\n", http.StatusUnauthorized)
			return
		}
		h(w, request)
	}
}

func isAuthorized(username, password string) bool {
	if username == sbUser && password == sbPass {
		return true
	}
	return false
}

// CatalogHandler GET /v2/catalog
func CatalogHandler(w http.ResponseWriter, request *http.Request) {
	log.Trace(fmt.Sprintf("%s /v2/catalog", request.Method))
	switch request.Method {
	case "GET":
		c := Catalog{}
		err := c.Fetch()
		if err != nil {
			writeJSONResponse(w, http.StatusInternalServerError, err.Error())
			return
		}
		jsonCatalog, err := json.Marshal(c)
		if err != nil {
			writeJSONResponse(w, http.StatusInternalServerError, err.Error())
			return
		}
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.WriteHeader(http.StatusOK)
		w.Write(jsonCatalog)

	default:
		writeJSONResponse(w, http.StatusMethodNotAllowed, "Allowed Methods: GET")
	}
	return
}

// InstanceHandler handles put and delete
// (PI) PUT /v2/service_instances/:id
// (RI) DELETE /v2/service_instances/:id
func InstanceHandler(w http.ResponseWriter, request *http.Request) {
	vars := mux.Vars(request)
	log.Trace(fmt.Sprintf("%s /v2/service_instances/:instance_id :: %+v", request.Method, vars))
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")

	switch request.Method {
	case "PUT":
		type instanceRequest struct {
			ServiceID      string `json:"service_id"`
			Plan           string `json:"plan_id"`
			OrganizationID string `json:"organization_guid"`
			SpaceID        string `json:"space_guid"`
		}
		ir := instanceRequest{}
		body, err := ioutil.ReadAll(request.Body)
		if err != nil {
			log.Error(fmt.Sprintf("%s /v2/service_instances/:instance_id %s", request.Method, err))
			writeJSONResponse(w, http.StatusInternalServerError, err.Error())
			return
		}
		err = json.Unmarshal(body, &ir)
		if err != nil {
			log.Error(fmt.Sprintf("%s /v2/service_instances/:instance_id ! %s", request.Method, err))
			writeJSONResponse(w, http.StatusInternalServerError, err.Error())
			return
		}
		instance, err := NewServiceInstance(
			vars["instance_id"],
			ir.ServiceID,
			ir.Plan,
			ir.OrganizationID,
			ir.SpaceID,
		)
		if err != nil {
			log.Error(fmt.Sprintf("%s /v2/service_instances/:instance_id ! %s", request.Method, err))
			writeJSONResponse(w, http.StatusInternalServerError, err.Error())
			return
		}
		err = instance.Provision()
		if err != nil {
			log.Error(fmt.Sprintf("%s /v2/service_instances/:instance_id ! %s", request.Method, err))
			writeJSONResponse(w, http.StatusInternalServerError, err.Error())
			return
		}

		msg := fmt.Sprintf("Provisioned Instance %s", instance.InstanceID)
		writeJSONResponse(w, http.StatusOK, msg)
		return
	case "DELETE":
		instance, err := instances.FindByInstanceID(vars["instance_id"])
		if err != nil {
			log.Error(fmt.Sprintf("%s /v2/service_instances/:instance_id ! %s", request.Method, err))
			msg := fmt.Sprintf("Could not find instance %s, perhaps it was already deleted?", vars["instance_id"])
			writeJSONResponse(w, http.StatusInternalServerError, msg)
			return
		}
		err = instance.Decommission()
		if err != nil {
			log.Error(fmt.Sprintf("%s /v2/service_instances/:instance_id %s", request.Method, err))
			writeJSONResponse(w, http.StatusInternalServerError, "There was an error decommissioning instance "+instance.InstanceID)
			return
		}
		writeJSONResponse(w, http.StatusOK, "Successfully Deprovisioned Instance "+instance.InstanceID)
	default:
		writeJSONResponse(w, http.StatusMethodNotAllowed, "Allowed Methods: PUT, DELETE")
	}
}

// BindingHandler handles binding services
// (CB) PUT /v2/service_instances/:instance_id/service_bindings/:binding_id
// (RB) DELETE /v2/service_instances/:instance_id/service_bindings/:binding_id

func BindingHandler(w http.ResponseWriter, request *http.Request) {
	vars := mux.Vars(request)
	log.Trace(fmt.Sprintf("%s /v2/service_instances/:instance_id/service_bindings/:binding_id :: %+v", request.Method, vars))
	switch request.Method {
	case "PUT":
		binding := Binding{InstanceID: vars["instance_id"], BindingID: vars["binding_id"]}
		err := binding.Create()
		if err != nil {
			log.Error(fmt.Sprintf("%s /v2/service_instances/:instance_id/service_bindings/:binding_id %s", request.Method, err))
			writeJSONResponse(w, http.StatusInternalServerError, err.Error())
			return
		}
		j, err := json.Marshal(binding)
		if err != nil {
			log.Error(fmt.Sprintf("%s /v2/service_instances/:instance_id/service_bindings/:binding_id %s", request.Method, err))
			writeJSONResponse(w, http.StatusInternalServerError, err.Error())
			return
		}
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, string(j))
		return

	case "DELETE":
		binding := Binding{BindingID: vars["binding_id"]}
		err := binding.Remove()
		if err != nil {
			log.Error(fmt.Sprintf("%s /v2/service_instances/:instance_id/service_bindings/:binding_id %s", request.Method, err))
			msg := "Binding does not exist or has already been deleted."
			writeJSONResponse(w, http.StatusInternalServerError, msg)
			return
		} else {
			writeJSONResponse(w, http.StatusOK, "Binding Removed")
			return
		}
	default:
		writeJSONResponse(w, http.StatusMethodNotAllowed, "Allowed Methods: PUT, DELETE")
		return
	}
}

func writeJSONResponse(writer http.ResponseWriter, status int, description string) {
	resp := Response{Status: status, Description: description}
	msg, err := json.Marshal(resp)
	if err != nil {
		log.Error(err.Error())
	}
	log.Debug("Returning response: " + string(msg))
	writer.WriteHeader(status)
	http.Error(writer, string(msg), status)
}
