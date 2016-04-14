package admin

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/gorilla/mux"
	"github.com/starkandwayne/rdpgd/log"
)

var (
	adminPort, adminUser, adminPass string
	pgPort, pbPort, pgPass          string
)

type Admin struct {
}

func init() {
	adminPort = os.Getenv(`RDPGD_ADMIN_PORT`)
	if adminPort == "" {
		adminPort = `58888`
	}
	adminUser = os.Getenv(`RDPGD_ADMIN_USER`)
	if adminUser == "" {
		adminUser = `rdpg`
	}
	adminPass = os.Getenv(`RDPGD_ADMIN_PASS`)
	if adminPass == "" {
		adminPass = `admin`
	}
	pgPort = os.Getenv(`RDPGD_PG_PORT`)
	if pgPort == `` {
		pgPort = `5432`
	}
	pbPort = os.Getenv(`RDPGD_PB_PORT`)
	if pbPort == `` {
		pbPort = `5432`
	}
	pgPass = os.Getenv(`RDPGD_PG_PASS`)
	if pgPass == `` {
		pgPass = `admin`
	}
}

type ResponseObject struct {
	Status      int    `json:"status"`
	Description string `json:"description"`
}

func API() (err error) {
	AdminMux := http.NewServeMux()
	router := mux.NewRouter()

	statsHandler := NewStatsHandler(&AgentStats{})

	router.HandleFunc(`/health/{check}`, httpAuth(HealthHandler))
	router.HandleFunc(`/services/{service}/{action}`, httpAuth(ServiceHandler))
	router.HandleFunc(`/databases`, httpAuth(DatabasesHandler))
	router.HandleFunc(`/stats`, httpAuth(statsHandler.ServeHTTP))
	router.HandleFunc(`/stats/locks/{database}`, httpAuth(LocksHandler))
	router.HandleFunc(`/databases/{action}`, httpAuth(DatabasesHandler))
	router.HandleFunc(`/databases/{action}/{database}`, httpAuth(DatabasesHandler))
	router.HandleFunc(`/clusters/{clusterid}/capacity/instances/allowed/{value}`, httpAuth(CapacityHandler))
	router.HandleFunc(`/clusters/{clusterid}/capacity/instances`, httpAuth(CapacityHandler))
	router.HandleFunc(`/env/{key}`, httpAuth(EnvHandler))
	router.HandleFunc(`/backup/{how:(now|enqueue)}`, httpAuth(BackupHandler)).Methods("POST")
	router.HandleFunc(`/backup/list`, httpAuth(BackupListAllHandler)).Methods("GET")
	router.HandleFunc(`/backup/list/{where:(local|remote)}`, httpAuth(BackupListHandler)).Methods("GET")
	router.HandleFunc(`/backup/only/{where:(local|remote|diff|both)}`, httpAuth(BackupDiffHandler)).Methods("GET")
	router.HandleFunc(`/backup/retention/custom`, httpAuth(CustomRetentionRulesHandler)).Methods("GET")
	router.HandleFunc(`/backup/retention/policy/{where:(local|remote)}`, httpAuth(RetentionPolicyHandler)).Methods("GET", "PUT")
	router.HandleFunc(`/backup/remote/copyto`, httpAuth(RemoteCopyHandler)).Methods("PUT")
	router.HandleFunc(`/restore/inplace`, httpAuth(RestoreInPlaceHandler)).Methods("POST")

	AdminMux.Handle("/", router)
	err = http.ListenAndServe(":"+adminPort, AdminMux)
	log.Error(fmt.Sprintf(`admin.API() ! %s`, err))
	return
}

func httpAuth(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, request *http.Request) {
		if len(request.Header[`Authorization`]) == 0 {
			log.Trace(fmt.Sprintf(`httpAuth(): Authorization Required`))
			http.Error(w, `Authorization Required`, http.StatusUnauthorized)
			return
		}

		auth := strings.SplitN(request.Header[`Authorization`][0], " ", 2)
		if len(auth) != 2 || auth[0] != `Basic` {
			log.Error(fmt.Sprintf(`httpAuth(): Unhandled Authorization Type, Expected Basic`))
			http.Error(w, "Unhandled Authroization Type, Expected Basic\n", http.StatusBadRequest)
			return
		}
		payload, err := base64.StdEncoding.DecodeString(auth[1])
		if err != nil {
			log.Error(fmt.Sprintf(`httpAuth(): Authorization Failed (Decoding)`))
			http.Error(w, "Authorization Failed (Decoding)\n", http.StatusUnauthorized)
			return
		}
		nv := strings.SplitN(string(payload), ":", 2)
		if (len(nv) != 2) || !isAuthorized(nv[0], nv[1]) {
			log.Error(fmt.Sprintf(`httpAuth(): Authorization Failed isAuthorized()`))
			http.Error(w, "Authorization Failed\n", http.StatusUnauthorized)
			return
		}
		h(w, request)
	}
}

func isAuthorized(username, password string) bool {
	if username == adminUser && password == adminPass {
		return true
	}
	return false
}
