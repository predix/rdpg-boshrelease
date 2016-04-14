package admin

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strconv"

	"github.com/gorilla/mux"
	consulapi "github.com/hashicorp/consul/api"
	"github.com/starkandwayne/rdpgd/log"
)

type ClusterCapacity struct {
	ClusterID        string `db:"cluster_id" json:"cluster_id"`
	InstancesAllowed int    `json:"instances_allowed"`
	InstancesLimit   int    `json:"instances_limit"`
}

func CapacityHandler(w http.ResponseWriter, request *http.Request) {
	vars := mux.Vars(request)
	clusterIDRe := regexp.MustCompile(`^sc-([[:alnum:]|-])*m[0-9]+-c[0-9]+$`)

	client, err := consulapi.NewClient(consulapi.DefaultConfig())
	if err != nil {
		log.Error(fmt.Sprintf("admin.cluster#CapacityHandler(): consulapi.NewClient() ! %s", err))
		return
	}
	kv := client.KV()
	keyAllowed := fmt.Sprintf("rdpg/%s/capacity/instances/allowed", vars[`clusterid`])
	keyLimit := fmt.Sprintf("rdpg/%s/capacity/instances/limit", vars[`clusterid`])

	clusterID := vars[`clusterid`]

	if clusterIDRe.MatchString(clusterID) {
		switch request.Method {
		case `GET`:
			log.Trace(fmt.Sprintf(`%s /clusters/%s/capacity/instances/`, request.Method, vars[`clusterid`]))
			kvpAllowed, _, err := kv.Get(keyAllowed, nil)
			if err != nil {
				msg := fmt.Sprintf(`{"status": %d, "description": "%s"}`, http.StatusInternalServerError, err)
				log.Error(fmt.Sprintf(`admin.CapacityHandler():get allowed instances number KVP %s %+v ! %s`, msg, vars, err))
				http.Error(w, msg, http.StatusInternalServerError)
				return
			}
			instancesAllowedString := string(kvpAllowed.Value)
			instancesAllowed, err := strconv.Atoi(instancesAllowedString)
			if err != nil {
				log.Error(fmt.Sprintf("admin.CapacityHandler() : get allowed instances number for %s! %s", vars[`clusterid`], err))
			}

			kvpLimit, _, err := kv.Get(keyLimit, nil)
			if err != nil {
				msg := fmt.Sprintf(`{"status": %d, "description": "%s"}`, http.StatusInternalServerError, err)
				log.Error(fmt.Sprintf(`admin.CapacityHandler():get limit instances number KVP %s %+v ! %s`, msg, vars, err))
				http.Error(w, msg, http.StatusInternalServerError)
				return
			}
			instancesLimitString := string(kvpLimit.Value)
			instancesLimit, err := strconv.Atoi(instancesLimitString)
			if err != nil {
				log.Error(fmt.Sprintf("admin.CapacityHandler() : get limit instances number for %s! %s", vars[`clusterid`], err))
			}

			clusterCapacity := ClusterCapacity{
				ClusterID:        vars[`clusterid`],
				InstancesAllowed: instancesAllowed,
				InstancesLimit:   instancesLimit,
			}

			jsonClusterCapacity, err := json.Marshal(clusterCapacity)
			if err != nil {
				msg := fmt.Sprintf(`{"status": %d, "description": "%s"}`, http.StatusInternalServerError, err)
				log.Error(fmt.Sprintf(`admin.CapacityHandler(): json.Marshal(clusterCapacity %s %+v ! %s`, msg, vars, err))
				http.Error(w, msg, http.StatusInternalServerError)
			} else {
				w.Header().Set("Content-Type", "application/json; charset=UTF-8")
				w.WriteHeader(http.StatusOK)
				w.Write(jsonClusterCapacity)
			}
		case `PUT`:
			if vars[`value`] == "" {
				msg := fmt.Sprintf(`{"status": %d, "description": "Invalid value %s, need to be an integer more than the original value."}`, http.StatusBadRequest, vars[`value`])
				log.Error(fmt.Sprintf(`admin.CapacityHandler(): Set New Capacity %s`, msg))
				http.Error(w, msg, http.StatusBadRequest)
			} else {
				log.Trace(fmt.Sprintf(`%s /clusters/%s/capacity/instances/allowed/%s`, request.Method, vars[`clusterid`], vars[`value`]))
				kvpAllowed, _, err := kv.Get(keyAllowed, nil)
				if err != nil {
					msg := fmt.Sprintf(`{"status": %d, "description": "%s"}`, http.StatusInternalServerError, err)
					log.Error(fmt.Sprintf(`admin.CapacityHandler():get allowed instances number KVP %s %+v ! %s`, msg, vars, err))
					http.Error(w, msg, http.StatusInternalServerError)
					return
				}
				kvpAllowed.Value = []byte(vars[`value`])
				_, err = kv.Put(kvpAllowed, &consulapi.WriteOptions{})
				if err != nil {
					msg := fmt.Sprintf(`{"status": %d, "description": "%s"}`, http.StatusInternalServerError, err)
					log.Error(fmt.Sprintf(`admin.CapacityHandler(): put allowed instances number KVP %s %+v ! %s`, msg, vars, err))
					http.Error(w, msg, http.StatusInternalServerError)
					return
				} else {
					w.Header().Set("Content-Type", "application/json; charset=UTF-8")
					w.WriteHeader(http.StatusOK)
				}
			}
		default:
			msg := fmt.Sprintf(`{"status": %d, "description": "Method not allowed %s"}`, http.StatusMethodNotAllowed, request.Method)
			log.Error(fmt.Sprintf(`admin.CapacityHandler() %+v %s`, vars, msg))
			http.Error(w, msg, http.StatusMethodNotAllowed)
		}
	} else {
		msg := fmt.Sprintf(`{"status": %d, "description": "Invalid ClusterID %s."}`, http.StatusBadRequest, vars[`clusterid`])
		log.Error(fmt.Sprintf(`admin.CapacityHandler(): Get Capacity %s`, msg))
		http.Error(w, msg, http.StatusBadRequest)
	}
}
