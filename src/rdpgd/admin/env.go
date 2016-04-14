package admin

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	"github.com/gorilla/mux"
	"github.com/starkandwayne/rdpgd/log"
)

type EnvVar struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func EnvHandler(w http.ResponseWriter, request *http.Request) {
	vars := mux.Vars(request)
	envKey := vars["key"]
	envValue := os.Getenv(envKey)
	log.Trace(fmt.Sprintf(`%s /env/%s => %s`, request.Method, envKey, envValue))
	switch request.Method {
	case `GET`:
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.WriteHeader(http.StatusOK)
		ev := EnvVar{Key: envKey, Value: envValue}
		msg, err := json.Marshal(ev)
		if err != nil {
			log.Error(fmt.Sprintf(`admin.EnvHandler() %s`, err))
		}
		w.Write(msg)
	default:
		resp := ResponseObject{Status: http.StatusMethodNotAllowed, Description: request.Method}
		msg, err := json.Marshal(resp)
		if err != nil {
			log.Error(fmt.Sprintf(`admin.EnvHandler() %s`, err))
		}
		log.Error(fmt.Sprintf(`admin.EnvHandler() %+v %s`, vars, string(msg)))
		http.Error(w, string(msg), http.StatusMethodNotAllowed)
	}
}
