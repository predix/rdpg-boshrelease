package admin

import (
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/starkandwayne/rdpgd/log"
	"github.com/starkandwayne/rdpgd/services"
)

/*
POST /services/{service}/{action}
*/
func ServiceHandler(w http.ResponseWriter, request *http.Request) {
	vars := mux.Vars(request)
	log.Trace(fmt.Sprintf(`%s /services/%s/%s`, request.Method, vars[`service`], vars[`action`]))
	switch request.Method {
	case `PUT`:
		service, err := services.NewService(vars[`service`])
		if err != nil {
			msg := fmt.Sprintf(`{"status": %d, "description": "%s"}`+"\n", http.StatusInternalServerError, err)
			log.Error(fmt.Sprintf(`admin.ServiceHandler(): NewService(%s) %+v %s`, service, vars, msg))
			http.Error(w, msg, http.StatusInternalServerError)
			return
		}

		switch vars[`action`] {
		case `configure`:
			err := service.Configure()
			if err != nil {
				msg := fmt.Sprintf(`{"status": %d, "description": "%s"}`+"\n", http.StatusInternalServerError, err)
				log.Error(fmt.Sprintf(`admin.ServiceHandler(): NewService(%s) %+v %s`, service, vars, msg))
				http.Error(w, msg, http.StatusInternalServerError)
			}
			msg := fmt.Sprintf(`{"status": %d, "description": "%s %s"}`+"\n", http.StatusOK, vars[`service`], vars[`action`])
			log.Trace(fmt.Sprintf(`admin.ServiceHandler(): NewService(%s) %s`, service, msg))
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, msg)
		default:
			msg := fmt.Sprintf(`{"status": %d, "description": "Invalid Action %s for %s"}`+"\n", http.StatusBadRequest, vars[`action`], vars[`service`])
			log.Error(fmt.Sprintf(`admin.ServiceHandler(): NewService(%s) %s`, service, msg))
			http.Error(w, msg, http.StatusBadRequest)
		}
	default:
		msg := fmt.Sprintf(`{"status": %d, "description": "Method not allowed %s"}`+"\n", http.StatusMethodNotAllowed, request.Method)
		log.Error(fmt.Sprintf(`admin.ServiceHandler(): NewService() %+v %s`, vars, msg))
		http.Error(w, msg, http.StatusMethodNotAllowed)
	}
}
