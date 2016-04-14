package tasks

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"regexp"

	consulapi "github.com/hashicorp/consul/api"
	"github.com/starkandwayne/rdpgd/instances"
	"github.com/starkandwayne/rdpgd/log"
	"github.com/starkandwayne/rdpgd/pg"
)

func (t *Task) ReconcileAvailableDatabases() (err error) {
	log.Trace(fmt.Sprintf(`tasks.ReconcileAvailableDatabases(%s)...`, t.Data))
	client, err := consulapi.NewClient(consulapi.DefaultConfig())
	if err != nil {
		log.Error(fmt.Sprintf("rdpg.newRDPG() consulapi.NewClient()! %s", err))
		return
	}
	catalog := client.Catalog()
	svcs, _, err := catalog.Services(nil)
	if err != nil {
		log.Error(fmt.Sprintf("tasks.Task#ReconcileAvailableDatabases() catalog.Service() ! %s", err))
		return err
	}
	clusterInstances := []instances.Instance{}
	re := regexp.MustCompile(`^(rdpg(sc[0-9]+$))|(sc-([[:alnum:]|-])*m[0-9]+-c[0-9]+$)`)
	for key, _ := range svcs {
		if re.MatchString(key) {
			// Fetch list of available databases for each service cluster
			svcs, _, err := catalog.Service(key, "", nil)
			if err != nil {
				log.Error(fmt.Sprintf("tasks.Task#ReconcileAvailableDatabases() catalog.Service() ! %s", err))
				return err
			}
			log.Trace(fmt.Sprintf("tasks.Task#ReconcileAvailableDatabases() svcs: %+v", svcs))
			if len(svcs) == 0 {
				log.Error("tasks.Task#ReconcileAvailableDatabases() ! No services found, no known nodes?!")
				return err
			}
			url := fmt.Sprintf("http://%s:%s/%s", svcs[0].Address, os.Getenv("RDPGD_ADMIN_PORT"), `databases/available`)
			req, err := http.NewRequest("GET", url, bytes.NewBuffer([]byte("{}")))
			log.Trace(fmt.Sprintf(`tasks.Task#ReconcileAvailableDatabases() > POST %s`, url))
			//req.Header.Set("Content-Type", "application/json")
			// TODO: Retrieve from configuration in database.
			req.SetBasicAuth(os.Getenv("RDPGD_ADMIN_USER"), os.Getenv("RDPGD_ADMIN_PASS"))
			httpClient := &http.Client{}
			resp, err := httpClient.Do(req)
			if err != nil {
				log.Error(fmt.Sprintf(`tasks.Task#ReconcileAvailableDatabases() httpClient.Do() %s ! %s`, url, err))
				return err
			}
			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				log.Error(fmt.Sprintf("tasks.Task#ReconcileAvailableDatabases() ! %s", err))
				continue
			}
			is := []instances.Instance{}
			err = json.Unmarshal(body, &is)
			if err != nil {
				log.Error(fmt.Sprintf("tasks.Task#ReconcileAvailableDatabases() ! %s json: %s", err, string(body)))
				continue
			}
			clusterInstances = append(clusterInstances, is...)
		}
	}
	p := pg.NewPG(`127.0.0.1`, pbPort, `rdpg`, `rdpg`, pgPass)
	db, err := p.Connect()
	if err != nil {
		log.Error(fmt.Sprintf("tasks.Task#ReconcileAvailableDatabases() Failed connecting to %s err: %s", p.URI, err))
		return err
	}
	defer db.Close()

	for index, _ := range clusterInstances {
		i, err := instances.FindByDatabase(clusterInstances[index].Database)
		if err != nil {
			log.Error(fmt.Sprintf("tasks.Task#ReconcileAvailableDatabases() Failed connecting to %s err: %s", p.URI, err))
			return err
		}
		if i == nil {
			i = &clusterInstances[index]
			log.Trace(fmt.Sprintf(`tasks.Task#ReconcileAvailableDatabases() Reconciling database %s for cluster %s`, i.Database, i.ClusterID))
			i.Register()
		} else {
			continue
		}
	}
	return
}

func (t *Task) ReconcileAllDatabases() (err error) {
	log.Trace(fmt.Sprintf(`tasks.ReconcileAllDatabases(%s)...`, t.Data))
	client, err := consulapi.NewClient(consulapi.DefaultConfig())
	if err != nil {
		log.Error(fmt.Sprintf("rdpg.newRDPG() consulapi.NewClient()! %s", err))
		return
	}
	catalog := client.Catalog()
	svcs, _, err := catalog.Services(nil)
	if err != nil {
		log.Error(fmt.Sprintf("tasks.Task#ReconcileAllDatabases() catalog.Service() ! %s", err))
		return err
	}
	clusterInstances := []instances.Instance{}
	re := regexp.MustCompile(`^(rdpg(sc[0-9]+$))|(sc-([[:alnum:]|-])*m[0-9]+-c[0-9]+$)`)
	for key, _ := range svcs {
		if re.MatchString(key) {
			// Fetch list of available databases for each service cluster
			svcs, _, err := catalog.Service(key, "", nil)
			if err != nil {
				log.Error(fmt.Sprintf("tasks.Task#ReconcileAllDatabases() catalog.Service() ! %s", err))
				return err
			}
			log.Trace(fmt.Sprintf("tasks.Task#ReconcileAllDatabases() svcs: %+v", svcs))
			if len(svcs) == 0 {
				log.Error("tasks.Task#ReconcileAllDatabases() ! No services found, no known nodes?!")
				return err
			}
			url := fmt.Sprintf("http://%s:%s/%s", svcs[0].Address, os.Getenv("RDPGD_ADMIN_PORT"), `databases`)
			req, err := http.NewRequest("GET", url, bytes.NewBuffer([]byte("{}")))
			log.Trace(fmt.Sprintf(`tasks.Task#ReconcileAllDatabases() > POST %s`, url))
			//req.Header.Set("Content-Type", "application/json")
			// TODO: Retrieve from configuration in database.
			req.SetBasicAuth(os.Getenv("RDPGD_ADMIN_USER"), os.Getenv("RDPGD_ADMIN_PASS"))
			httpClient := &http.Client{}
			resp, err := httpClient.Do(req)
			if err != nil {
				log.Error(fmt.Sprintf(`tasks.Task#ReconcileAllDatabases() httpClient.Do() %s ! %s`, url, err))
				return err
			}
			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				log.Error(fmt.Sprintf("tasks.Task#ReconcileAllDatabases() ! %s", err))
				continue
			}
			is := []instances.Instance{}
			err = json.Unmarshal(body, &is)
			if err != nil {
				log.Error(fmt.Sprintf("tasks.Task#ReconcileAllDatabases() ! %s json: %s", err, string(body)))
				continue
			}
			for _, i := range is {
				clusterInstances = append(clusterInstances, i)
			}
		}
	}
	p := pg.NewPG(`127.0.0.1`, pbPort, `rdpg`, `rdpg`, pgPass)
	db, err := p.Connect()
	if err != nil {
		log.Error(fmt.Sprintf("tasks.Task#ReconcileAllDatabases() Failed connecting to %s err: %s", p.URI, err))
		return err
	}
	defer db.Close()

	for index, _ := range clusterInstances {
		err = clusterInstances[index].Reconcile()
		if err != nil {
			log.Error(fmt.Sprintf("tasks.Task#ReconcileAllDatabases() ! %s", err))
			return err
		}
	}
	return
}
