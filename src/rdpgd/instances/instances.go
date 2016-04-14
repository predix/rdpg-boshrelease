package instances

import (
	"fmt"
	"net"
	"os"
	"strings"

	consulapi "github.com/hashicorp/consul/api"

	"github.com/starkandwayne/rdpgd/log"
)

var (
	pbPort       string
	pgPass       string
	ClusterID    string
	MatrixName   string
	MatrixColumn string
)

var MatrixNameSplit []string

type Instance struct {
	ID             string `db:"id"`
	ClusterID      string `db:"cluster_id" json:"cluster_id"`
	ClusterService string `db:"cluster_service" json:"cluster_service"`
	InstanceID     string `db:"instance_id" json:"instance_id"`
	ServiceID      string `db:"service_id" json:"service_id"`
	PlanID         string `db:"plan_id" json:"plan_id"`
	OrganizationID string `db:"organization_id" json:"organization_id"`
	SpaceID        string `db:"space_id" json:"space_id"`
	Database       string `db:"dbname" json:"dbname"`
	User           string `db:"dbuser" json:"uname"`
	Pass           string `db:"dbpass" json:"pass"`
	lock           *consulapi.Lock
	lockCh         <-chan struct{}
}

func init() {
	MatrixName = os.Getenv(`RDPGD_MATRIX`)
	MatrixNameSplit = strings.SplitAfterN(MatrixName, `-`, -1)
	MatrixColumn = os.Getenv(`RDPGD_MATRIX_COLUMN`)
	ClusterID = os.Getenv(`RDPGD_CLUSTER`)
	if ClusterID == "" {
		for i := 0; i < len(MatrixNameSplit)-1; i++ {
			ClusterID = ClusterID + MatrixNameSplit[i]
		}
		ClusterID = ClusterID + "c" + MatrixColumn
	}

	if ClusterID == "" {
		log.Error(`instance.init() RDPGD_CLUSTER not found in environment!!!`)
	}
	pbPort = os.Getenv(`RDPGD_PB_PORT`)
	if pbPort == `` {
		pbPort = `6432`
	}
	pgPass = os.Getenv(`RDPGD_PG_PASS`)
}

func (i *Instance) ExternalDNS() (dns string, err error) {
	client, err := consulapi.NewClient(consulapi.DefaultConfig())
	if err != nil {
		log.Error(fmt.Sprintf(`instances.Instance#ExternalDNS(%s) i.ClusterIPs() ! %s`, i.InstanceID, err))
		return
	}
	catalog := client.Catalog()

	services, _, err := catalog.Service(fmt.Sprintf(`%s-master`, i.ClusterID), "", nil)
	if err != nil {
		log.Error(fmt.Sprintf("instances.Instance#ExternalDNS(%s) consulapi.Catalog().Service() ! %s", i.ClusterID, err))
		return
	}
	if len(services) == 0 {
		// Master is missing, use the first service node available...
		log.Error(fmt.Sprintf("instances.Instance#ExternalDNS(%s) ! Master service node not found via Consul...?!", i.ClusterID))
		return
	}

	HostValue := os.Getenv(`PGBDR_DSN_HOST`)

	if HostValue == "manifestIP" {
		log.Trace(fmt.Sprintf(`Detected manifestIP value of PGBDR_DSN_HOST = %s`, HostValue))
		//Query Consul and use deployed manifest IP
		masterIP := services[0].Address
		dns = fmt.Sprintf(`%s:5432`, masterIP)
	} else if HostValue == "consulDNS" {
		log.Trace(fmt.Sprintf(`Detected consulDNS value of PGBDR_DSN_HOST = %s`, HostValue))
		//Query Consul and use deployed servicename and append remaining values for a  unique FQDN
		masterIP := services[0].ServiceName + `.service.` + os.Getenv(`DATACENTER`) + `.consul`
		dns = fmt.Sprintf(`%s:5432`, masterIP)
	} else if net.ParseIP(HostValue) != nil {
		log.Trace(fmt.Sprintf(`Detected IP Address for PGBDR_DSN_HOST = %s`, HostValue))
		masterIP := HostValue
		dns = fmt.Sprintf(`%s:5432`, masterIP)
	} else if HostValue != "" {
		log.Trace(fmt.Sprintf(`Detected manual DNS Name for PGBDR_DSN_HOST = %s`, HostValue))
		masterIP := HostValue
		dns = fmt.Sprintf(`%s:5432`, masterIP)
	} else {
		log.Trace(fmt.Sprintf(`Defaulting to manifestIP`))
		//Query Consul and use deployed manifest IP
		masterIP := services[0].Address
		dns = fmt.Sprintf(`%s:5432`, masterIP)
	}
	return
}

func (i *Instance) URI() (uri string, err error) {
	dns, err := i.ExternalDNS()
	if err != nil {
		log.Error(fmt.Sprintf("instances.Instance#URI(%s) ! %s", i.ClusterID))
		return
	}
	d := `postgres://%s:%s@%s/%s?sslmode=%s`
	uri = fmt.Sprintf(d, i.User, i.Pass, dns, i.Database, `disable`)
	return
}

func (i *Instance) DSN() (uri string, err error) {
	dns, err := i.ExternalDNS()
	if err != nil {
		log.Error(fmt.Sprintf("instances.Instance#DSN(%s) ! %s", i.ClusterID))
		return
	}
	s := strings.Split(dns, ":")
	d := `host=%s port=%s user=%s password=%s dbname=%s connect_timeout=%s sslmode=%s`
	uri = fmt.Sprintf(d, s[0], s[1], i.User, i.Pass, i.Database, `5`, `disable`)
	return
}

func (i *Instance) JDBCURI() (uri string, err error) {
	dns, err := i.ExternalDNS()
	if err != nil {
		log.Error(fmt.Sprintf("instances.Instance#JDBCURI(%s) ! %s", i.ClusterID))
		return
	}
	d := `jdbc:postgres://%s:%s@%s/%s?sslmode=%s`
	uri = fmt.Sprintf(d, i.User, i.Pass, dns, i.Database, `disable`)
	return
}

// Lock the instance within the current cluster via Consul.
func (i *Instance) Lock() (err error) {
	key := fmt.Sprintf("rdpg/%s/database/%s/lock", i.ClusterID, i.Database)
	client, _ := consulapi.NewClient(consulapi.DefaultConfig())
	i.lock, err = client.LockKey(key)
	if err != nil {
		log.Error(fmt.Sprintf("scheduler.Schedule() Error Locking Scheduler Key %s ! %s", key, err))
		return
	}
	i.lockCh, err = i.lock.Lock(nil)
	if err != nil {
		log.Error(fmt.Sprintf("scheduler.Lock() Error aquiring instance Key lock %s ! %s", key, err))
		return
	}
	if i.lockCh == nil {
		err = fmt.Errorf(`Scheduler Lock not aquired.`)
	}
	return
}

func (i *Instance) Unlock() (err error) {
	if i.lock != nil {
		err = i.lock.Unlock()
	}
	return
}
