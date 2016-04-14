package services

import (
	"fmt"
	"os"
	"strings"

	consulapi "github.com/hashicorp/consul/api"

	"github.com/starkandwayne/rdpgd/log"
)

/*
Service struct represents a known configurable service within RDPG.
*/
type Service struct {
	Name string `db:"name" json:"name"`
}

var (
	pgPort string
	pbPort string
)

func init() {
	pgPort = os.Getenv("RDPGD_PG_PORT")
	if pgPort == "" {
		pgPort = "5432"
	}
	pbPort = os.Getenv(`RDPGD_PB_PORT`)
	if pbPort == `` {
		pbPort = `6432`
	}
}

/*
NewService is used to create and return a new service of known handled services.
*/
func NewService(name string) (s Service, err error) {
	switch name {
	case "haproxy", "pgbouncer", "postgresql", "pgbdr", "consul":
		s = Service{Name: name}
	default:
	}
	return
}

/*
Configure is a Service struct method which delegates to the appropriate Service
configuration function.
*/
func (s *Service) Configure() (err error) {
	log.Trace(fmt.Sprintf(`services.Service<%s>#Configure()`, s.Name))
	// TODO: Protect each service configuration with a consul lock for the host
	// so that only one may be done at a time and we don't encounter write conflicts.
	switch s.Name {
	case "consul":
		err = s.ConfigureConsul()
	case "haproxy":
		err = s.ConfigureHAProxy()
	case "pgbouncer":
		err = s.ConfigurePGBouncer()
	case "postgresql":
		err = s.ConfigurePostgreSQL()
	case "pgbdr":
		err = s.ConfigurePGBDR()
	default:
		return fmt.Errorf(`services.Service<%s>#Configure() is unknown`, s.Name)
	}
	if err != nil {
		log.Error(fmt.Sprintf("services.Service<%s>#Configure() ! %s", s.Name, err))
		return
	}
	return
}

/*
GetWriteMasterIP returns the write master IP for the service cluster.
*/
func (s *Service) GetWriteMasterIP() (ip string, err error) {
	log.Trace(fmt.Sprintf(`services.Service<%s>#GetWriteMaster()`, s.Name))
	client, err := consulapi.NewClient(consulapi.DefaultConfig())
	if err != nil {
		log.Error(fmt.Sprintf("services.Service<%s>#GetWriteMaster() ! %s", s.Name, err))
		return
	}
	catalog := client.Catalog()

	clusterID := os.Getenv("RDPGD_CLUSTER")
	if clusterID == "" {
		matrixName := os.Getenv(`RDPGD_MATRIX`)
		matrixNameSplit := strings.SplitAfterN(matrixName, `-`, -1)
		matrixColumn := os.Getenv(`RDPGD_MATRIX_COLUMN`)
		for i := 0; i < len(matrixNameSplit)-1; i++ {
			clusterID = clusterID + matrixNameSplit[i]
		}
		clusterID = clusterID + "c" + matrixColumn
	}
	svcs, _, err := catalog.Service(fmt.Sprintf(`%s-master`, clusterID), "", nil)

	if err != nil {
		log.Error(fmt.Sprintf(`services.Service<%s>#GetWriteMaster() ! %s`, s.Name, err))
		return
	}

	if len(svcs) == 0 {
		return "", nil
	}
	ip = svcs[0].Address
	return
}

/*
ClusterIPs returns a string array of IPs for the given clusterID
*/
func (s *Service) ClusterIPs(clusterID string) (ips []string, err error) {
	client, err := consulapi.NewClient(consulapi.DefaultConfig())
	if err != nil {
		log.Error(fmt.Sprintf("services.Service<%s>#ClusterIPs() ! %s", s.Name, err))
		return
	}
	catalog := client.Catalog()
	services, _, err := catalog.Service(clusterID, "", nil)
	if err != nil {
		log.Error(fmt.Sprintf("services.Service<%s>#ClusterIPs() ! %s", s.Name, err))
		return
	}
	if len(services) == 0 {
		log.Error(fmt.Sprintf("services.Service<%s>#ClusterIPs() ! No services found, no known nodes?!", s.Name))
		return
	}
	ips = []string{}
	for index := range services {
		ips = append(ips, services[index].Address)
	}
	return
}
