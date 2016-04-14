package gpb

import (
	"errors"
	"fmt"

	consulapi "github.com/hashicorp/consul/api"
	"github.com/starkandwayne/rdpgd/log"
)

func getMasterIP(clusterName string) (masterIp string, err error) {

	log.Trace(fmt.Sprintf("gpb#consul.getMasterIP() Calling out to Consul at address %s", mcConsulIP))

	consulConfig := consulapi.DefaultConfig()
	consulConfig.Address = mcConsulIP
	consulClient, err := consulapi.NewClient(consulConfig)
	if err != nil {
		log.Error(fmt.Sprintf(`gpb#consul.getMasterIP() Consul IP: %s ! %s`, mcConsulIP, err))
		return
	}

	masterNode, _, err := consulClient.Catalog().Service(fmt.Sprintf(`%s-master`, clusterName), "", nil)
	if err != nil {
		log.Error(fmt.Sprintf("gpb#consul.getMasterIP() Cluster Name: %s ! %s", clusterName, err))
		return
	}

	if len(masterNode) == 0 {
		masterIp = "0.0.0.0"
		return masterIp, errors.New("Could not find the consul master ip")
	}

	masterIp = masterNode[0].Address
	log.Trace(fmt.Sprintf("gpb#consul.getMasterIP() Found master ip for %s = %s", clusterName, masterIp))
	return masterIp, err

}
