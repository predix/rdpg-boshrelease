package rdpgconsul

import (
	"fmt"

	consulapi "github.com/hashicorp/consul/api"
	"github.com/starkandwayne/rdpgd/globals"
	"github.com/starkandwayne/rdpgd/log"
)

func GetNode() (node string, err error) {
	node = ``
	client, err := consulapi.NewClient(consulapi.DefaultConfig())
	if err != nil {
		log.Error(fmt.Sprintf("consul.GetNode() consulapi.NewClient()! %s", err))
		return
	}

	consulAgent := client.Agent()
	info, err := consulAgent.Self()
	node = info["Config"]["AdvertiseAddr"].(string)
	return
}

func IsWriteNode(currentIP string) (isWriter bool) {
	isWriter = false

	client, err := consulapi.NewClient(consulapi.DefaultConfig())
	if err != nil {
		log.Error(fmt.Sprintf(`consul.consul_helpers IsWriteMode() ! %s`, err))
		return
	}
	catalog := client.Catalog()
	svcs, _, err := catalog.Service(fmt.Sprintf(`%s-master`, globals.ClusterID), "", nil)
	if err != nil {
		log.Error(fmt.Sprintf(`consul.consul_helpers IsWriteMode() Retrieving Service Catalog ! %s`, err))
		return
	}
	if len(svcs) == 0 {
		return
	}
	if svcs[0].Address == currentIP {
		isWriter = true
		return
	}

	return
}
