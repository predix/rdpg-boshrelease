package instances

import (
	"fmt"
	"regexp"
	"strconv"

	consulapi "github.com/hashicorp/consul/api"

	"github.com/starkandwayne/rdpgd/log"
)

func (i *Instance) ClusterIPs() (ips []string, err error) {
	client, err := consulapi.NewClient(consulapi.DefaultConfig())
	if err != nil {
		log.Error(fmt.Sprintf("instances.Instance<%s>#ClusterIPs() ! %s", i.Database, err))
		return
	}
	catalog := client.Catalog()
	svcs, _, err := catalog.Service(i.ClusterID, "", nil)
	if err != nil {
		log.Error(fmt.Sprintf("instances.Instance<%s>#ClusterIPs() ! %s", i.Database, err))
		return
	}
	if len(svcs) == 0 {
		log.Error(fmt.Sprintf("instances.Instance<%s>#ClusterIPs() ! No services found, no known nodes?!", i.Database))
		return
	}
	ips = []string{}
	for index, _ := range svcs {
		ips = append(ips, svcs[index].Address)
	}
	return
}

func ClusterCapacity() (totalClusterCapacity int, err error) {
	totalClusterCapacity = 0
	client, err := consulapi.NewClient(consulapi.DefaultConfig())
	if err != nil {
		log.Error(fmt.Sprintf("instances.cluster#ClusterCapacity() ! %s", err))
		return
	}

	catalog := client.Catalog()
	services, _, err := catalog.Services(nil)
	if err != nil {
		log.Error(fmt.Sprintf("instances.cluster#ClusterCapacity() ! %s", err))
		return
	}
	if len(services) == 0 {
		log.Error(fmt.Sprintf("instances.cluster#ClusterCapacity() ! No services found, no known clusters?!"))
		return
	}
	re := regexp.MustCompile(`^(rdpgsc[0-9]+$)|(sc-([[:alnum:]|-])*m[0-9]+-c[0-9]+$)`)

	kv := client.KV()
	for key, _ := range services {
		if re.MatchString(key) {
			kvp, _, err := kv.Get("rdpg/"+key+"/capacity/instances/allowed", nil)
			if err != nil {
				log.Error(fmt.Sprintf("instances.cluster#ClusterCapacity() : getKeyValue! %s", err))
				return 0, err
			}
			if kvp == nil {
				log.Trace(fmt.Sprintf(`instances.cluster#ClusterCapacity() kv.Get(%s) Key is not set...`, ClusterID, key))
				return 0, err
			}
			s := string(kvp.Value)
			allowed, err := strconv.Atoi(s)

			kvp, _, err = kv.Get("rdpg/"+key+"/capacity/instances/limit", nil)
			if err != nil {
				log.Error(fmt.Sprintf("instances.cluster#ClusterCapacity() : getKeyValue! %s", err))
				return 0, err
			}
			if kvp == nil {
				log.Trace(fmt.Sprintf(`rdpg.RDPG<%s>#getKey() kv.Get(%s) Key is not set...`, ClusterID, key))
				return 0, err
			}
			s = string(kvp.Value)
			limit, err := strconv.Atoi(s)

			if allowed < limit {
				totalClusterCapacity += allowed
			} else {
				totalClusterCapacity += limit
			}
		}
	}
	return
}
