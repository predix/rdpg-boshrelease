package consul_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"regexp"
	"strings"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	consulapi "github.com/hashicorp/consul/api"
	"github.com/starkandwayne/rdpg-acceptance-tests/helpers"
	. "github.com/starkandwayne/rdpg-acceptance-tests/rdpg-service/helper-functions"
)

type EnvVar struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func fetchConsulValue(key string) (value string, err error) {
	consulConfig := consulapi.DefaultConfig()
	consulConfig.Address = helpers.TestConfig.ConsulIP
	consulClient, _ := consulapi.NewClient(consulConfig)
	kv := consulClient.KV()
	kvp, _, err := kv.Get(key, nil)
	if err != nil {
		fmt.Println(`%s`, err)
		return value, err
	}
	if kvp == nil {
		return
	}
	value = string(kvp.Value)
	return
}

func fetchAdminAPIEnvKeyValue(ip, envKey string) (value string, err error) {
	// TODO: Allow for passing in Admin API port/user/pass
	adminPort := os.Getenv("RDPGD_ADMIN_PORT")
	adminUser := os.Getenv("RDPGD_ADMIN_USER")
	adminPass := os.Getenv("RDPGD_ADMIN_PASS")
	url := fmt.Sprintf("http://rdpg:admin@%s:%s/env/%s", ip, adminPort, envKey)
	req, err := http.NewRequest("GET", url, bytes.NewBuffer([]byte("{}")))
	req.SetBasicAuth(adminUser, adminPass)
	httpClient := &http.Client{}
	resp, err := httpClient.Do(req)
	if err != nil {
		fmt.Println(`%s`, err)
		return value, err
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(`%s`, err)
		return value, err
	}
	ev := EnvVar{}
	err = json.Unmarshal(body, &ev)
	return ev.Value, err
}

var _ = Describe("Consul Checks...", func() {

	It("Check Node Counts", func() {

		expectedPosgresqlSCNodeCount := 1
		expectedPgbdrScNodeCount := 2
		expectedPgbdrMcNodeCount := 3
		allClusterNames := GetAllClusterNames()

		for _, key := range allClusterNames {
			tempClusterNodes := GetNodesByClusterName(key)
			if key == "rdpgmc" {
				fmt.Printf("Found %d of %d Management Cluster %s Nodes\n", len(tempClusterNodes), expectedPgbdrMcNodeCount, key)
				Expect(len(tempClusterNodes)).To(Equal(expectedPgbdrMcNodeCount))
			} else {
				clusterService := GetClusterServiceType(tempClusterNodes[0].ServiceName)
				if clusterService == `pgbdr` {
					fmt.Printf("Found %d of %d Service Cluster %s Nodes\n", len(tempClusterNodes), expectedPgbdrScNodeCount, key)
					Expect(len(tempClusterNodes)).To(Equal(expectedPgbdrScNodeCount))
				} else {
					fmt.Printf("Found %d of %d Service Cluster %s Nodes\n", len(tempClusterNodes), expectedPosgresqlSCNodeCount, key)
					Expect(len(tempClusterNodes)).To(Equal(expectedPosgresqlSCNodeCount))

				}
			}
		}
	})

	It("Check Datacenter Name", func() {

		rdpgmcNodes := GetNodesByClusterName("rdpgmc")
		datacenter := helpers.TestConfig.Datacenter
		host := "consul.service." + datacenter + ".consul"
		//NOTE:  ConsulIP also contains the port.
		consulIP := strings.Split(helpers.TestConfig.ConsulIP, ":")[0]
		fmt.Printf("Digging host %s and address %s\n", host, consulIP)
		digResult, err := helpers.Dig(host, consulIP)
		fmt.Printf("Dig Answer count is %d, while the number of Management Cluster Nodes is %d\n", len(digResult.Answers), len(rdpgmcNodes))
		Expect(err).Should(BeNil())
		Expect(len(digResult.Answers)).To(Equal(len(rdpgmcNodes)))
		for _, res := range digResult.Answers {
			fmt.Printf("Dig Answer host name for %s is %s\n", res.Address, res.Host)
			Expect(res.Host).To(Equal(host + "."))
		}
	})

	It("Check Leader", func() {
		leader := GetLeader()
		Expect(leader).NotTo(BeEmpty())
	})

	It("Check Peers", func() {
		peersNum := 3
		peers := GetPeers()
		Expect(len(peers)).To(Equal(peersNum))
	})

	It("Check Health of all Services on Each Node", func() {
		allNodeNames := GetAllNodeNames()
		for _, name := range allNodeNames {
			healthCheck := GetNodeHealthByNodeName(name)
			Expect(len(healthCheck)).To(BeNumerically(">=", 1))
			for i := 0; i < len(healthCheck); i++ {
				fmt.Printf("The status for CheckId: %s on Node: %s is %s.\n", healthCheck[i].CheckID, name, healthCheck[i].Status)
				Expect(healthCheck[i].CheckID).NotTo(BeEmpty())
				Expect(healthCheck[i].Status).To(Equal("passing"))
			}
		}
	})

	It("Check Instances Hard and Soft Limits are set correctly in K/V Store", func() {
		consulConfig := consulapi.DefaultConfig()
		consulConfig.Address = helpers.TestConfig.ConsulIP
		consulClient, _ := consulapi.NewClient(consulConfig)
		catalog := consulClient.Catalog()
		services, _, _ := catalog.Services(nil)
		re := regexp.MustCompile(`^(rdpg(sc[0-9]+$))|(sc-([[:alnum:]|-])*m[0-9]+-c[0-9]+$)`)

		for clusterName := range services {
			if re.MatchString(clusterName) {
				fmt.Printf("Cluster %s:\n", clusterName)
				clusterNodes, _, _ := catalog.Service(clusterName, "", nil)

				manifestValue, _ := fetchAdminAPIEnvKeyValue(clusterNodes[0].Address, `RDPGD_INSTANCE_ALLOWED`)
				consulKey := fmt.Sprintf("rdpg/%s/capacity/instances/allowed", clusterName)
				consulValue, _ := fetchConsulValue(consulKey)
				fmt.Printf("Soft Instances Limit (allowed) manifest=%s, consul=%s \n", manifestValue, consulValue)
				Expect(consulValue).To(Equal(manifestValue))

				manifestValue, _ = fetchAdminAPIEnvKeyValue(clusterNodes[0].Address, `RDPGD_INSTANCE_LIMIT`)
				consulKey = fmt.Sprintf("rdpg/%s/capacity/instances/limit", clusterName)
				consulValue, _ = fetchConsulValue(consulKey)
				fmt.Printf("Hard Instances Limit (limit) manifest=%s, consul=%s \n", manifestValue, consulValue)
				Expect(consulValue).To(Equal(manifestValue))
			}
		}
	})

})
