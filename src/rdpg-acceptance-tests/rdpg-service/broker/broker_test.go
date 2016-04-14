package broker_test

import (
	"fmt"
	"net/http"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/starkandwayne/rdpg-acceptance-tests/helpers"
)

var _ = Describe("RDPG Service broker", func() {
	var (
		url string
	)

	BeforeEach(func() {
		url = fmt.Sprintf("http://%s/v2/catalog",
			helpers.TestConfig.BrokerUrlBase)
	})

	It("prompts for Basic Auth creds when they aren't provided", func() {
		resp, err := http.Get(url)
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(http.StatusUnauthorized))
	})

	It("does not accept bad Basic Auth creds", func() {
		req, err := http.NewRequest("GET", url, nil)
		req.SetBasicAuth("bad_username", "bad_password")
		resp, err := http.DefaultClient.Do(req)

		Expect(err).NotTo(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(http.StatusUnauthorized))
	})

	It("accepts valid Basic Auth creds", func() {
		req, err := http.NewRequest("GET", url, nil)
		req.SetBasicAuth(
			helpers.TestConfig.BrokerAdminUser,
			helpers.TestConfig.BrokerAdminPassword,
		)
		resp, err := http.DefaultClient.Do(req)
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.StatusCode).To(Equal(http.StatusOK))
	})

	// It("Registers a route", func() {
	// 	uri := fmt.Sprintf("http://%s:%s@%s/v2/catalog",
	// 		helpers.TestConfig.BrokerAdminUser,
	// 		helpers.TestConfig.BrokerAdminPassword,
	// 		helpers.TestConfig.BrokerUrlBase)

	// 	fmt.Printf("\n*** Curling url: %s\n", uri)
	// 	curlArgs := runner.Curl(uri, )
	// 	curlCmd := runner.NewCmdRunner(curlArgs, helpers.TestContext.ShortTimeout()).Run()
	// 	Expect(curlCmd).To(Say("HTTP Basic: Access denied."))
	// 	fmt.Println("Expected failure occured")
	// })
})
