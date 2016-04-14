package helpers

import (
	"fmt"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/cloudfoundry-incubator/cf-test-helpers/services"
)

var TestConfig RdpgIntegrationConfig
var TestContext services.Context

func PrepareAndRunTests(packageName string, t *testing.T, withContext bool) {
	var err error
	TestConfig, err = LoadConfig()
	if err != nil {
		panic("Loading config: " + err.Error())
	}

	err = ValidateConfig(&TestConfig)
	if err != nil {
		panic("Validating config: " + err.Error())
	}

	TestContext = services.NewContext(TestConfig.Config, "RdpgATS")

	if withContext {
		BeforeEach(TestContext.Setup)
		AfterEach(TestContext.Teardown)
	}

	fmt.Printf("Plans: %#v\n", TestConfig.Plans)

	RegisterFailHandler(Fail)
	RunSpecs(t, fmt.Sprintf("RDPG Acceptance Tests -- %s", packageName))
}
