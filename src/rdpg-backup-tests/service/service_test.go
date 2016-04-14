package service_test

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pborman/uuid"

	"github.com/cloudfoundry-incubator/cf-test-helpers/cf"
	"github.com/cloudfoundry-incubator/cf-test-helpers/runner"
	"github.com/cloudfoundry-incubator/cf-test-helpers/services"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gbytes"
	. "github.com/onsi/gomega/gexec"
)

var config = loadConfig()
var httpClient = initHttpClient()

// services.Config  Structure required for use of the CF-CLI Go API.
// ServiceName			Name of the service to be tested.
// PlanNames				Names of the plans to be tested.
// APIPort				The port that the RDPG admin API listens on.
// APIUsername		The username used for admin API HTTP authentication.
// APIPassword		The password used for admin API HTTP authentication.
type backupTestConfig struct {
	services.Config

	ServiceName      string   `json:"service_name"`
	PlanNames        []string `json:"plan_names"`
	RestorePlanNames []string `json:"restore_plan_names"`
	APIPort          int      `json:"rdpg_api_port"`
	APIUsername      string   `json:"rdpg_api_username"`
	APIPassword      string   `json:"rdpg_api_password"`
	TestQueueBackup  bool     `json:"test_queue_backup"`
	WorkerWaitPeriod int      `json:"worker_wait_period"`
	BackupWaitPeriod int      `json:"backup_wait_period"`
	S3Enabled        bool     `json:"s3_enabled"`
}

// Takes config file from environment variable CONFIG_PATH and parses it as
// JSON into the backupTestConfig structure, which is returned.
func loadConfig() (testConfig backupTestConfig) {
	path := os.Getenv("CONFIG_PATH")
	if path == "" {
		panic("No Config Path was Set!")
	}
	configFile, err := os.Open(path)
	if err != nil {
		panic(err)
	}

	decoder := json.NewDecoder(configFile)
	err = decoder.Decode(&testConfig)
	if err != nil {
		panic(err)
	}

	return testConfig
}

func initHttpClient() *http.Client {
	trans := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
	return &http.Client{Transport: trans}
}

// Returns the base location string of the launched app in the network.
func appUri(appName string) string {
	return "http://" + appName + "." + config.AppsDomain
}

func rdpgUri(clusterLocation string) string {
	return fmt.Sprintf("http://%s:%s@%s:%d", config.APIUsername, config.APIPassword,
		clusterLocation, config.APIPort)
}

//Returns true if the string is in the given slice. False otherwise.
func sliceContainsString(slice []string, target string) bool {
	for _, v := range slice {
		if v == target {
			return true
		}
	}
	return false
}

//Beginning of the test function.
var _ = Describe("RDPG Service Broker", func() {
	var timeout = time.Second * 60
	var retryInterval = time.Second / 2
	var appPath = "../assets/postgres-test-app"

	var appName string

	validChars := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z", "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z", "0", "1", "2", "3", "4", "5", "6,", "7", "8", "9"}

	randomNameStartingWithLetter := func() string {
		ret := validChars[rand.Int31n(52)]
		for i := 0; i < 9; i++ {
			ret = ret + validChars[rand.Int31n(62)]
		}
		return ret
	}

	randomServiceName := func() string {
		return uuid.NewRandom().String()
	}

	assertAppIsRunning := func(appName string) {
		pingUri := appUri(appName) + "/ping"
		fmt.Println("Checking that the app is responding at url: ", pingUri)
		Eventually(runner.Curl(pingUri, "-k"), config.ScaledTimeout(timeout), retryInterval).Should(Say("SUCCESS"))
		fmt.Println("\n")
	}

	getBackups := func(uri, dbname string) []map[string]string {
		request, err := http.NewRequest("GET", uri+"?dbname="+dbname, nil)
		Ω(err).ShouldNot(HaveOccurred())
		resp, err := httpClient.Do(request)
		Ω(err).ShouldNot(HaveOccurred())
		Ω(resp.StatusCode).Should(Equal(http.StatusOK))
		backupsMap := make(map[string]interface{})
		backupsJSON, err := ioutil.ReadAll(resp.Body)
		fmt.Printf("backupsJSON:\n%s", backupsJSON)
		Ω(err).ShouldNot(HaveOccurred())
		fmt.Println("\n--Unmarshaling JSON")
		err = json.Unmarshal(backupsJSON, &backupsMap)
		Ω(err).ShouldNot(HaveOccurred())
		//If there isn't a backup for this database.....
		if backupsMap == nil || len(backupsMap) == 0 || backupsMap[dbname] == nil {
			//Then, hand back an empty array
			return []map[string]string{}
		}
		//Otherwise, hand back this database's array of backups.
		// Go is annoying and makes me make a new map to return, basically.
		retMaps := make([]map[string]string, 0)
		for i, m := range backupsMap[dbname].([]interface{}) {
			thisMap := m.(map[string]interface{})
			retMaps = append(retMaps, map[string]string{})
			for k, v := range thisMap {
				retMaps[i][k] = v.(string)
			}
		}
		return retMaps
	}

	getLocalBackups := func(location, dbname string) []map[string]string {
		uri := rdpgUri(location) + "/backup/list/local"
		return getBackups(uri, dbname)
	}

	getRemoteBackups := func(location, dbname string) []map[string]string {
		uri := rdpgUri(location) + "/backup/list/remote"
		return getBackups(uri, dbname)
	}

	backupInList := func(target string, list []map[string]string) bool {
		for _, v := range list {
			if v["Name"] == target {
				return true
			}
		}
		return false
	}

	assertNewBackup := func(oldList, newList []map[string]string) {
		//Check that the newest backup is... newer... than the old-newest backup.
		//Note that the name of the backup is a timestamp
		//First condition: If there are no backups... there is no new backup.
		cond := len(newList) != 0
		Ω(cond).Should(BeTrue())
		// If there were no old backups, the existance of a backup now means that one was made.
		// Otherwise, check their names, which are timestamps, and assert that the most recent one
		// in the newList is newer than that of the oldList.
		cond = len(oldList) == 0 ||
			newList[len(newList)-1]["Name"] > oldList[len(oldList)-1]["Name"]
		Ω(cond).Should(BeTrue())
		//...And that the new backup file isn't empty
		numBytes, err := strconv.Atoi(newList[len(newList)-1]["Bytes"])
		Ω(err).ShouldNot(HaveOccurred())
		cond = numBytes > 0
		Ω(cond).Should(BeTrue())
	}

	BeforeSuite(func() {
		config.TimeoutScale = 3
		services.NewContext(config.Config, "rdpg-postgres-smoke-test").Setup()
	})

	AssertBackupBehavior := func(planName string) {
		serviceInstanceName := randomServiceName()
		serviceCreated := false
		serviceBound := false
		appName = randomServiceName()

		It("Can create a service and bind an app", func() {
			Eventually(cf.Cf("push", appName, "-m", "256M", "-p", appPath, "-s", "cflinuxfs2", "--no-start"), config.ScaledTimeout(timeout)).Should(Exit(0))
			Eventually(cf.Cf("create-service", config.ServiceName, planName, serviceInstanceName), config.ScaledTimeout(timeout)).Should(Exit(0))
			serviceCreated = true
			Eventually(cf.Cf("bind-service", appName, serviceInstanceName), config.ScaledTimeout(timeout)).Should(Exit(0))
			serviceBound = true
			Eventually(cf.Cf("start", appName), config.ScaledTimeout(5*time.Minute)).Should(Exit(0))
			assertAppIsRunning(appName)
		})

		It(fmt.Sprintf("successfully creates backups on service cluster for plan %s", planName), func() {
			//Successful endpoint calls respond 200 and their first line is "SUCCESS"

			//Let's first confirm that the application was able to get the uri of the database.
			uri := appUri(appName) + "/uri"
			fmt.Println("\n--Checking if the application received a database uri")
			Eventually(runner.Curl(uri, "-k", "-X", "GET"), config.ScaledTimeout(timeout), retryInterval).Should(Say("SUCCESS"))

			//If we can get a timestamp, we are connected to the database
			uri = appUri(appName) + "/timestamp"
			fmt.Println("\n--Checking that the a connection to the database can be made")
			Eventually(runner.Curl(uri, "-k", "-X", "GET"), config.ScaledTimeout(timeout), retryInterval).Should(Say("SUCCESS"))

			//Inserting a table into the database.
			//This is less of a test and more of setting up a later restore test.
			//But it doesn't hurt as a test... basic functionality
			firstTableName := randomNameStartingWithLetter()
			fmt.Printf("--Creating new table: public.%s\n", firstTableName)
			sql := fmt.Sprintf("CREATE TABLE public.%s (key varchar(255) PRIMARY KEY, value int);", firstTableName)
			uri = appUri(appName) + "/exec"
			Eventually(runner.Curl(uri, "-k", "-X", "POST", "-d", "sql="+sql), config.ScaledTimeout(timeout), retryInterval).Should(Say("SUCCESS"))

			fmt.Println("--Verifying that newly created table is present.")
			sql = fmt.Sprintf("SELECT * FROM public.%s;", firstTableName)
			Eventually(runner.Curl(uri, "-k", "-X", "POST", "-d", "sql="+sql), config.ScaledTimeout(timeout), retryInterval).Should(Say("SUCCESS"))

			uri = appUri(appName) + "/uri/location"
			fmt.Println("\n--Getting the location of the targeted database")
			locationBuffer := runner.Curl(uri, "-k", "-X", "GET").Buffer()
			Eventually(locationBuffer, config.ScaledTimeout(timeout), retryInterval).Should(Say("SUCCESS"))
			location := strings.TrimPrefix(string(locationBuffer.Contents()), "SUCCESS\n")

			uri = appUri(appName) + "/uri/dbname"
			fmt.Println("\n--Getting the name of the targeted database")
			nameBuffer := runner.Curl(uri, "-k", "-X", "GET").Buffer()
			Eventually(nameBuffer, config.ScaledTimeout(timeout), retryInterval).Should(Say("SUCCESS"))
			dbname := strings.TrimPrefix(string(nameBuffer.Contents()), "SUCCESS\n")

			fmt.Println("\n--Getting the list of preexisting backups")
			oldBackups := getLocalBackups(location, dbname)

			uri = rdpgUri(location) + "/backup/now"
			fmt.Println("\n--Waiting before directly initiating a backup")
			time.Sleep(time.Duration(config.BackupWaitPeriod) * time.Second)
			fmt.Println("\n--Directly initiating a backup")
			resp, err := httpClient.PostForm(uri, url.Values{"dbname": {dbname}})
			Ω(err).ShouldNot(HaveOccurred())
			Ω(resp.StatusCode).Should(Equal(http.StatusOK))

			fmt.Println("\n--Checking the list of backups again")
			newBackups := getLocalBackups(location, dbname)

			assertNewBackup(oldBackups, newBackups)

			if config.TestQueueBackup {
				uri = rdpgUri(location) + "/backup/enqueue"
				fmt.Println("\n--Enqueuing a backup with RDPG's task system")
				resp, err = httpClient.PostForm(uri, url.Values{"dbname": {dbname}})
				Ω(err).ShouldNot(HaveOccurred())
				Ω(resp.StatusCode).Should(Equal(http.StatusOK))
				fmt.Printf("\n--Waiting for %d seconds before checking to see if backup was completed.\n", config.WorkerWaitPeriod)
				time.Sleep(time.Duration(config.WorkerWaitPeriod) * time.Second)

				fmt.Println("\n--Checking if backup is present in local backups")
				oldBackups = newBackups
				newBackups := getLocalBackups(location, dbname)

				assertNewBackup(oldBackups, newBackups)
			} else {
				fmt.Println("\n--SKIPPING QUEUE PORTION OF BACKUP TEST")
			}

			if sliceContainsString(config.RestorePlanNames, planName) {
				fmt.Printf("--Testing in-place restores for plan %s\n", planName)
				currentBackup := newBackups[len(newBackups)-1]["Name"]
				//Make a change.
				secondTableName := ""
				for secondTableName == "" || secondTableName == firstTableName {
					secondTableName = randomNameStartingWithLetter()
				}
				fmt.Printf("--Creating new table: public.%s\n", secondTableName)
				sql = fmt.Sprintf("CREATE TABLE public.%s (key varchar(255) PRIMARY KEY, value int);", secondTableName)
				uri = appUri(appName) + "/exec"
				Eventually(runner.Curl(uri, "-k", "-X", "POST", "-d", "sql="+sql), config.ScaledTimeout(timeout), retryInterval).Should(Say("SUCCESS"))

				fmt.Println("--Verifying that newly created table is present.")
				sql = fmt.Sprintf("SELECT * FROM public.%s;", secondTableName)
				Eventually(runner.Curl(uri, "-k", "-X", "POST", "-d", "sql="+sql), config.ScaledTimeout(timeout), retryInterval).Should(Say("SUCCESS"))

				//Let's do a restore to revert that change.
				uri = rdpgUri(location) + "/restore/inplace"
				fmt.Println("\n--Initiating a restore.")
				resp, err = httpClient.PostForm(uri, url.Values{"dbname": {dbname}, "filename": {currentBackup}})
				Ω(err).ShouldNot(HaveOccurred())
				Ω(resp.StatusCode).Should(Equal(http.StatusOK))

				uri = appUri(appName) + "/exec"
				//Is the database actually restored? Or just deleted?
				fmt.Println("\n--Verifying that the table that was made before the backup can still be accessed")
				sql = fmt.Sprintf("SELECT * FROM public.%s;", firstTableName)
				Eventually(runner.Curl(uri, "-k", "-X", "POST", "-d", "sql="+sql), config.ScaledTimeout(timeout), retryInterval).Should(Say("SUCCESS"))

				//Let's make sure the newer changes were reverted.
				fmt.Println("\n--Verifying that previously created table is no longer present. THE CURL SHOULD RETURN FAILURE!")
				sql = fmt.Sprintf("SELECT * FROM public.%s;", secondTableName)
				Eventually(runner.Curl(uri, "-k", "-X", "POST", "-d", "sql="+sql), config.ScaledTimeout(timeout), retryInterval).Should(Say("FAILURE"))
			} else {
				fmt.Printf("SKIPPING RESTORE TESTS FOR PLAN %s. NOT ENABLED IN CONFIG\n", planName)
			}

			//Test S3 Storage
			if config.S3Enabled {
				currentBackup := newBackups[len(newBackups)-1]["Name"]
				//Move the backup up to the remote storage.
				uri := rdpgUri(location) + "/backup/remote/copyto"
				fmt.Println("\n--Moving backups up to remote storage.")
				request, err := http.NewRequest("PUT", uri, nil)
				Ω(err).ShouldNot(HaveOccurred())
				request.Form = url.Values{"dbname": {dbname}, "filename": {currentBackup}}
				resp, err = httpClient.Do(request)
				Ω(err).ShouldNot(HaveOccurred())
				Ω(resp.StatusCode).Should(Equal(http.StatusOK))

				//Now check to see that the backup is actually up on remote storage.
				uri = rdpgUri(location) + "/backup/list/remote"
				fmt.Println("\n--Checking to see that the backup is in the cloud.")
				remoteBackups := getRemoteBackups(location, dbname)
				Ω(backupInList(currentBackup, remoteBackups)).Should(BeTrue())
			}
		})

		It("Can unbind and delete the service", func() {
			if serviceBound {
				Eventually(cf.Cf("unbind-service", appName, serviceInstanceName), config.ScaledTimeout(timeout)).Should(Exit(0))
			}
			if serviceCreated {
				Eventually(cf.Cf("delete-service", "-f", serviceInstanceName), config.ScaledTimeout(timeout)).Should(Exit(0))
			}
		})

	}

	Context("for each plan", func() {
		for _, planName := range config.PlanNames {
			AssertBackupBehavior(planName)
		}
	})
})
