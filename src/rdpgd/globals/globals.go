//Package globals -  package where information relevant across RDPG can be stored.
package globals

/* By the nature of this package, many files may need to import this package, and
   therefore imports of other packages from RDPG from within this package should
   be extremely minimal. Basically, only log. */
import (
	"fmt"
	"os"
	"strconv"
	"strings"

	consulapi "github.com/hashicorp/consul/api"
	"github.com/starkandwayne/rdpgd/log"
)

var (
	MyIP                string
	ServiceRole         string //Gets set in main->parseArgs()
	ClusterService      string
	ClusterID           string
	LocalBackupPath     string
	RestoreStagePath    string
	LocalRetentionTime  float64 //In hours
	RemoteRetentionTime float64 //In hours
	CanAutoRestore      bool
	PBPort              string
	PGPass              string
	PGPort              string
	StuckDuration       string
	UserExtensions      string
)

const TIME_FORMAT string = "20060102150405"
const PSQL_PATH string = `/var/vcap/packages/postgresql-9.4/bin/psql`

func init() {
	// Set MyIP variable
	client, err := consulapi.NewClient(consulapi.DefaultConfig())
	if err != nil {
		log.Error(fmt.Sprintf("config.init() consulapi.NewClient()! %s", err))
	} else {
		agent := client.Agent()
		info, err := agent.Self()
		if err != nil {
			log.Error(fmt.Sprintf("config.init() agent.Self()! %s", err))
		} else {
			MyIP = info["Config"]["AdvertiseAddr"].(string)
		}
	}

	ClusterService = os.Getenv("RDPGD_CLUSTER_SERVICE")
	//Set up the ClusterID
	MatrixName := os.Getenv(`RDPGD_MATRIX`)
	MatrixNameSplit := strings.SplitAfterN(MatrixName, `-`, -1)
	MatrixColumn := os.Getenv(`RDPGD_MATRIX_COLUMN`)
	ClusterID = os.Getenv("RDPGD_CLUSTER")
	if ClusterID == "" {
		for i := 0; i < len(MatrixNameSplit)-1; i++ {
			ClusterID = ClusterID + MatrixNameSplit[i]
		}
		ClusterID = ClusterID + "c" + MatrixColumn
	}
	//LocalBackupPath has a prerequisite that ClusterID has already been assigned. Be careful if order switching.
	LocalBackupPath = fmt.Sprintf("/var/vcap/store/pgbdr/backups/%s/%s", os.Getenv(`RDPGD_ENVIRONMENT_NAME`), ClusterID)
	RestoreStagePath = `/var/vcap/store/recover/`

	LocalRetentionTime, err = strconv.ParseFloat(os.Getenv(`RDPGD_LOCAL_RETENTION_TIME`), 64)
	if err != nil {
		log.Error(fmt.Sprintf("globals.init() ! Parsing local retention time: strconv.ParseFloat(%s, 64) : %s", os.Getenv(`RDPGD_LOCAL_RETENTION_TIME`), err))
	}
	RemoteRetentionTime, err = strconv.ParseFloat(os.Getenv(`RDPGD_REMOTE_RETENTION_TIME`), 64)
	if err != nil {
		log.Error(fmt.Sprintf("globals.init() ! Parsing remote retention time: strconv.ParseFloat(%s, 64) : %s", os.Getenv(`RDPGD_REMOTE_RETENTION_TIME`), err))
	}
	//CanAutoRestore requires that ClusterService is already assigned.
	CanAutoRestore = (ClusterService != "pgbdr")

	PBPort = os.Getenv("RDPGD_PB_PORT")
	if PBPort == "" {
		PBPort = "6432"
	}
	PGPass = os.Getenv("RDPGD_PG_PASS")
	if PGPass == "" {
		PGPass = "admin"
	}

	PGPort = os.Getenv("RDPGD_PG_PORT")
	if PGPort == "" {
		log.Warn("RDPGD_PG_PORT environment variable was not configured")
	}

	StuckDuration = os.Getenv("RDPGD_STUCK_DURATION")
	if StuckDuration == "" {
		StuckDuration = `6 hours`
	}

	UserExtensions = os.Getenv("RDPGD_PG_EXTENSIONS")

}
