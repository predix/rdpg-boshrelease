package gpb

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/starkandwayne/rdpgd/log"
)

var (
	mcUser       string
	mcPass       string
	mcPort       string
	mcIP         string
	mcConsulIP   string
	pgbFrequency int
)

//Work - Select a task from the queue for this server
func Work() {
	mcUser = os.Getenv(`RDPGD_PG_USER`)
	mcPass = os.Getenv(`RDPGD_PG_PASS`)
	mcPort = os.Getenv(`RDPGD_PG_PORT`)
	mcIP = os.Getenv(`RDPGD_PG_IP`)
	mcConsulIP = os.Getenv(`RDPGD_CONSUL_IP`)
	pgbFrequency, _ = strconv.Atoi(os.Getenv(`RDPGD_FREQUENCY`))

	for {

		err := configureGlobalPGBouncer()
		if err != nil {
			log.Error(fmt.Sprintf(`gpb.configureGlobalPGBouncer() ! Error: %s`, err))
		}

		log.Info(fmt.Sprintf(`Time goes by, sleeping for %d seconds...`, pgbFrequency))
		time.Sleep(time.Duration(pgbFrequency) * time.Second)
	}

}

/*
configureGlobalPGBouncer configures PGBouncer on the current system.
*/

func configureGlobalPGBouncer() (err error) {

	iniHeaderFile := `/var/vcap/jobs/global-pgbouncer/config/pgbouncer.ini.header`
	iniOutputFile := `/var/vcap/store/global-pgbouncer/config/pgbouncer.ini`
	userHeaderFile := `/var/vcap/jobs/global-pgbouncer/config/users.header`
	userOutputFile := `/var/vcap/store/global-pgbouncer/config/users`

	log.Info("gpb.configureGlobalPGBouncer()...")

	dir := `/var/vcap/jobs/rdpgd-global-pgbouncer`
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		log.Trace(fmt.Sprintf("gpb.configureGlobalPGBouncer() Not a global pgbouncer node since %s doesn't exist, skipping.", dir))
		return nil
	}

	instances, err := active()
	if err != nil {
		log.Error(fmt.Sprintf("gpb.configureGlobalPGBouncer() ! %s", err))
		return err
	}

	pgbIni, err := ioutil.ReadFile(iniHeaderFile)
	if err != nil {
		log.Error(fmt.Sprintf("gpb.configureGlobalPGBouncer() Attempted to read file %s ! %s", iniHeaderFile, err))
		return err
	}
	pgbUsers, err := ioutil.ReadFile(userHeaderFile)
	if err != nil {
		log.Error(fmt.Sprintf("gpb.configureGlobalPGBouncer() Attempted to read file %s ! %s", userHeaderFile, err))
		return err
	}
	pi := []string{string(pgbIni)}
	pu := []string{string(pgbUsers)}
	for index := range instances {
		i := instances[index]
		log.Trace(fmt.Sprintf("gpb.configureGlobalPGBouncer() Looking up master IP for database %s on cluster %s", i.Database, i.ClusterID))
		hostIP, err := getMasterIP(i.ClusterID)
		if err != nil {
			log.Error(fmt.Sprintf("gpb.configureGlobalPGBouncer() Could not resolve master ip for database %s on cluster %s, pgbouncer.ini will not be overwritten or reloaded", i.Database, i.ClusterID))
			return err
		}
		log.Trace(fmt.Sprintf("gpb.configureGlobalPGBouncer() master ip for database %s on cluster! %s", hostIP, i.Database))
		pi = append(pi, fmt.Sprintf(`%s = host=%s port=%s dbname=%s`, i.Database, hostIP, "7432", i.Database))
		pu = append(pu, fmt.Sprintf(`"%s" "%s"`, i.User, i.Pass))
	}
	pi = append(pi, "")
	pu = append(pu, "")

	beforeChecksum, _ := getFileChecksum(iniOutputFile)

	err = ioutil.WriteFile(iniOutputFile, []byte(strings.Join(pi, "\n")), 0640)
	if err != nil {
		log.Error(fmt.Sprintf("gpb.configureGlobalPGBouncer() ! %s", err))
		return err
	}

	afterChecksum, err := getFileChecksum(iniOutputFile)
	if err != nil {
		log.Error(fmt.Sprintf("gpb.configureGlobalPGBouncer() Could not determine the checksum of the pgbouncer.ini file ! %s", err))
		return err
	}

	err = ioutil.WriteFile(userOutputFile, []byte(strings.Join(pu, "\n")), 0640)
	if err != nil {
		log.Error(fmt.Sprintf("gpb.configureGlobalPGBouncer() ! %s", err))
		return err
	}

	if bytes.Equal(beforeChecksum, afterChecksum) {
		log.Info(fmt.Sprintf("gpb.configureGlobalPGBouncer() Checksum before: %x after: %x, since there are no changes not reloading pgBouncer", beforeChecksum, afterChecksum))
	} else {
		log.Info(fmt.Sprintf("gpb.configureGlobalPGBouncer() Checksum before: %x after: %x, since there are changes reloading pgBouncer", beforeChecksum, afterChecksum))
		cmd := exec.Command("/var/vcap/jobs/global-pgbouncer/bin/control", "reload")
		err = cmd.Run()
		if err != nil {
			log.Error(fmt.Sprintf("gpb.configureGlobalPGBouncer() ! %s", err))
			return err
		}

	}
	return
}

func getFileChecksum(filePath string) (checksum []byte, err error) {
	var result []byte
	file, err := os.Open(filePath)
	if err != nil {
		return result, err
	}
	defer file.Close()

	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		return result, err
	}

	return hash.Sum(result), err

}
