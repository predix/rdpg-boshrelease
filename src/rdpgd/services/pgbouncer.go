package services

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"

	"github.com/starkandwayne/rdpgd/instances"
	"github.com/starkandwayne/rdpgd/log"
)

/*
ConfigurePGBouncer configures PGBouncer on the current system.
*/
func (s *Service) ConfigurePGBouncer() (err error) {
	log.Trace(fmt.Sprintf("services#Service.ConfigurePGBouncer()..."))

	dir := `/var/vcap/jobs/rdpgd-service`
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		log.Trace(fmt.Sprintf("services#Service.ConfigurePGBouncer() Not a service node since %s doesn't exist, skipping.", dir))
		return nil
	}
	// TODO: Adjust for cluster role...
	// TODO: This only happens on service clusters... simply return for management
	instances, err := instances.Active()
	if err != nil {
		log.Error(fmt.Sprintf("services#Service.ConfigurePGBouncer() ! %s", err))
		return err
	}

	pgbIni, err := ioutil.ReadFile(`/var/vcap/jobs/rdpgd-service/config/pgbouncer/pgbouncer.ini`)
	if err != nil {
		log.Error(fmt.Sprintf("services#Service.ConfigurePGBouncer() ! %s", err))
		return err
	}
	pgbUsers, err := ioutil.ReadFile(`/var/vcap/jobs/rdpgd-service/config/pgbouncer/users`)
	if err != nil {
		log.Error(fmt.Sprintf("services#Service.ConfigurePGBouncer() ! %s", err))
		return err
	}
	pi := []string{string(pgbIni)}
	pu := []string{string(pgbUsers)}
	// Currently these are done in the bosh release:
	//pi = append(pi, fmt.Sprintf(`health = host=127.0.0.1 port=%s dbname=health`, pgPort))
	//pu = append(pu, fmt.Sprintf(`"health" md5("checkhealth")`))
	for index := range instances {
		i := instances[index]
		pi = append(pi, fmt.Sprintf(`%s = host=%s port=%s dbname=%s`, i.Database, "127.0.0.1", pgPort, i.Database))
		pu = append(pu, fmt.Sprintf(`"%s" "%s"`, i.User, i.Pass))
	}
	pi = append(pi, "")
	pu = append(pu, "")

	err = ioutil.WriteFile(`/var/vcap/store/pgbouncer/config/pgbouncer.ini`, []byte(strings.Join(pi, "\n")), 0640)
	if err != nil {
		log.Error(fmt.Sprintf("services#Service.ConfigurePGBouncer() ! %s", err))
		return err
	}

	err = ioutil.WriteFile(`/var/vcap/store/pgbouncer/config/users`, []byte(strings.Join(pu, "\n")), 0640)
	if err != nil {
		log.Error(fmt.Sprintf("services#Service.ConfigurePGBouncer() ! %s", err))
		return err
	}

	cmd := exec.Command("/var/vcap/jobs/pgbouncer/bin/control", "reload")
	err = cmd.Run()
	if err != nil {
		log.Error(fmt.Sprintf("services#Service.ConfigurePGBouncer() ! %s", err))
		return err
	}
	return
}
