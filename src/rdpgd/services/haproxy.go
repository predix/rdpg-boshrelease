package services

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"

	"github.com/starkandwayne/rdpgd/log"
)

/*
ConfigureHAProxy configures HAProxy on the current system.
*/
func (s *Service) ConfigureHAProxy() (err error) {
	log.Trace(fmt.Sprintf(`services#Service.ConfigureHAProxy()...`))

	dir := `/var/vcap/jobs/rdpgd-service`
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		log.Trace(fmt.Sprintf(`services#Service.Configure() Not a service node since %s doesn't exist, skipping.`, dir))
		return nil
	}
	header, err := ioutil.ReadFile(`/var/vcap/jobs/rdpgd-service/config/haproxy/haproxy.cfg.header`)
	if err != nil {
		log.Error(fmt.Sprintf(`services.Service#Configure() ! %s`, err))
		return err
	}

	writeMasterIP, err := s.GetWriteMasterIP()
	if err != nil {
		log.Error(fmt.Sprintf(`services.Service#ConfigureHAProxy() ! %s`, err))
	}
	if writeMasterIP == "" {
		log.Trace(fmt.Sprintf(`services.Service#ConfigureHAProxy() No Write Master IP.`))
		return
	}

	// TODO: 5432 & 6432 from environmental configuration.
	// TODO: Should this list come from active Consul registered hosts instead?
	footer := fmt.Sprintf(`
frontend pgbdr_write_port
bind 0.0.0.0:5432
  mode tcp
  default_backend pgbdr_write_master

backend pgbdr_write_master
  mode tcp
	server master %s:6432 check
	`, writeMasterIP)

	hc := []string{string(header), footer}
	err = ioutil.WriteFile(`/var/vcap/jobs/haproxy/config/haproxy.cfg`, []byte(strings.Join(hc, "\n")), 0640)
	if err != nil {
		log.Error(fmt.Sprintf(`services#Service.Configure() ! %s`, err))
		return err
	}

	cmd := exec.Command(`/var/vcap/jobs/haproxy/bin/control`, "reload")
	err = cmd.Run()
	if err != nil {
		log.Error(fmt.Sprintf(`services#Service.Configure() ! %s`, err))
		return err
	}
	return
}
