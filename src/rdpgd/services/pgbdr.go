package services

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/starkandwayne/rdpgd/log"
	"github.com/starkandwayne/rdpgd/pg"
)

/*
ConfigurePGBDR configures PGBDR on the current system.
*/
func (s *Service) ConfigurePGBDR() (err error) {
	// TODO: Adjust for cluster role...

	clusterID := os.Getenv("RDPGD_CLUSTER")
	if clusterID == "" {
		matrixName := os.Getenv(`RDPGD_MATRIX`)
		matrixNameSplit := strings.SplitAfterN(matrixName, `-`, -1)
		matrixColumn := os.Getenv(`RDPGD_MATRIX_COLUMN`)
		for i := 0; i < len(matrixNameSplit)-1; i++ {
			clusterID = clusterID + matrixNameSplit[i]
		}
		clusterID = clusterID + "c" + matrixColumn
	}
	ips, err := s.ClusterIPs(clusterID)
	if err != nil {
		log.Error(fmt.Sprintf("services#Service.ConfigurePGBDR() ! %s", err))
		return err
	}
	hbaHeader, err := ioutil.ReadFile(`/var/vcap/jobs/pgbdr/config/pg_hba.conf`)
	if err != nil {
		log.Error(fmt.Sprintf("services#Service.ConfigurePGBDR() ! %s", err))
		return err
	}

	hba := []string{string(hbaHeader)}
	for _, ip := range ips {
		hba = append(hba, fmt.Sprintf(`host    replication   postgres %s/32  trust`, ip))
		hba = append(hba, fmt.Sprintf(`host    all           postgres %s/32  trust`, ip))
		hba = append(hba, fmt.Sprintf(`host    all           rdpg %s/32  trust`, ip))
	}

	hba = append(hba, "")

	err = ioutil.WriteFile(`/var/vcap/store/pgbdr/data/pg_hba.conf`, []byte(strings.Join(hba, "\n")), 0640)
	if err != nil {
		log.Error(fmt.Sprintf("services#Service.ConfigurePGBDR() ! %s", err))
		return err
	}

	p := pg.NewPG(`127.0.0.1`, pgPort, `postgres`, `postgres`, ``)
	db, err := p.Connect()
	if err != nil {
		log.Error(fmt.Sprintf("tasks.ConfigurePGBDR() Failed connecting to %s err: %s", p.URI, err))
		return
	}
	defer db.Close()

	var successful bool
	err = db.Get(&successful, `SELECT pg_reload_conf()`)
	if err != nil {
		log.Error(fmt.Sprintf("services.ConfigurePGBDR(pgbdr) pg_reload_conf() ! %s", err))
		return
	}
	if !successful {
		log.Error("services.ConfigurePGBDR(pgbdr) ! ERROR pg_reload_conf() was unsuccessful!")
		return
	}
	return
}
