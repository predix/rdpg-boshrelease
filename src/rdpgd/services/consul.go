package services

import (
	"errors"
	"fmt"

	"github.com/starkandwayne/rdpgd/log"
)

/*
ConfigureConsul configures consul on the current system.
TODO: Actually configure consul adjusting for cluster roles.
*/
func (s *Service) ConfigureConsul() (err error) {
	log.Trace(fmt.Sprintf("services#Service.ConfigureConsul()..."))
	return errors.New(`services.Service#Configure("consul") is not yet implemented`)
}
