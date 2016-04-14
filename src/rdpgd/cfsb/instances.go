package cfsb

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/starkandwayne/rdpgd/instances"
	"github.com/starkandwayne/rdpgd/log"
	"github.com/starkandwayne/rdpgd/pg"
)

func NewServiceInstance(instanceID, serviceID, planID, organizationID, spaceID string) (i *instances.Instance, err error) {
	re := regexp.MustCompile("[^A-Za-z0-9_]")
	id := instanceID
	identifier := strings.ToLower(string(re.ReplaceAll([]byte(id), []byte(""))))
	i = &instances.Instance{
		InstanceID:     strings.ToLower(instanceID),
		ServiceID:      strings.ToLower(serviceID),
		PlanID:         strings.ToLower(planID),
		OrganizationID: strings.ToLower(organizationID),
		SpaceID:        strings.ToLower(spaceID),
		Database:       "d" + identifier,
		User:           "u" + identifier,
	}
	if i.ServiceID == "" {
		err = errors.New("Service ID is required.")
		return
	}
	if i.PlanID == "" {
		err = errors.New("Plan ID is required.")
		return
	}
	if i.OrganizationID == "" {
		err = errors.New("Organization ID is required.")
		return
	}
	if i.SpaceID == "" {
		err = errors.New("Space ID is required.")
		return
	}
	// TODO: Good to go now, assign to a service cluster...
	return
}

func Active() (is []instances.Instance, err error) {
	p := pg.NewPG(`127.0.0.1`, pbPort, `rdpg`, `rdpg`, pgPass)
	db, err := p.Connect()
	if err != nil {
		log.Error(fmt.Sprintf("cfsb.Active() ! %s", err))
		return
	}
	defer db.Close()

	sq := ` SELECT id, instance_id, service_id, plan_id, organization_id, space_id, dbname, dbuser, dbpass FROM cfsb.instances WHERE effective_at IS NOT NULL AND decommissioned_at IS NULL LIMIT 1; `
	err = db.Select(&is, sq)
	if err != nil {
		// TODO: Change messaging if err is sql.NoRows then say couldn't find instance with instanceId
		log.Error(fmt.Sprintf("cfsb.Active() ! %s", err))
	}
	return
}
