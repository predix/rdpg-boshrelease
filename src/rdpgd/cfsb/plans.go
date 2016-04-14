package cfsb

import (
	"fmt"

	"github.com/starkandwayne/rdpgd/log"
	"github.com/starkandwayne/rdpgd/pg"
)

type PlanDetails struct {
	Cost        string              `json:"cost"`
	Bullets     []map[string]string `json:"bullets"`
	DisplayName string              `json:"displayname"`
}

type Plan struct {
	PlanID      string      `db:"plan_id" json:"id"`
	Name        string      `db:"name" json:"name"`
	Description string      `db:"description" json:"description"`
	Metadata    PlanDetails `json:"metadata"`
	MgmtDbUri   string      `json:""`
}

func FindPlan(planID string) (plan *Plan, err error) {
	p := pg.NewPG(`127.0.0.1`, pbPort, `rdpg`, `rdpg`, pgPass)
	db, err := p.Connect()
	if err != nil {
		log.Error(fmt.Sprintf("cfsb#FindPlan(%s) ! %s", planID, err))
		return
	}
	defer db.Close()

	plan = &Plan{}
	sq := fmt.Sprintf(`SELECT plan_id,name,description FROM cfsb.plans WHERE id='%s' LIMIT 1;`, planID)
	log.Trace(fmt.Sprintf("cfsb.FindPlan(%s) > %s", planID, sq))
	err = db.Get(&plan, sq, planID)
	if err != nil {
		log.Error(fmt.Sprintf("cfsb.FindPlan(%s) %s", planID, err))
	}
	return plan, err
}
