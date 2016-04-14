package cfsb

import (
	"fmt"

	"github.com/starkandwayne/rdpgd/log"
	"github.com/starkandwayne/rdpgd/pg"
)

type Catalog struct {
	Services []Service `json:"services"`
}

func (c *Catalog) Fetch() (err error) {
	log.Trace(`cfsb.Catalog#Fetch()...`)
	p := pg.NewPG(`127.0.0.1`, pbPort, `rdpg`, `rdpg`, pgPass)
	db, err := p.Connect()
	if err != nil {
		log.Error(fmt.Sprintf("cfsb.Catalog#Fetch() ! %s", err))
		return
	}
	defer db.Close()

	sq := `SELECT service_id,name,description,bindable FROM cfsb.services;`
	log.Trace(fmt.Sprintf(`cfsb.Catalog#Fetch() > %s`, sq))
	err = db.Select(&c.Services, sq)
	if err != nil {
		log.Error(fmt.Sprintf("cfsb.Catalog#Fetch() db.Select() ! %s", err.Error()))
		return
	}

	// TODO: Account for plans being associated with a service.
	for i, _ := range c.Services {
		service := &c.Services[i]
		sq := fmt.Sprintf(`SELECT plan_id,name,description FROM cfsb.plans WHERE service_id = '%s' ORDER BY name;`, service.ServiceID)
		log.Trace(fmt.Sprintf(`cfsb.Catalog#Fetch() > %s`, sq))
		err = db.Select(&service.Plans, sq)
		if err != nil {
			log.Error(fmt.Sprintf("cfsb.Catalog#Fetch() db.Select() ! %s", err.Error()))
			return
		}
		c.Services[i].Tags = []string{"rdpg", "postgresql"}
		// c.Services[i].Dashboard = DashboardClient{}
	}
	return
}
