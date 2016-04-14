package cfsb

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/starkandwayne/rdpgd/instances"
	"github.com/starkandwayne/rdpgd/log"
	"github.com/starkandwayne/rdpgd/pg"
)

type Binding struct {
	ID         int          `db:"id"`
	BindingID  string       `db:"binding_id" json:"binding_id"`
	InstanceID string       `db:"instance_id" json:"instance_id"`
	Creds      *Credentials `json:"credentials"`
}

// Create Binding in the data store
func (b *Binding) Create() (err error) {
	log.Trace(fmt.Sprintf(`cfsb.Binding#Create(%s,%s) ... `, b.InstanceID, b.BindingID))

	instance, err := instances.FindByInstanceID(b.InstanceID)
	if err != nil {
		log.Error(fmt.Sprintf(`cfsb.Binding#Create(%s) instances.FindByInstanceID(%s) ! %s`, b.BindingID, b.InstanceID, err))
		return
	}

	dns, err := instance.ExternalDNS()
	if err != nil {
		log.Error(fmt.Sprintf(`cfsb.Binding#Create(%s) instance.ExternalDNS(%s) ! %s`, b.BindingID, b.InstanceID, err))
		return
	}
	// For now the crednetials are fixed, future feature is that we will be able to
	// create new users/credentials for each binding later on. Currently only
	// one set of credentials exists for each Instance.
	s := strings.Split(dns, ":")
	uri, err := instance.URI()
	if err != nil {
		log.Error(fmt.Sprintf(`cfsb.Binding#Create(%s) instance.URI(%s) ! %s`, b.BindingID, b.InstanceID, err))
		return
	}
	dsn, err := instance.DSN()
	if err != nil {
		log.Error(fmt.Sprintf(`cfsb.Binding#Create(%s) instance.DSN(%s) ! %s`, b.BindingID, b.InstanceID, err))
		return
	}
	jdbc, err := instance.JDBCURI()
	if err != nil {
		log.Error(fmt.Sprintf(`cfsb.Binding#Create(%s) instance.JDBCURI(%s) ! %s`, b.BindingID, b.InstanceID, err))
		return
	}

	b.Creds = &Credentials{
		InstanceID: b.InstanceID,
		BindingID:  b.BindingID,
		URI:        uri,
		DSN:        dsn,
		JDBCURI:    jdbc,
		Host:       s[0],
		Port:       s[1],
		UserName:   instance.User,
		Password:   instance.Pass,
		Database:   instance.Database,
	}

	p := pg.NewPG(`127.0.0.1`, pbPort, `rdpg`, `rdpg`, pgPass)
	db, err := p.Connect()
	if err != nil {
		log.Error(fmt.Sprintf("cfsb.Binding#Create(%s) ! %s", b.BindingID, err))
		return
	}
	defer db.Close()

	err = b.Find()
	if err != nil {
		if err == sql.ErrNoRows { // Does not yet exist, insert the binding and it's credentials.
			sq := fmt.Sprintf(`INSERT INTO cfsb.bindings (instance_id,binding_id) VALUES (lower('%s'),lower('%s'));`, b.InstanceID, b.BindingID)
			log.Trace(fmt.Sprintf(`cfsb.Binding#Create() > %s`, sq))
			_, err = db.Exec(sq)
			if err != nil {
				log.Error(fmt.Sprintf(`cfsb.Binding#Create(%s) %s ! %s`, b.BindingID, sq, err))
			}
			err := b.Creds.Create()
			if err != nil {
				log.Error(fmt.Sprintf(`cfsb.Binding#Create(%s) b.Creds.Create() ! %s`, b.BindingID, err))
			}
		}
	} else { // Binding already exists, return existing binding and credentials.
		return
	}
	return
}

func (b *Binding) Find() (err error) {
	log.Trace(fmt.Sprintf(`cfsb.Binding#Find(%s) ... `, b.BindingID))

	if b.BindingID == "" {
		return errors.New("Binding ID is empty, can not Binding#Find()")
	}
	p := pg.NewPG(`127.0.0.1`, pbPort, `rdpg`, `rdpg`, pgPass)
	db, err := p.Connect()
	if err != nil {
		log.Error(fmt.Sprintf("cfsb.Binding#Find(%s) ! %s", b.BindingID, err))
		return
	}
	defer db.Close()

	sq := fmt.Sprintf(`SELECT id,instance_id FROM cfsb.bindings WHERE binding_id=lower('%s') LIMIT 1`, b.BindingID)
	log.Trace(fmt.Sprintf(`cfsb.Binding#Find(%s) > %s`, b.BindingID, sq))
	err = db.Get(b, sq)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Error(fmt.Sprintf("cfsb.Binding#Find(%s) ! Could not find binding with given Binding ID", b.BindingID))
		} else {
			log.Error(fmt.Sprintf("cfsb.Binding#Find(%s) ! %s", b.BindingID, err))
		}
	} else {
		// TODO: Load creds: b.Creds := Credentials{} ... b.Creds.Find()
	}
	return
}

func (b *Binding) Remove() (err error) {
	log.Trace(fmt.Sprintf(`cfsb.Binding#Remove(%s) ... `, b.BindingID))
	err = b.Find()
	if err != nil {
		log.Error(fmt.Sprintf(`cfsb.Binding#Remove(%s) ! %s`, b.BindingID, err))
		return
	}
	p := pg.NewPG(`127.0.0.1`, pbPort, `rdpg`, `rdpg`, pgPass)
	db, err := p.Connect()
	if err != nil {
		log.Error(fmt.Sprintf("cfsb.Binding#Remove(%s) ! %s", b.BindingID, err))
		return
	}
	defer db.Close()

	// TODO: Scheduled background task that does any cleanup necessary for an
	// unbinding (remove credentials?)
	sq := fmt.Sprintf(`UPDATE cfsb.bindings SET ineffective_at=CURRENT_TIMESTAMP WHERE binding_id=lower('%s')`, b.BindingID)
	log.Trace(fmt.Sprintf(`cfsb.Binding#Remove(%s) SQL > %s`, b.BindingID, sq))
	_, err = db.Exec(sq)
	if err != nil {
		log.Error(fmt.Sprintf(`cfsb.Binding#Remove(%s) ! %s`, b.BindingID, err))
	}

	b.Creds = &Credentials{
		InstanceID: b.InstanceID,
		BindingID:  b.BindingID,
	}

	err = b.Creds.Remove()
	if err != nil {
		log.Error(fmt.Sprintf(`cfsb.Binding#Remove(%s) b.Creds.Remove() ! %s`, b.BindingID, err))
	}
	return
}
