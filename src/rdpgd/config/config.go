package config

import (
	"database/sql"
	"fmt"

	"github.com/starkandwayne/rdpgd/globals"
	"github.com/starkandwayne/rdpgd/log"
	"github.com/starkandwayne/rdpgd/pg"
	"github.com/starkandwayne/rdpgd/utils/rdpgpg"
)

//DefaultConfig - holds one row object from rdpg.config
type DefaultConfig struct {
	ClusterID string `db:"cluster_id" json:"cluster_id"`
	Key       string `db:"key" json:"key"`
	Value     string `db:"value" json:"value"`
}

//Add - Insert a new schedule into tasks.schedules
func (dc *DefaultConfig) Add() (err error) {
	p := pg.NewPG(`127.0.0.1`, globals.PGPort, `rdpg`, `rdpg`, globals.PGPass)
	p.Set(`database`, `rdpg`)

	db, err := p.Connect()
	if err != nil {
		log.Error(fmt.Sprintf(`tasks.DefaultConfig() Could not open connection ! %s`, err))
	}

	defer db.Close()

	sq := fmt.Sprintf(`INSERT INTO rdpg.config (key,cluster_id,value) SELECT '%s', '%s', '%s' WHERE NOT EXISTS (SELECT key FROM rdpg.config WHERE key = '%s' AND cluster_id = '%s')`, dc.Key, dc.ClusterID, dc.Value, dc.Key, dc.ClusterID)
	log.Trace(fmt.Sprintf(`tasks#DefaultConfig.Add(): %s`, sq))
	_, err = db.Exec(sq)
	if err != nil {
		log.Error(fmt.Sprintf(`tasks#DefaultConfig.Add():%s`, err))
	}

	return
}

// GetValue - Returns the key value from rdpg.config
func GetValue(keyName string) (defaultBasePath string, err error) {
	address := `127.0.0.1`
	sq := fmt.Sprintf(`SELECT value AS keyvalue FROM rdpg.config WHERE key = '%s' AND cluster_id = '%s' ; `, keyName, globals.ClusterID)
	keyValue, err := rdpgpg.GetList(address, sq)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Error(fmt.Sprintf("config.GetValue ! No default value found for %s ! %s", keyName, err))
		} else {
			log.Error(fmt.Sprintf("config.GetValue ! Error when retrieving key value %s ! %s", keyName, err))
		}
		return ``, err
	}
	if len(keyValue) == 0 {
		log.Error(fmt.Sprintf("config.GetValue ! No value found for %s ! %s", keyName, err))
		return ``, fmt.Errorf("Key name %s not found", keyName)
	}
	return keyValue[0], nil
}
