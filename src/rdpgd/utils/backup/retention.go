package backup

import (
	"errors"
	"fmt"
	"time"

	"github.com/starkandwayne/rdpgd/globals"
	"github.com/starkandwayne/rdpgd/log"
	"github.com/starkandwayne/rdpgd/pg"
)

//Returns the local and remote retention policies for the database requested
// If no custom local/remote rule is found, the default is put in its place.
func GetRetentionPolicy(dbname string) (ret RetentionPolicy, err error) {
	p := pg.NewPG(`127.0.0.1`, globals.PGPort, `rdpg`, `rdpg`, globals.PGPass)
	db, err := p.Connect()
	if err != nil {
		log.Error(fmt.Sprintf("utils/backup.GetRetentionPolicy ! pg.Connect() : %s", err.Error()))
		return RetentionPolicy{}, err
	}
	defer db.Close()
	/* Maybe this isn't the retention system's responsibility after all...
	dbExists, err := DatabaseExists(dbname)
	if err != nil {
		log.Error(fmt.Sprintf("utils/backup.GetRetentionPolicy ! Error when trying to retrieve list of databases: %s # %s", dbname, err.Error()))
		return RetentionPolicy{}, err
	}
	if !dbExists {
		errorMessage := fmt.Sprintf("utils/backup.GetRetentionPolicy ! Requested retention policy for non-existant database: %s", dbname)
		log.Warn(errorMessage)
		return RetentionPolicy{}, errors.New(errorMessage)
	}
	*/
	//Get custom retention rules
	response := []RetentionRuleRow{}
	sql := fmt.Sprintf("SELECT * FROM backups.retention_rules WHERE dbname='%s'", dbname)
	err = db.Select(&response, sql)
	if err != nil {
		log.Error(fmt.Sprintf("utils/backup.GetRetentionPolicy ! sqlx.db.Select(%s) : %s", sql, err))
		return RetentionPolicy{}, err
	}
	//Theres only local and remote, so if theres more than two rules, something is very wrong.
	if len(response) > 2 {
		errorMessage := "utils/backup.GetRetentionPolicy: More than two rules for a database?!"
		log.Error(errorMessage)
		return RetentionPolicy{}, errors.New(errorMessage)
	}
	//Establish defaults
	ret = RetentionPolicy{
		DBName:      dbname,
		LocalHours:  globals.LocalRetentionTime,
		RemoteHours: globals.RemoteRetentionTime,
	}
	//Look for a local and remote retention rule
	for _, v := range response {
		if v.IsRemoteRule {
			ret.RemoteHours = v.Hours
		} else {
			ret.LocalHours = v.Hours
		}
	}
	return
}

//Sets a retention rule for this cluster for the information specified in the RetentionRuleRow struct.
func (ret *RetentionRuleRow) Put() (err error) {
	p := pg.NewPG(`127.0.0.1`, globals.PGPort, `rdpg`, `rdpg`, globals.PGPass)
	db, err := p.Connect()
	if err != nil {
		log.Error(fmt.Sprintf("utils/backup.RetentionRuleRow.Put ! pg.Connect() : %s", err.Error()))
		return err
	}
	defer db.Close()
	exists, err := DatabaseExists(ret.DBName)
	if err != nil {
		log.Error(fmt.Sprintf("utils/backup.RetentionRuleRow.Put ! Error when trying to check if database exists: %s # %s", ret.DBName, err.Error()))
		return err
	}
	if !exists {
		errorMessage := fmt.Sprintf("utils/backup.RetentionRuleRow.Put ! Requested retention policy for non-existant database: %s", ret.DBName)
		log.Warn(errorMessage)
		return errors.New(errorMessage)
	}

	//Stringify the boolean
	rulebool := "false"
	if ret.IsRemoteRule {
		rulebool = "true"
	}
	//Check if this row already exists in the database
	sql := fmt.Sprintf("SELECT COUNT(*) FROM backups.retention_rules WHERE dbname='%s' AND is_remote_rule=%s", ret.DBName, rulebool)
	count := []int{}
	err = db.Select(&count, sql)
	if err != nil {
		log.Error(fmt.Sprintf("utils/backup.RetentionRuleRow.Put ! sqlx.db.Select(%s) : %s", sql, err))
		return err
	}
	//If the row already exists, update the row.
	if count[0] > 0 {
		sql = fmt.Sprintf("UPDATE backups.retention_rules SET hours=%f WHERE dbname='%s' AND is_remote_rule=%s", ret.Hours, ret.DBName, rulebool)
	} else { //Otherwise, insert a new row
		sql = fmt.Sprintf("INSERT INTO backups.retention_rules(dbname, hours, is_remote_rule) VALUES ('%s', %f, %s)", ret.DBName, ret.Hours, rulebool)
	}
	//Do the thing with the query.
	_, err = db.Exec(sql)
	if err != nil {
		log.Error(fmt.Sprintf("utils/backup.RetentionRuleRow.Put ! sqlx.db.Select(%s) : %s", sql, err))
		return err
	}
	return
}

//Returns all the custom retention rules on this cluster.
func GetCustomRetentionRules() (ret []RetentionRuleRow, err error) {
	p := pg.NewPG(`127.0.0.1`, globals.PGPort, `rdpg`, `rdpg`, globals.PGPass)
	db, err := p.Connect()
	if err != nil {
		log.Error(fmt.Sprintf("utils/backup.GetRetentionPolicy ! pg.Connect() : %s", err.Error()))
		return nil, err
	}
	defer db.Close()
	//Get custom retention rules
	ret = []RetentionRuleRow{}
	sql := fmt.Sprintf("SELECT * FROM backups.retention_rules;")
	err = db.Select(&ret, sql)
	if err != nil {
		log.Error(fmt.Sprintf("utils/backup.GetRetentionPolicy ! sqlx.db.Select(%s) : %s", sql, err))
		return nil, err
	}
	return
}

//Calculates whether that given backup should be retained - in other words,
// if more time has passed since the timestamp given than the amount of time
// given by the appropriate retention policy for the database given.
func ShouldRetain(timestamp, dbname string, isRemote bool) (bool, error) {
	backupTime, err := time.Parse(globals.TIME_FORMAT, timestamp)
	if err != nil {
		log.Error(fmt.Sprintf("tasks.EnforceFileRetention() ! time.Parse(%s, %s) encountered an error : %s", globals.TIME_FORMAT, timestamp, err.Error()))
		return true, err
	}
	timePassed := time.Since(backupTime)
	//Get the retention policy for this database
	policy, err := GetRetentionPolicy(dbname)
	if err != nil {
		log.Error(fmt.Sprintf("tasks.EnforceFileRetention() ! utils/backup.GetRetentionPolicy(%s) produced an error: %s", dbname, err))
		return true, err
	}
	if isRemote {
		return timePassed.Hours() < policy.RemoteHours, nil
	} else {
		return timePassed.Hours() < policy.LocalHours, nil
	}
}
