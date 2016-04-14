package pg

import (
	"database/sql"
	"fmt"
	"regexp"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/starkandwayne/rdpgd/log"
)

type PG struct {
	// Name string `` ???
	IP             string `db:"ip" json:"ip"`
	Port           string `db:"port" json:"port"`
	User           string `db:"user" json:"user"`
	Pass           string `db:"pass" json:"pass"`
	Database       string `db:"database" json:"database"`
	ConnectTimeout string `db:"connect_timeout" json:"connect_timeout,omitempty"`
	SSLMode        string `db:"sslmode" json:"sslmode,omitempty"`
	URI            string `db:"uri" json:"uri"`
	DSN            string `db:"dsn" json:"dsn"`
}

// Create and return a new PG using default parameters
func NewPG(ip, port, user, database, pass string) (p *PG) {
	p = &PG{IP: ip, Port: port, User: user, Database: database, Pass: pass}
	p.ConnectTimeout = `5` // Default connection time out.
	p.SSLMode = `disable`  // Default disable SSL Mode, can be overwritten using Set()
	p.pgURI()
	p.pgDSN()
	return
}

// Check if the given PostgreSQL User Exists on the host.
func (p *PG) UserExists(dbuser string) (exists bool, err error) {
	log.Trace(fmt.Sprintf(`pg.PG<%s>#UserExists(%s) Checking if postgres user exists...`, p.IP, dbuser))
	p.Set(`database`, `postgres`)
	db, err := p.Connect()
	if err != nil {
		log.Error(fmt.Sprintf("pg.PG<%s>#UserExists(%s) %s ! %s", p.IP, dbuser, p.URI, err))
		return
	}
	defer db.Close()

	type name struct {
		Name string `db:"name"`
	}
	var n name
	sq := fmt.Sprintf(`SELECT rolname AS name FROM pg_roles WHERE rolname='%s' LIMIT 1;`, dbuser)
	err = db.Get(&n, sq)
	if err != nil {
		if err == sql.ErrNoRows {
			exists = false
			err = nil
		} else {
			log.Error(fmt.Sprintf(`pg.PG<%s>#UserExists(%s) ! %s`, p.IP, dbuser, err))
			return
		}
	}
	if n.Name != "" {
		exists = true
	} else {
		exists = false
	}
	return
}

// Check if the given PostgreSQL Database Exists on the host.
func (p *PG) DatabaseExists(dbname string) (exists bool, err error) {
	log.Trace(fmt.Sprintf(`pg.PG<%s>#DatabaseExists(%s) Checking if postgres database exists...`, p.IP, dbname))
	p.Set(`database`, `postgres`)
	db, err := p.Connect()
	if err != nil {
		log.Error(fmt.Sprintf("pg.PG<%s>#DatabaseExists(%s) %s ! %s", p.IP, dbname, p.URI, err))
		return
	}
	defer db.Close()

	type name struct {
		Name string `db:"name"`
	}
	var n name
	sq := fmt.Sprintf(`SELECT datname AS name FROM pg_database WHERE datname='%s' LIMIT 1;`, dbname)
	err = db.Get(&n, sq)
	if err != nil {
		if err == sql.ErrNoRows {
			exists = false
			err = nil
		} else {
			log.Error(fmt.Sprintf(`pg.PG<%s>#DatabaseExists(%s) ! %s`, p.IP, dbname, err))
			return
		}
	}
	if n.Name != "" {
		exists = true
	} else {
		exists = false
	}
	return
}

// Create a given user on a single target host.
func (p *PG) CreateUser(dbuser, dbpass string) (err error) {
	log.Trace(fmt.Sprintf(`pg.PG<%s>#CreateUser(%s) Creating postgres user exists...`, p.IP, dbuser))
	p.Set(`database`, `postgres`)
	db, err := p.Connect()
	if err != nil {
		log.Error(fmt.Sprintf("pg.PG<%s>#CreateUser(%s) %s ! %s", p.IP, dbuser, p.URI, err))
		return
	}
	defer db.Close()

	exists, err := p.UserExists(dbuser)
	if err != nil {
		log.Error(fmt.Sprintf("pg.PG<%s>#CreateUser(%s) ! %s", p.IP, dbuser, err))
		return
	}
	if exists {
		log.Trace(fmt.Sprintf(`pg.PG<%s>#CreateUser(%s) already exists, skipping.`, p.IP, dbuser))
		return nil
	}

	sq := fmt.Sprintf(`CREATE USER %s;`, dbuser)
	log.Trace(fmt.Sprintf(`pg.PG<%s>#CreateUser(%s) > %s`, p.IP, dbuser, sq))
	_, err = db.Exec(sq)
	if err != nil {
		log.Error(fmt.Sprintf("pg.PG<%s>#CreateUser(%s) ! %s", p.IP, dbuser, err))
		db.Close()
		return err
	}

	sq = fmt.Sprintf(`ALTER USER %s ENCRYPTED PASSWORD '%s';`, dbuser, dbpass)
	log.Trace(fmt.Sprintf(`pg.PG<%s>#CreateUser(%s)`, p.IP, dbuser))
	_, err = db.Exec(sq)
	if err != nil {
		log.Error(fmt.Sprintf(`pg.PG<%s>#CreateUser(%s) ! %s`, p.IP, dbuser, err))
		return
	}

	return
}

// Create a given user on a single target host.
func (p *PG) GrantUserPrivileges(dbuser string, priviliges []string) (err error) {
	log.Trace(fmt.Sprintf(`pg.PG<%s>#GrantUserPrivileges(%s) Granting postgres user priviliges %+v...`, p.IP, dbuser, priviliges))
	p.Set(`database`, `postgres`)
	db, err := p.Connect()
	if err != nil {
		log.Error(fmt.Sprintf("pg.PG<%s>#UserGrantPrivileges(%s) %s ! %s", p.IP, dbuser, p.URI, err))
		return
	}
	defer db.Close()

	for _, priv := range priviliges {
		sq := fmt.Sprintf(`ALTER USER %s WITH %s;`, dbuser, priv)
		log.Trace(fmt.Sprintf(`pg.PG<%s>#UserGrantPrivileges(%s) > %s`, p.IP, dbuser, sq))
		result, err := db.Exec(sq)
		rows, _ := result.RowsAffected()
		if rows > 0 {
			log.Trace(fmt.Sprintf(`pg.PG<%s>#CreateUser(%s) Successfully Created.`, p.IP, dbuser))
		}
		if err != nil {
			log.Error(fmt.Sprintf(`pg.PG<%s>#CreateUser(%s) ! %s`, p.IP, dbuser, err))
			return err
		}
	}
	return nil
}

// Create a given database owned by user on a single target host.
func (p *PG) CreateDatabase(dbname, dbuser string) (err error) {
	log.Trace(fmt.Sprintf(`pg.PG#CreateDatabase(%s) Creating postgres database...`, dbname))
	p.Set(`database`, `postgres`)
	db, err := p.Connect()
	if err != nil {
		log.Error(fmt.Sprintf("pg.PG<%s>#CreateDatabase(%s,%s) %s ! %s", p.IP, dbname, dbuser, p.URI, err))
		return
	}
	defer db.Close()

	exists, err := p.UserExists(dbuser)
	if err != nil {
		log.Error(fmt.Sprintf("pg.PG<%s>#CreateDatabase(%s,%s) ! %s", p.IP, dbname, dbuser, err))
		return
	}
	if !exists {
		err = fmt.Errorf(`User does not exist, ensure that postgres user '%s' exists first.`, dbuser)
		log.Error(fmt.Sprintf("pg.PG<%s>#CreateDatabase(%s,%s) ! %s", p.IP, dbname, dbuser, err))
		return
	}

	exists, err = p.DatabaseExists(dbname)
	if err != nil {
		log.Error(fmt.Sprintf("pg.PG<%s>#CreateDatabase(%s,%s) ! %s", p.IP, dbname, dbuser, err))
		return
	}
	if exists {
		log.Trace(fmt.Sprintf("pg.PG<%s>#CreateDatabase(%s,%s) Database already exists, skipping.", p.IP, dbname, dbuser))
		return
	}

	sq := fmt.Sprintf(`CREATE DATABASE %s WITH OWNER %s TEMPLATE template0 ENCODING 'UTF8'`, dbname, dbuser)
	log.Trace(fmt.Sprintf(`pg.PG<%s>#CreateDatabase(%s,%s) > %s`, p.IP, dbname, dbuser, sq))
	_, err = db.Query(sq)
	if err != nil {
		log.Error(fmt.Sprintf("pg.PG<%s>#CreateDatabase(%s,%s) ! %s", p.IP, dbname, dbuser, err))
		return
	}

	sq = fmt.Sprintf(`REVOKE ALL ON DATABASE "%s" FROM public`, dbname)
	log.Trace(fmt.Sprintf(`pg.PG<%s>#CreateDatabase(%s,%s) > %s`, p.IP, dbname, dbuser, sq))
	_, err = db.Exec(sq)
	if err != nil {
		log.Error(fmt.Sprintf("pg.PG<%s>#CreateDatabase(%s,%s) ! %s", p.IP, dbname, dbuser, err))
	}

	sq = fmt.Sprintf(`GRANT ALL PRIVILEGES ON DATABASE %s TO %s`, dbname, dbuser)
	log.Trace(fmt.Sprintf(`pg.PG<%s>#CreateDatabase(%s,%s) > %s`, p.IP, dbname, dbuser, sq))
	_, err = db.Query(sq)
	if err != nil {
		log.Error(fmt.Sprintf("pg.PG<%s>#CreateDatabase(%s,%s) ! %s", p.IP, dbname, dbuser, err))
		return
	}
	return nil
}

// Create given extensions on a single target host.
func (p *PG) CreateExtensions(dbname string, exts []string) (err error) {
	log.Trace(fmt.Sprintf(`pg.PG<%s>#CreateExtensions(%s) Creating postgres extensions %+v on database...`, p.IP, dbname, exts))
	p.Set(`database`, dbname)
	db, err := p.Connect()
	if err != nil {
		log.Error(fmt.Sprintf("pg.PG<%s>#CreateExtensions(%s) %s ! %s", p.IP, dbname, p.URI, err))
		return
	}

	ddlLockRE := regexp.MustCompile(`cannot acquire DDL lock|Database is locked against DDL operations`)
	// TODO: Only create extension if it doesn't already exist.
	for _, ext := range exts {
		sq := fmt.Sprintf(`CREATE EXTENSION IF NOT EXISTS "%s";`, ext)
		log.Trace(fmt.Sprintf(`pg.PG<%s>#CreateExtensions() > %s`, p.IP, sq))
		for { // Retry loop for acquiring DDL schema lock.
			_, err = db.Exec(sq)
			if err != nil {
				if ddlLockRE.MatchString(err.Error()) {
					log.Trace("pg.PG#CreateExtensions() DDL Lock not available, waiting...")
					time.Sleep(1 * time.Second)
					continue
				}
				db.Close()
				log.Error(fmt.Sprintf("pg.PG<%s>#CreateExtensions() %s ! %s", p.IP, ext, err))
				return
			}
			break
		}
	}
	db.Close()
	return
}

// First break replication within the specified database
// Then disables the specific database from the postgres database
func (p *PG) BDRDisableDatabase(dbname string) (err error) {
	log.Trace(fmt.Sprintf(`pg.PG<%s>#BDRDisableDatabase(%s) Disabling postgres database...`, p.IP, dbname))
	p.Set(`database`, dbname)
	db, err := p.Connect()
	if err != nil {
		log.Error(fmt.Sprintf("pg.PG<%s>#BDRDisableDatabase(%s) %s ! %s", p.IP, dbname, p.URI, err))
		return
	}

	nodes := []string{}
	sq := `SELECT node_name FROM bdr.bdr_nodes;`
	err = db.Select(&nodes, sq)
	if err != nil {
		log.Error(fmt.Sprintf("pg.PG<%s>#BDRDisableDatabase(%s) %s ! %s", p.IP, dbname, sq, err))
	}
	for index, _ := range nodes {
		sq := fmt.Sprintf(`SELECT bdr.bdr_part_by_node_names(ARRAY['%s']);`, nodes[index])
		_, err = db.Exec(sq)
		if err != nil {
			log.Error(fmt.Sprintf("pg.PG<%s>#BDRDisableDatabase(%s) %s ! %s", p.IP, dbname, sq, err))
		}
	}
	db.Close()

	p.DisableDatabase(dbname) // Call the non-BDR disabling function.
	return
}

func (p *PG) BDRGroupCreate(nodeName, dbname string) (err error) {
	log.Trace(fmt.Sprintf(`pg.PG<%s>#BDRGroupCreate(%s,%s) Creating postgres BDR Group for database...`, p.IP, dbname, nodeName))
	p.Set(`database`, dbname)
	db, err := p.Connect()
	if err != nil {
		return
	}
	defer db.Close()
	sq := fmt.Sprintf(`SELECT bdr.bdr_group_create( local_node_name := '%s', node_external_dsn := 'host=%s port=%s user=%s dbname=%s'); `, nodeName, p.IP, p.Port, p.User, dbname)
	log.Trace(fmt.Sprintf(`pg.PG<%s>#BDRGroupCreate(%s,%s) > %s`, p.IP, nodeName, dbname, sq))
	_, err = db.Exec(sq)
	if err == nil {
		sq = `SELECT bdr.bdr_node_join_wait_for_ready();`
		log.Trace(fmt.Sprintf(`pg.PG<%s>#BDRGroupCreate(%s,%s) > %s`, p.IP, nodeName, dbname, sq))
		_, err = db.Exec(sq)
	}
	db.Close()
	return
}

func (p *PG) BDRGroupJoin(nodeName, dbname string, target PG) (err error) {
	log.Trace(fmt.Sprintf(`pg.PG<%s>#BDRGroupJoin(%s,%s) Joining postgres BDR Group for database...`, p.IP, dbname, nodeName))
	p.Set(`database`, dbname)
	db, err := p.Connect()
	if err != nil {
		return
	}
	defer db.Close()

	sq := fmt.Sprintf(`SELECT bdr.bdr_group_join( local_node_name := '%s', node_external_dsn := 'host=%s port=%s user=%s dbname=%s', join_using_dsn := 'host=%s port=%s user=%s dbname=%s'); `, nodeName, p.IP, p.Port, p.User, p.Database, target.IP, target.Port, target.User, dbname)
	log.Trace(fmt.Sprintf(`pg.PG<%s>#BDRGroupJoin(%s) > %s`, p.IP, dbname, sq))
	_, err = db.Exec(sq)
	if err == nil {
		sq = `SELECT bdr.bdr_node_join_wait_for_ready();`
		log.Trace(fmt.Sprintf(`pg.PG<%s>#BDRGroupJoin(%s) > %s`, p.IP, dbname, sq))
		for {
			_, err = db.Exec(sq)
			if err == nil {
				break
			} else {
				re := regexp.MustCompile(`canceling statement due to conflict with recovery`)
				if re.MatchString(err.Error()) {
					time.Sleep(3 * time.Second)
					continue
				} else {
					log.Error(fmt.Sprintf(`pg.PG<%s>#BDRGroupJoin(%s) ! %s`, p.IP, dbname, err))
					return err
				}
			}
		}
	}
	return
}

func (p *PG) BDRJoinWaitForReady() (err error) {
	db, err := p.Connect()
	if err != nil {
		return
	}
	defer db.Close()
	sq := `SELECT bdr.bdr_node_join_wait_for_ready()`
	log.Trace(fmt.Sprintf(`pg.PG<%s>#BDRJoinWaitForReady() > %s`, p.IP, sq))
	_, err = db.Exec(sq)
	if err != nil {
		log.Error(fmt.Sprintf(`pg.PG<%s>#BDRJoinWaitForReady() > %s`, p.IP, sq))
	}
	return
}

func (p *PG) StopReplication(dbname string) (err error) {
	// TODO Finish this function
	p.Set(`database`, `postgres`)
	db, err := p.Connect()
	if err != nil {
		log.Error(fmt.Sprintf("pg.PG<%s>#DropDatabase(%s) ! %s", p.IP, dbname, err))
		return
	}
	// sq := fmt.Sprintf(SELECT slot_name FROM pg_replication_slots WHERE database='%s',dbname);
	// pg_recvlogical --drop-slot

	defer db.Close()
	return
}

func (p *PG) DropDatabase(dbname string) (err error) {
	log.Trace(fmt.Sprintf(`pg.PG<%s>#DropDatabase(%s) Dropping postgres database...`, p.IP, dbname))
	p.Set(`database`, `postgres`)
	db, err := p.Connect()
	if err != nil {
		log.Error(fmt.Sprintf("pg.PG<%s>#DropDatabase(%s) ! %s", p.IP, dbname, err))
		return
	}
	defer db.Close()

	exists, err := p.DatabaseExists(dbname)
	if err != nil {
		log.Error(fmt.Sprintf("pg.PG<%s>#DropDatabase(%s) ! %s", p.IP, dbname, err))
		return
	}
	if !exists {
		log.Error(fmt.Sprintf("pg.PG<%s>#DropDatabase(%s) Database %s already does not exist.", p.IP, dbname, err))
		return
	}

	// TODO: How do we drop a database in bdr properly?
	sq := fmt.Sprintf(`DROP DATABASE IF EXISTS %s`, dbname)
	log.Trace(fmt.Sprintf(`pg.PG<%s>#DropDatabase(%s) > %s`, p.IP, dbname, sq))
	_, err = db.Exec(sq)
	if err != nil {
		log.Error(fmt.Sprintf("pg.PG<%s>#DropDatabase(%s) ! %s", p.IP, dbname, err))
		return
	}
	return
}

func (p *PG) DropUser(dbuser string) (err error) {
	log.Trace(fmt.Sprintf(`pg.PG<%s>#DropUser(%s) Dropping postgres user...`, p.IP, dbuser))
	p.Set(`database`, `postgres`)
	db, err := p.Connect()
	if err != nil {
		log.Error(fmt.Sprintf("pg.PG<%s>#DropUser(%s) %s ! %s", p.IP, dbuser, p.URI, err))
		return
	}
	defer db.Close()

	exists, err := p.UserExists(dbuser)
	if err != nil {
		log.Error(fmt.Sprintf("pg.PG<%s>#DropUser(%s) ! %s", p.IP, dbuser, err))
		return
	}
	if !exists {
		log.Error(fmt.Sprintf("pg.PG<%s>#DropUser(%s) User already does not exist, skipping.", p.IP, dbuser))
		return
	}
	// TODO: How do we drop a database in bdr properly?
	sq := fmt.Sprintf(`DROP USER %s`, dbuser)
	log.Trace(fmt.Sprintf(`pg.PG<%s>#DropDatabase(%s) > %s`, p.IP, dbuser, sq))
	_, err = db.Exec(sq)
	if err != nil {
		log.Error(fmt.Sprintf("pg.PG<%s>#DropDatabase(%s) ! %s", p.IP, dbuser, err))
		return
	}
	return
}

// Set host property to given value then regenerate the URI and DSN properties.
func (p *PG) Set(key, value string) (err error) {
	switch key {
	case "ip":
		p.IP = value
	case "port":
		p.Port = value
	case "user":
		p.User = value
	case "database":
		p.Database = value
	case "connect_timeout":
		p.ConnectTimeout = value
	case "sslmode":
		p.SSLMode = value
	case "pass":
		p.Pass = value
	case "default": // A Bug
		err = fmt.Errorf(`Attempt to set unknown key %s to value %s for host %+v.`, key, value, *p)
		return err
	}
	p.pgURI()
	p.pgDSN()
	return
}

// Build and set the host's URI property according to the pattern:
//   postgres://user:password@ip:port/database?sslmode=&connect_timeout=&...
func (p *PG) pgURI() {
	p.URI = "postgres://"
	if p.User != "" {
		p.URI += p.User
	}
	if p.Pass != "" {
		p.URI += fmt.Sprintf(`:%s`, p.Pass)
	}
	if p.IP != "" {
		p.URI += fmt.Sprintf(`@%s`, p.IP)
	}
	if p.Port != "" {
		p.URI += fmt.Sprintf(`:%s`, p.Port)
	}
	if p.Database != "" {
		p.URI += fmt.Sprintf(`/%s`, p.Database)
	}
	p.URI += fmt.Sprintf(`?sslmode=%s&fallback_application_name=rdpgd`, p.SSLMode)
	if p.ConnectTimeout != "" {
		p.URI += fmt.Sprintf(`&connect_timeout=%s`, p.ConnectTimeout)
	}
	return
}

// Build and set the host's DSN property
func (p *PG) pgDSN() {
	p.DSN = ""
	if p.IP != "" {
		p.DSN += fmt.Sprintf(` host=%s`, p.IP)
	}
	if p.Port != "" {
		p.DSN += fmt.Sprintf(` port=%s`, p.Port)
	}
	if p.User != "" {
		p.DSN += fmt.Sprintf(` user=%s`, p.User)
	}
	if p.Pass != "" {
		p.DSN += fmt.Sprintf(` password=%s`, p.Pass)
	}
	if p.Database != "" {
		p.DSN += fmt.Sprintf(` dbname=%s`, p.Database)
	}
	if p.ConnectTimeout != "" {
		p.DSN += fmt.Sprintf(` connect_timeout=%s`, p.ConnectTimeout)
	}
	p.DSN += fmt.Sprintf(` fallback_application_name=rdpgd sslmode=%s`, p.SSLMode)
	return
}

// Connect to the host's database and return database connection object if successful
func (p *PG) Connect() (db *sqlx.DB, err error) {
	db, err = sqlx.Connect(`postgres`, p.URI)
	if err != nil {
		log.Error(fmt.Sprintf(`pg.PG<%s>#Connect() %+v ! %s`, p.IP, p.URI, err))
		return db, err
	}
	return db, nil
}

func (p *PG) WaitForRegClass(k string) (err error) {
	log.Trace(fmt.Sprintf(`pg.PG<%s>#WaitForRegClass(%s) %+v`, p.IP, k, p.URI))
	db, err := p.Connect()
	if err != nil {
		log.Error(fmt.Sprintf("pg.PG<%s>#WaitForRegClass(%s) Failed connecting to %s err: %s", p.IP, k, p.URI, err))
		return err
	}
	defer db.Close()

	for { // TODO: Max Attempts.
		names := []string{}
		sq := fmt.Sprintf(`SELECT to_regclass('%s') AS name`, k)
		err := db.Select(&names, sq)
		if err != nil {
			log.Trace(fmt.Sprintf(`pg.PG<%s>#WaitForRegClass(%s) ! %s`, p.IP, k, err))
			time.Sleep(3 * time.Second)
			continue
		}
		log.Trace(fmt.Sprintf(`pg.PG<%s>#WaitForRegClass(%s) names: %+v`, p.IP, k, names))
		if len(names[0]) == 0 {
			time.Sleep(3 * time.Second)
			continue
		} else {
			break
		}
	}
	return nil
}

func (p *PG) DisableDatabase(dbname string) (err error) {
	log.Trace(fmt.Sprintf(`pg.PG<%s>#DisableDatabase(%s) Disabling database...`, p.IP, dbname))

	p.Set(`database`, `postgres`)
	db, err := p.Connect()
	if err != nil {
		log.Error(fmt.Sprintf("pg.PG<%s>#DisableDatabase(%s) %s ! %s", p.IP, dbname, p.URI, err))
		return
	}

	sq := fmt.Sprintf(`SELECT rdpg.disable_database('%s');`, dbname)
	log.Trace(fmt.Sprintf(`pg.PG<%s>#DisableDatabase(%s) SQL > %s`, p.IP, dbname, sq))
	_, err = db.Exec(sq)
	if err != nil {
		log.Error(fmt.Sprintf("pg.PG<%s>#DisableDatabase(%s) SQL ! %s", p.IP, dbname, err))
	}
	db.Close()

	return
}
