package tasks

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/starkandwayne/rdpgd/globals"

	consulapi "github.com/hashicorp/consul/api"

	"github.com/starkandwayne/rdpgd/bdr"
	"github.com/starkandwayne/rdpgd/instances"
	"github.com/starkandwayne/rdpgd/log"
	"github.com/starkandwayne/rdpgd/pg"
	"github.com/starkandwayne/rdpgd/uuid"
)

/*
PrecreateDatabases is called as a scheduled task for precreating databaes.
*/
func (t *Task) PrecreateDatabases() (err error) {
	if globals.ServiceRole != "service" { //Safety valve...
		log.Error(fmt.Sprintf("tasks.Task#PrecreateDatabases() ! Not precreating databases as we are not running on a service node..."))
		return
	}
	t.ClusterID = os.Getenv(`RDPGD_CLUSTER`)
	if t.ClusterID == "" {
		matrixName := os.Getenv(`RDPGD_MATRIX`)
		matrixNameSplit := strings.SplitAfterN(matrixName, `-`, -1)
		matrixColumn := os.Getenv(`RDPGD_MATRIX_COLUMN`)
		for i := 0; i < len(matrixNameSplit)-1; i++ {
			t.ClusterID = t.ClusterID + matrixNameSplit[i]
		}
		t.ClusterID = t.ClusterID + "c" + matrixColumn
	}

	client, err := consulapi.NewClient(consulapi.DefaultConfig())
	if err != nil {
		log.Error(fmt.Sprintf("tasks.Task#PrecreateDatabases() ! %s", err))
		return
	}
	// Lock Database Creation (and Deletion) via Consul Lock
	key := fmt.Sprintf(`rdpg/%s/database/existance/lock`, t.ClusterID)
	lo := &consulapi.LockOptions{
		Key:         key,
		SessionName: fmt.Sprintf(`rdpg/%s/databases/existance`, t.ClusterID),
	}
	log.Trace(fmt.Sprintf(`tasks.Task<%s>#PrecreateDatabases() Attempting to acquire database existance lock %s...`, t.ClusterID, key))
	databaseCreateLock, err := client.LockOpts(lo)
	if err != nil {
		log.Error(fmt.Sprintf(`tasks.Task<%s>#PrecreateDatabases() LockKey() Error locking database existance Key %s ! %s`, t.ClusterID, key, err))
		return
	}
	databaseCreateLockCh, err := databaseCreateLock.Lock(nil)
	if err != nil {
		log.Error(fmt.Sprintf(`tasks.Task<%s>#PrecreateDatabases() Lock() database/existance lock %s ! %s`, t.ClusterID, key, err))
		return
	}
	defer databaseCreateLock.Unlock()
	if databaseCreateLockCh == nil {
		log.Error(fmt.Sprintf(`tasks.Task<%s>#PrecreateDatabases() database/existance Lock not aquired, halting Creation!!!`, t.ClusterID))
		return
	}

	// We have the database existance lock...
	key = fmt.Sprintf(`rdpg/%s/capacity/instances/limit`, t.ClusterID)
	kv := client.KV()
	kvp, _, err := kv.Get(key, nil)
	if err != nil {
		log.Error(fmt.Sprintf(`tasks.Task<%s>#PrecreateDatabases() kv.Get(%s) ! %s`, t.ClusterID, key, err))
		return
	}
	if kvp == nil {
		log.Trace(fmt.Sprintf(`tasks.Task<%s>#PrecreateDatabases() kv.Get(%s) : key is not set...`, t.ClusterID, key))
		return
	}
	maxLimitString := string(kvp.Value)
	maxLimit, err := strconv.Atoi(maxLimitString)
	if err != nil {
		log.Error(fmt.Sprintf(`tasks.Task<%s>#PrecreateDatabases() strconv.Atoi(%s) (Limit)`, t.ClusterID, kvp.Value))
		return
	}
	log.Trace(fmt.Sprintf(`tasks.Task<%s>#PrecreateDatabases() retrived max_instances_limit: %d`, t.ClusterID, maxLimit))

	key = fmt.Sprintf(`rdpg/%s/capacity/instances/allowed`, t.ClusterID)
	kvp, _, err = kv.Get(key, nil)
	if err != nil {
		log.Error(fmt.Sprintf(`tasks.Task<%s>#PrecreateDatabases() kvp.Get(%s) ! %s`, t.ClusterID, key, err))
		return
	}
	if kvp == nil {
		log.Trace(fmt.Sprintf(`tasks.Task<%s>#PrecreateDatabases() kv.Get(%s) : key is not set...`, t.ClusterID, key))
		return
	}
	maxAllowedString := string(kvp.Value)
	maxAllowed, err := strconv.Atoi(maxAllowedString)
	if err != nil {
		log.Error(fmt.Sprintf(`tasks.Task<%s>#PrecreateDatabases() strconv.Atoi(%s) (Allowed)`, t.ClusterID, kvp.Value))
		return
	}
	log.Trace(fmt.Sprintf(`tasks.Task<%s>#PrecreateDatabases() retrived max_instances_allowed: %d`, t.ClusterID, maxAllowed))

	maxCapacity := maxAllowed
	if maxLimit < maxCapacity {
		maxCapacity = maxLimit
	}

	i, err := instances.Available()
	if err != nil {
		log.Error(fmt.Sprintf("tasks.Task<%s>#PrecreateDatabases() instances.Available()! %s", t.ClusterID, err))
		return
	}
	numAvailable := len(i)
	//i, err = instances.All()
	i, err = instances.Undecommissioned()
	if err != nil {
		log.Error(fmt.Sprintf("tasks.Task<%s>#PrecreateDatabases() instances.Undecommissioned() ! %s", t.ClusterID, err))
		return
	}
	numInstances := len(i)
	totalNeeded := poolSize - numAvailable

	if len(t.Data) != 0 { // Admin creating a database.
		// In this case we were called with a number to precreate, such as from
		// admin api "precreate 100":  for 1 .. N TODO: admin API endpoint for this
		n, err := strconv.Atoi(t.Data)
		if err != nil {
			log.Error(fmt.Sprintf("tasks.Task#PrecreateDatabases() strconv.Atoi(%s) (t.Data) ! %s", t.Data, err))
		} else {
			totalNeeded = n // TODO: Account for maxAllowed
		}
	}

	log.Trace(fmt.Sprintf("tasks.Task#PrecreateDatabases() The total databases which need to be Precreated is %d, based on there being %d instances already, %d unused databases, a desired pool size of %d and a maximum capacity of %d...", totalNeeded, numInstances, numAvailable, poolSize, maxCapacity))

	if totalNeeded > 0 {
		totalCreated := numInstances
		for index := 0; (index < totalNeeded) && (totalCreated < maxCapacity); index++ {
			log.Trace(fmt.Sprintf("tasks.Task#PrecreateDatabases() Precreating a new database for cluster type %s...", t.ClusterService))
			if t.ClusterService == "pgbdr" {
				err = t.bdrPrecreateDatabase(client)
			} else if t.ClusterService == "postgresql" {
				err = t.postgresqlPrecreateDatabase(client)
			} else {
				log.Error(fmt.Sprintf("tasks.Task#PrecreateDatabases() Bad ClusterService for Precreate ! Attempted to determine the service cluster type and do not know how to handle type '%s' ", t.ClusterService))
				return
			}
			if err != nil {
				log.Error(fmt.Sprintf("tasks.Task#PrecreateDatabases() t.PrecreateDatabases() ! %s", err))
				return err
			}
			totalCreated++
		}
	}

	return
}

func (t *Task) postgresqlPrecreateDatabase(client *consulapi.Client) (err error) {
	re := regexp.MustCompile("[^A-Za-z0-9_]")
	u1 := uuid.NewUUID().String()
	u2 := uuid.NewUUID().String()
	identifier := strings.ToLower(string(re.ReplaceAll([]byte(u1), []byte(""))))
	dbpass := strings.ToLower(string(re.ReplaceAll([]byte(u2), []byte(""))))

	i := &instances.Instance{
		ClusterID:      ClusterID,
		Database:       "d" + identifier,
		User:           "u" + identifier,
		Pass:           dbpass,
		ClusterService: t.ClusterService,
	}
	// TODO: Keep the databases under rdpg schema, link to them in the
	// cfsb.instances table so that we separate the concerns of CF and databases.

	p := pg.NewPG(`127.0.0.1`, pgPort, `rdpg`, `rdpg`, pgPass)
	db, err := p.Connect()
	if err != nil {
		log.Error(fmt.Sprintf("tasks.Work() Failed connecting to %s err: %s", p.URI, err))
		return err
	}
	defer db.Close()

	err = p.CreateUser(i.User, i.Pass)
	if err != nil {
		log.Error(fmt.Sprintf("tasks.Task#postgresqlPrecreateDatabases(%s) CreateUser(%s) ! %s", i.Database, i.User, err))
		return err
	}

	err = p.CreateDatabase(i.Database, i.User)
	if err != nil {
		log.Error(fmt.Sprintf("tasks.Task#postgresqlPrecreateDatabases(%s) CreateDatabase(%s,%s) ! %s", i.Database, i.Database, i.User, err))
		return err
	}

	sq := fmt.Sprintf(`INSERT INTO cfsb.instances (cluster_id,dbname, dbuser, dbpass, cluster_service) VALUES ('%s','%s','%s','%s','%s')`, ClusterID, i.Database, i.User, i.Pass, t.ClusterService)
	log.Trace(fmt.Sprintf(`tasks.postgresqlPrecreateDatabase(%s) > %s`, i.Database, sq))
	_, err = db.Query(sq)
	if err != nil {
		log.Error(fmt.Sprintf(`tasks.postgresqlPrecreateDatabase(%s) ! %s`, i.Database, err))
		return err
	}

	err = p.CreateExtensions(i.Database, []string{`btree_gist`, `pg_stat_statements`, `uuid-ossp`, `hstore`, `pg_trgm`, `pgcrypto`})
	if err != nil {
		log.Error(fmt.Sprintf("tasks.Task#postgresqlPrecreateDatabases(%s) CreateExtensions(%s,%s) ! %s", i.Database, i.Database, i.User, err))
		return err
	}

	//Loop through and add any additional extensions specified in the rdpgd_service properties of the deployment manifest
	if len(globals.UserExtensions) > 1 {
		userExtensions := strings.Split(globals.UserExtensions, " ")
		err = p.CreateExtensions(i.Database, userExtensions)
		if err != nil {
			log.Error(fmt.Sprintf("tasks.Task#postgresqlPrecreateDatabases(%s) CreateExtensions(%s,%s) Creating Extra User Extensions ! %s", i.Database, i.Database, i.User, err))
			return err
		}
	}
	sq = fmt.Sprintf(`UPDATE cfsb.instances SET effective_at=CURRENT_TIMESTAMP WHERE dbname='%s'`, i.Database)
	log.Trace(fmt.Sprintf(`tasks.postgresqlPrecreateDatabase(%s) > %s`, i.Database, sq))
	_, err = db.Query(sq)
	if err != nil {
		log.Error(fmt.Sprintf(`tasks.postgresqlPrecreateDatabase(%s) ! %s`, i.Database, err))
		return err
	}

	// Tell the management cluster about the newly available database.
	// TODO: This can be in a function.
	catalog := client.Catalog()
	svcs, _, err := catalog.Service("rdpgmc", "", nil)
	if err != nil {
		log.Error(fmt.Sprintf("tasks.Task#postgresqlPrecreateDatabase(%s) catalog.Service() ! %s", i.Database, err))
		return err
	}
	if len(svcs) == 0 {
		log.Error(fmt.Sprintf("tasks.Task#postgresqlPrecreateDatabase(%s) ! No services found, no known nodes?!", i.Database))
		return err
	}
	body, err := json.Marshal(i)
	if err != nil {
		log.Error(fmt.Sprintf("tasks.Task#postgresqlPrecreateDatabase(%s) json.Marchal(i) ! %s", i.Database, err))
		return err
	}
	url := fmt.Sprintf("http://%s:%s/%s", svcs[0].Address, os.Getenv("RDPGD_ADMIN_PORT"), `databases/register`)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer([]byte(body)))
	if err != nil {
		log.Error(fmt.Sprintf(`tasks.Task#postgresqlPrecreateDatabase(%s) http.NewRequest() POST %s ! %s`, i.Database, url, err))
		return err
	}
	log.Trace(fmt.Sprintf(`tasks.Task#postgresqlPrecreateDatabase(%s) POST %s`, i.Database, url))
	req.SetBasicAuth(os.Getenv("RDPGD_ADMIN_USER"), os.Getenv("RDPGD_ADMIN_PASS"))
	httpClient := &http.Client{}
	resp, err := httpClient.Do(req)
	if err != nil {
		log.Error(fmt.Sprintf(`tasks.Task#postgresqlPrecreateDatabase(%s) httpClient.Do() %s ! %s`, i.Database, url, err))
		return err
	}
	resp.Body.Close()
	return
}

func (t *Task) bdrPrecreateDatabase(client *consulapi.Client) (err error) {
	/*
	   key := fmt.Sprintf(`rdpg/%s/cluster/service`, ClusterID)
	   kvp, _, err := kv.Get(key, nil)
	   if err != nil {
	   	log.Error(fmt.Sprintf(`rdpg.RDPG<%s>#bdrGroupCreate() kv.Get() ! %s`, ClusterID, err))
	   	return
	   }
	   v := ``
	   if kvp != nil {
	   	v = string(kvp.Value)
	   }
	   if len(v) > 0 {
	   	log.Trace(fmt.Sprintf(`rdpg.RDPG<%s>#bdrPrecreateDatabase()`, ClusterID))
	   	return
	   }
	*/
	b := bdr.NewBDR(t.ClusterID, client)
	re := regexp.MustCompile("[^A-Za-z0-9_]")
	u1 := uuid.NewUUID().String()
	u2 := uuid.NewUUID().String()
	identifier := strings.ToLower(string(re.ReplaceAll([]byte(u1), []byte(""))))
	dbpass := strings.ToLower(string(re.ReplaceAll([]byte(u2), []byte(""))))

	i := &instances.Instance{
		ClusterID:      ClusterID,
		Database:       "d" + identifier,
		User:           "u" + identifier,
		Pass:           dbpass,
		ClusterService: t.ClusterService,
	}
	// TODO: Keep the databases under rdpg schema, link to them in the
	// cfsb.instances table so that we separate the concerns of CF and databases.
	err = b.CreateUser(i.User, i.Pass)
	if err != nil {
		log.Error(fmt.Sprintf("tasks.Task#bdrPrecreateDatabases(%s) CreateUser(%s) ! %s", i.Database, i.User, err))
		return err
	}

	err = b.CreateDatabase(i.Database, i.User)
	if err != nil {
		log.Error(fmt.Sprintf("tasks.Task#bdrPrecreateDatabases(%s) CreateDatabase(%s,%s) ! %s", i.Database, i.Database, i.User, err))
		return err
	}

	p := pg.NewPG(`127.0.0.1`, pgPort, `rdpg`, `rdpg`, pgPass)
	db, err := p.Connect()
	if err != nil {
		log.Error(fmt.Sprintf("tasks.Work() Failed connecting to %s err: %s", p.URI, err))
		return err
	}
	defer db.Close()

	sq := fmt.Sprintf(`INSERT INTO cfsb.instances (cluster_id,dbname, dbuser, dbpass, cluster_service) VALUES ('%s','%s','%s','%s','%s')`, ClusterID, i.Database, i.User, i.Pass, t.ClusterService)
	log.Trace(fmt.Sprintf(`tasks.bdrPrecreateDatabase(%s) > %s`, i.Database, sq))
	_, err = db.Query(sq)
	if err != nil {
		log.Error(fmt.Sprintf(`tasks.bdrPrecreateDatabase(%s) ! %s`, i.Database, err))
		return err
	}

	err = b.CreateExtensions(i.Database, []string{`btree_gist`, `bdr`, `pg_stat_statements`, `uuid-ossp`, `hstore`, `pg_trgm`, `pgcrypto`})
	if err != nil {
		log.Error(fmt.Sprintf("tasks.Task#bdrPrecreateDatabases(%s) CreateExtensions(%s,%s) ! %s", i.Database, i.Database, i.User, err))
		return err
	}

	//Loop through and add any additional extensions specified in the rdpgd_service properties of the deployment manifest
	if len(globals.UserExtensions) > 1 {
		userExtensions := strings.Split(globals.UserExtensions, " ")
		err = b.CreateExtensions(i.Database, userExtensions)
		if err != nil {
			log.Error(fmt.Sprintf("tasks.Task#bdrPrecreateDatabases(%s) CreateExtensions(%s,%s) Creating Extra User Extensions ! %s", i.Database, i.Database, i.User, err))
			return err
		}
	}

	err = b.CreateReplicationGroup(i.Database)
	if err != nil {
		log.Error(fmt.Sprintf("tasks.Task#bdrPrecreateDatabases(%s) CreateReplicationGroup() ! %s", i.Database, err))
		return err
	}

	sq = fmt.Sprintf(`UPDATE cfsb.instances SET effective_at=CURRENT_TIMESTAMP WHERE dbname='%s'`, i.Database)
	log.Trace(fmt.Sprintf(`tasks.bdrPrecreateDatabase(%s) > %s`, i.Database, sq))
	_, err = db.Query(sq)
	if err != nil {
		log.Error(fmt.Sprintf(`tasks.bdrPrecreateDatabase(%s) ! %s`, i.Database, err))
		return err
	}

	// Tell the management cluster about the newly available database.
	// TODO: This can be in a function.
	catalog := client.Catalog()
	svcs, _, err := catalog.Service("rdpgmc", "", nil)
	if err != nil {
		log.Error(fmt.Sprintf("tasks.Task#bdrPrecreateDatabase(%s) catalog.Service() ! %s", i.Database, err))
		return err
	}
	if len(svcs) == 0 {
		log.Error(fmt.Sprintf("tasks.Task#bdrPrecreateDatabase(%s) ! No services found, no known nodes?!", i.Database))
		return err
	}
	body, err := json.Marshal(i)
	if err != nil {
		log.Error(fmt.Sprintf("tasks.Task#bdrPrecreateDatabase(%s) json.Marchal(i) ! %s", i.Database, err))
		return err
	}
	url := fmt.Sprintf("http://%s:%s/%s", svcs[0].Address, os.Getenv("RDPGD_ADMIN_PORT"), `databases/register`)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer([]byte(body)))
	if err != nil {
		log.Error(fmt.Sprintf(`tasks.Task#bdrPrecreateDatabase(%s) http.NewRequest() POST %s ! %s`, i.Database, url, err))
		return err
	}
	log.Trace(fmt.Sprintf(`tasks.Task#bdrPrecreateDatabase(%s) POST %s`, i.Database, url))
	req.SetBasicAuth(os.Getenv("RDPGD_ADMIN_USER"), os.Getenv("RDPGD_ADMIN_PASS"))
	httpClient := &http.Client{}
	resp, err := httpClient.Do(req)
	if err != nil {
		log.Error(fmt.Sprintf(`tasks.Task#bdrPrecreateDatabase(%s) httpClient.Do() %s ! %s`, i.Database, url, err))
		return err
	}
	resp.Body.Close()
	return
}
