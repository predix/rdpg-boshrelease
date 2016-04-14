package rdpg

import (
	"database/sql"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	consulapi "github.com/hashicorp/consul/api"
	"github.com/jmoiron/sqlx"

	"github.com/starkandwayne/rdpgd/globals"
	"github.com/starkandwayne/rdpgd/log"
	"github.com/starkandwayne/rdpgd/pg"
	"github.com/starkandwayne/rdpgd/services"
)

var (
	bootstrapLock   *consulapi.Lock
	bootstrapLockCh <-chan struct{}
	bdrJoinIP       string
)

// Bootstrap the RDPG Database and associated services.
func Bootstrap() (err error) {
	r := newRDPG()
	log.Info(fmt.Sprintf(`rdpg.RDPG<%s>#Bootstrap() Bootstrapping Cluster Node...`, ClusterID))
	err = r.initialBootstrap()
	if err != nil {
		log.Error(fmt.Sprintf(`rdpg.RDPG<%s>#Bootstrap() r.initialBootstrap() ! %s`, ClusterID, err))
		return
	}
	// Record clusterService in consul
	kv := r.ConsulClient.KV()
	key := fmt.Sprintf(`rdpg/%s/cluster/service`, ClusterID)
	kvp := &consulapi.KVPair{Key: key, Value: []byte(globals.ClusterService)}
	_, err = kv.Put(kvp, &consulapi.WriteOptions{})
	if err != nil {
		log.Error(fmt.Sprintf(`rdpg.RDPG<%s>#BootStrap(): key=%s globals.ClusterService=%s ! %s`, ClusterID, key, globals.ClusterService, err))
	}

	s, err := services.NewService(globals.ClusterService) // postgresql or pgbdr
	err = s.Configure()
	if err != nil {
		log.Error(fmt.Sprintf(`rdpg.RDPG<%s>#Bootstrap() s.Configure(%s) ! %s`, ClusterID, globals.ClusterService, err))
	}

	if globals.ClusterService == "pgbdr" {
		r.bdrBootstrap()
	} else { // TODO: This will be a switch statement when we have more than 2 service types.

		err = r.serviceClusterCapacityStore()
		if err != nil {
			log.Error(fmt.Sprintf(`rdpg.RDPG<%s>#bootstrapSystem() Store Service CLuster Instance Capacity in Consul KeyValue! %s`, ClusterID, err))
			return
		}

		err = r.bootstrapSystem()
		if err != nil {
			log.Error(fmt.Sprintf(`rdpg.RDPG<%s>#bdrLeaderBootstrap() r.bootstrapSystem(%s,%s) ! %s`, ClusterID, globals.ServiceRole, globals.ClusterService, err))
			return
		}
	}

	svcs := []string{`pgbouncer`, `haproxy`}
	for index := range svcs {
		s, err := services.NewService(svcs[index])
		err = s.Configure()
		if err != nil {
			log.Error(fmt.Sprintf(`rdpg.RDPG<%s>#Bootstrap() s.Configure(%s) ! %s`, ClusterID, svcs[index], err))
		}
	}

	err = r.registerConsulServices()
	if err != nil {
		log.Error(fmt.Sprintf(`rdpg.RDPG<%s>#Bootstrap() r.registerConsulServices() ! %s`, ClusterID, err))
	}

	err = r.registerConsulWatches()
	if err != nil {
		log.Error(fmt.Sprintf(`rdpg.RDPG<%s>#Bootstrap() r.registerConsulWatches() ! %s`, ClusterID, err))
	}

	log.Trace(fmt.Sprintf(`rdpg.RDPG<%s>#Bootstrap() Bootstrapping Cluster Node Completed.`, ClusterID))
	return
}

// General Boostrapping that should occur on every node irrespective of role/leader.
func (r *RDPG) initialBootstrap() (err error) {
	log.Trace(fmt.Sprintf(`rdpg.RDPG<%s>#initialBootstrap() Beginning general Bootstrapping...`, ClusterID))

	// TODO: Record somehow that general bootstrap was completed and do not re-run.
	p := pg.NewPG(`127.0.0.1`, pgPort, `postgres`, `postgres`, ``)
	err = p.CreateUser(`rdpg`, pgPass)
	if err != nil {
		log.Error(fmt.Sprintf(`r.RDPG<%s>#initialBootstrap() CreateUser(rdpg) ! %s`, ClusterID, err))
		return
	}

	err = p.CreateUser(`health`, `check`)
	if err != nil {
		log.Error(fmt.Sprintf(`r.RDPG<%s>#initialBootstrap() CreateUser(health) ! %s`, ClusterID, err))
		return
	}
	// TODO: ALTER USER health SET default_transaction_read_only=on;

	priviliges := []string{`SUPERUSER`, `CREATEDB`, `CREATEROLE`, `INHERIT`}
	err = p.GrantUserPrivileges(`rdpg`, priviliges)
	if err != nil {
		log.Error(fmt.Sprintf(`rdpg.RDPG<%s>#initialBootstrap() p.GrantUserPrivileges(rdpg,...) ! %s`, ClusterID, err))
		return
	}

	err = p.CreateDatabase(`rdpg`, `rdpg`)
	if err != nil {
		log.Error(fmt.Sprintf(`rdpg.RDPG<%s>#initialBootstrapping() CreateDatabase() ! %s`, ClusterID, err))
		return
	}

	err = p.CreateDatabase(`health`, `health`)
	if err != nil {
		log.Error(fmt.Sprintf(`rdpg.RDPG<%s>#initialBootstrapping() CreateDatabase() ! %s`, ClusterID, err))
		return
	}
	exts := []string{`btree_gist`, `pg_trgm`}
	if globals.ClusterService == "pgbdr" {
		exts = append(exts, `bdr`)
	}
	err = p.CreateExtensions(`rdpg`, exts)
	if err != nil {
		log.Error(fmt.Sprintf(`rdpg.RDPG<%s>#initialBootstrap() CreateExtensions() ! %s`, ClusterID, err))
		return
	}

	err = r.Register()
	if err != nil {
		log.Error(fmt.Sprintf(`rdpg.RDPG<%s>#initialBootstrap() Register() ! %s`, ClusterID, err))
		return
	}
	if globals.ClusterService == "pgbdr" {
		err = r.waitForClusterNodes()
		if err != nil {
			log.Error(fmt.Sprintf(`rdpg.RDPG<%s>#initialBootstrap() r.waitForClusterNodes() ! %s`, ClusterID, err))
			return
		}
	}
	// TODO: Record somehow that general bootstrap was completed and do not re-run.

	return
}

// Attempt to obtain a lock on the boostrap leader and bootstrap the cluster if we get the lock..
func (r *RDPG) bootstrapLock() (locked bool, err error) {
	const MAX_ACQUIRE_ATTEMPTS int = 10
	const TIME_BETWEEN_ATTEMPTS time.Duration = 5 * time.Second
	var numAcquireAttempts = 0
	key := fmt.Sprintf(`rdpg/%s/bootstrap/lock`, ClusterID)
	log.Trace(fmt.Sprintf(`rdpg.RDPG<%s>#bootstrapLock() Attempting to acquire boostrap leader lock /%s...`, ClusterID, key))
	locked = false
	bootstrapLock, err = r.ConsulClient.LockKey(key)
	if err != nil {
		log.Error(fmt.Sprintf(`rdpg.RDPG<%s>#bootstrapLock() LockKey() Error Locking Bootstrap Key %s ! %s`, ClusterID, key, err))
		return
	}
tryLockingBootstrap:
	bootstrapLockCh, err := bootstrapLock.Lock(nil)
	if err != nil {
		log.Error(fmt.Sprintf(`rdpg.RDPG<%s>#bootstrapLock() Lock() Attempt: %d, Error Acquiring Bootstrap Key lock %s ! %s`, ClusterID, numAcquireAttempts, key, err))
		if numAcquireAttempts < MAX_ACQUIRE_ATTEMPTS {
			numAcquireAttempts++
			time.Sleep(TIME_BETWEEN_ATTEMPTS)
			goto tryLockingBootstrap
		}
		return
	}
	if bootstrapLockCh == nil {
		log.Error(fmt.Sprintf(`rdpg.RDPG<%s>#bootstrapLock() Bootstrap Lock not aquired, halting bootstrap.`, ClusterID))
		return
	}
	locked = true
	return
}

// Unlock the bootstrap leader lock
func (r *RDPG) bootstrapUnlock() (err error) {
	if bootstrapLock != nil {
		log.Trace(fmt.Sprintf(`rdpg.RDPG<%s>#bootstrapUnlock() Unlocking bootstrap leader lock for cluster...`, ClusterID))
		bootstrapLock.Unlock()
	}
	return
}

func (r *RDPG) bootstrapSystem() (err error) {
	p := pg.NewPG(`127.0.0.1`, pgPort, `rdpg`, `rdpg`, pgPass)
	exts := []string{`pgcrypto`, `pg_stat_statements`, `uuid-ossp`, `hstore`, `pg_trgm`}
	err = p.CreateExtensions(`rdpg`, exts)
	if err != nil {
		log.Error(fmt.Sprintf(`rdpg.RDPG<%s>#initialBootstrap() CreateExtensions() ! %s`, ClusterID, err))
		return
	}

	err = r.InitSchema()
	if err != nil {
		log.Error(fmt.Sprintf(`rdpg.RDPG<%s>#bootstrapSystem() r.InitSchema(%s) ! %s`, ClusterID, globals.ServiceRole, err))
		return
	}

	cluster, err := NewCluster(ClusterID, r.ConsulClient)
	if err != nil {
		log.Error(fmt.Sprintf(`rdpg.RDPG<%s>#bootstrapSystem(%s) NewCluster() ! %s`, ClusterID, globals.ServiceRole, err))
		return err
	}
	err = cluster.SetWriteMaster(globals.MyIP)
	if err != nil {
		log.Error(fmt.Sprintf(`rdpg.RDPG<%s>#bootstrapSystem() SetWriteMaster() ! %s`, ClusterID, err))
		return
	}

	return
}

// Leader specific bootstrapping.
func (r *RDPG) bdrLeaderBootstrap() (err error) {
	log.Trace(fmt.Sprintf(`rdpg.RDPG<%s>#bdrLeaderBootstrap() bootstrapping leader for cluster...`, ClusterID))

	err = r.serviceClusterCapacityStore()
	if err != nil {
		log.Error(fmt.Sprintf(`rdpg.RDPG<%s>#bootstrapSystem() Store Service CLuster Instance Capacity in Consul KeyValue! %s`, ClusterID, err))
		return
	}

	err = r.bdrGroupCreate()
	if err != nil {
		log.Error(fmt.Sprintf(`rdpg.RDPG<%s>#bdrLeaderBootstrap() Error Creating BDR Group ! %s`, ClusterID, err))
	}

	r.bootstrapUnlock()

	err = r.waitForBDRNodes()
	if err != nil {
		log.Error(fmt.Sprintf(`rdpg.RDPG<%s>#bdrLeaderBootstrap() Waiting for BDR Nodes ! %s`, ClusterID, err))
		return
	}

	err = r.bootstrapSystem()
	if err != nil {
		log.Error(fmt.Sprintf(`rdpg.RDPG<%s>#bdrLeaderBootstrap() r.bootstrapSystem(%s,%s) ! %s`, ClusterID, globals.ServiceRole, globals.ClusterService, err))
		return
	}

	// TODO: Write a Consul Key /rdpg/%s/schema/initialized indicating completion.
	// Wait for this value on the non-leader nodes before they start.

	return
}

// Non-Leader specifc bootstrapping.
func (r *RDPG) bdrNonLeaderBootstrap() (err error) {
	log.Trace(fmt.Sprintf(`rdpg.RDPG<%s>#bdrNonLeaderBootstrap() bootstrapping non-leader...`, ClusterID))
	err = r.bdrGroupJoin()
	if err != nil {
		log.Error(fmt.Sprintf(`rdpg.RDPG<%s>#bdrNonLeaderBootstrap() bdrGroupJoin() ! %s`, ClusterID, err))
		r.bootstrapUnlock()
		return err // BDR join during bootstrap is critical path, unlock and exit.
	}
	r.bootstrapUnlock()

	err = r.waitForBDRNodes()
	if err != nil {
		log.Error(fmt.Sprintf(`rdpg.RDPG<%s>#bdrNonLeaderBootstrap() r.waitForBDRNodes() ! %s`, ClusterID, err))
	}

	p := pg.NewPG(`127.0.0.1`, pgPort, `rdpg`, `rdpg`, pgPass)
	db, err := p.Connect()
	if err != nil {
		log.Error(fmt.Sprintf("rdpg.RDPG<%s>#bdrNonLeaderBootstrap() ! %s", ClusterID, err))
		return
	}
	defer db.Close()

	err = p.WaitForRegClass("cfsb.instances")
	if err != nil {
		log.Error(fmt.Sprintf(`rdpg.RDPG<%s>#bdrNonLeaderBootstrap() p.WaitForRegClass() ! %s`, ClusterID, err))
	}

	err = r.waitForWriteMasterIP()
	if err != nil {
		log.Error(fmt.Sprintf(`rdpg.RDPG<%s>#bdrNonLeaderBootstrap() p.waitForWriteMasterIP() ! %s`, ClusterID, err))
	}
	return
}

// Create BDR Group
func (r *RDPG) bdrGroupCreate() (err error) {
	log.Trace(fmt.Sprintf(`rdpg.RDPG<%s>#bdrGroupCreate() Creating BDR Group rdpg for cluster...`, ClusterID))
	kv := r.ConsulClient.KV()
	key := fmt.Sprintf(`rdpg/%s/bdr/join/ip`, ClusterID)
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
		log.Trace(fmt.Sprintf(`rdpg.RDPG<%s>#bdrGroupCreate() Skipping creating BDR Group rdpg for cluster, it appears it was already done.`, ClusterID))
		return
	}
	// BDR Group not created yet, create and log globals.MyIP
	p := pg.NewPG(globals.MyIP, pgPort, `rdpg`, `rdpg`, pgPass)
	re := regexp.MustCompile(`[^0-9]+`)
	ip := strings.ToLower(string(re.ReplaceAll([]byte(globals.MyIP), []byte("_"))))
	localNodeName := fmt.Sprintf(`rdpg_%s`, ip)
	err = p.BDRGroupCreate(localNodeName, `rdpg`)
	if err != nil {
		log.Error(fmt.Sprintf(`rdpg.RDPG<%s>#bdrGroupCreate() Error creating BDR Group rdpg for cluster ! %s`, ClusterID, err))
		return
	}
	log.Trace(fmt.Sprintf(`rdpg.RDPG<%s>#bdrGroupCreate() Recording BDR Join IP for cluster > %s`, ClusterID, key))
	kvp = &consulapi.KVPair{Key: key, Value: []byte(globals.MyIP)}
	_, err = kv.Put(kvp, &consulapi.WriteOptions{})
	if err != nil {
		log.Error(fmt.Sprintf(`rdpg.RDPG<%s>#bdrGroupCreate() ! %s`, ClusterID, err))
		return
	}
	return
}

// Join BDR Group
func (r *RDPG) bdrGroupJoin() (err error) {
	log.Trace(fmt.Sprintf(`rdpg.RDPG<%s>#bdrGroupJoin() Joining BDR Group rdpg for cluster...`, ClusterID))
	p := pg.NewPG(globals.MyIP, pgPort, `rdpg`, `rdpg`, pgPass)
	joinPG := pg.NewPG(bdrJoinIP, pgPort, `rdpg`, `rdpg`, pgPass)
	re := regexp.MustCompile(`[^0-9]+`)
	ip := strings.ToLower(string(re.ReplaceAll([]byte(globals.MyIP), []byte("_"))))
	localNodeName := fmt.Sprintf(`rdpg_%s`, ip)
	err = p.BDRGroupJoin(localNodeName, `rdpg`, *joinPG)
	if err != nil {
		log.Error(fmt.Sprintf(`rdpg.RDPG<%s>#bdrGroupJoin(%s,rdpg) {HINT: Check pgbdr logs and pg_hba.conf} ! %s`, ClusterID, localNodeName, err))
		return
	}
	return
}

func (r *RDPG) serviceClusterCapacityStore() (err error) {
	if ClusterID == "rdpgmc" {
		return nil
	}
	instanceAllowed := os.Getenv(`RDPGD_INSTANCE_ALLOWED`)
	if instanceAllowed == "" {
		log.Error(fmt.Sprintf(`rdpg.RDPG<%s>#serviceClusterCapacityStore(): instanceAllowed is not configured or failed to export as environment variable! %s`, ClusterID))
	}
	instanceLimit := os.Getenv(`RDPGD_INSTANCE_LIMIT`)
	if instanceLimit == "" {
		log.Error(fmt.Sprintf(`rdpg.RDPG<%s>#serviceClusterCapacityStore(): instanceLimit is not configured or failed to export as environment variable! %s`, ClusterID))
	}
	kv := r.ConsulClient.KV()

	key := fmt.Sprintf(`rdpg/%s/capacity/instances/limit`, ClusterID)
	kvp := &consulapi.KVPair{Key: key, Value: []byte(instanceLimit)}
	_, err = kv.Put(kvp, &consulapi.WriteOptions{})
	if err != nil {
		log.Error(fmt.Sprintf(`rdpg.RDPG<%s>#serviceClusterCapacityStore(): instanceLimit ! %s`, ClusterID, err))
	}

	key = fmt.Sprintf(`rdpg/%s/capacity/instances/allowed`, ClusterID)
	kvp.Key = key
	kvp.Value = []byte(instanceAllowed)
	_, err = kv.Put(kvp, &consulapi.WriteOptions{})
	if err != nil {
		log.Error(fmt.Sprintf(`rdpg.RDPG<%s>#serviceClusterCapacityStore(): instanceAloowed ! %s`, ClusterID, err))
	}

	return
}

func (r *RDPG) getValue(key string) (val string, err error) {
	val = ""
	kv := r.ConsulClient.KV()
	kvp, _, err := kv.Get(key, nil)
	if err != nil {
		log.Error(fmt.Sprintf(`rdpg.RDPG<%s>#getValue() kv.Get(%s) ! %s`, ClusterID, key, err))
		return
	}
	if kvp == nil {
		log.Trace(fmt.Sprintf(`rdpg.RDPG<%s>#getKey() kv.Get(%s) Key is not set...`, ClusterID, key))
		return
	}
	val = string(kvp.Value)
	return
}

func (r *RDPG) waitForClusterNodes() (err error) {
	log.Trace(fmt.Sprintf(`rdpg.RDPG<%s>#waitForClusterNodes() waiting for all nodes to be registered as Consul services...`, ClusterID))
	cluster, err := NewCluster(ClusterID, r.ConsulClient)
	if err != nil {
		log.Error(fmt.Sprintf(`rdpg.RDPG<%s>#waitForClusterNodes() NewCluster() ! %s`, ClusterID, err))
		return err
	}
	for {
		ips, err := cluster.ClusterIPs()
		if err != nil {
			log.Error(fmt.Sprintf(`rdpg.RDPG<%s>#waitForClusterNodes() cluster.ClusterIPs() ! %s`, ClusterID, err))
			return err
		}
		switch ClusterID {
		case `rdpgmc`:
			if len(ips) > 2 {
				return nil
			}
		default: // rdpgsc*
			if len(ips) > 1 {
				return nil
			}
		}
		time.Sleep(2 * time.Second)
	}
}

func (r *RDPG) waitForBDRNodes() (err error) {
	log.Trace(fmt.Sprintf(`rdpg.RDPG<%s>#waitForBDRNodes() waiting for all BDR nodes to be joined...`, ClusterID))
	p := pg.NewPG(`127.0.0.1`, pgPort, `rdpg`, `rdpg`, pgPass)
	var db *sqlx.DB
	for {
		db, err = p.Connect()
		if err != nil {
			re := regexp.MustCompile("canceling statement due to conflict with recovery")
			if re.MatchString(err.Error()) {
				log.Error(fmt.Sprintf("rdpg.RDPG<%s>#waitForBDRNodes() p.Connect() (sleeping 2 seconds and trying again) ! %s", ClusterID, err))
				time.Sleep(2 * time.Second)
				continue // Sleep 2 seconds and try again...
			} else {
				log.Error(fmt.Sprintf("rdpg.RDPG<%s>#waitForBDRNodes() p.Connect() ! %s", ClusterID, err))
				return err
			}
		} else {
			break
		}
	}
	defer db.Close()

	for {
		nodes := []string{}
		sq := `SELECT node_name FROM bdr.bdr_nodes;`
		err = db.Select(&nodes, sq)
		if err != nil {
			if err == sql.ErrNoRows {
				log.Error(fmt.Sprintf("rdpg.RDPG<%s>#waitForBDRNodes() db.Select() %sq ! Sleeping 2 seconds and trying again.", ClusterID, sq))
				time.Sleep(2 * time.Second)
				continue
			}
			log.Error(fmt.Sprintf("rdpg.RDPG<%s>#waitForBDRNodes() db.Select() ! %s", ClusterID, err))
			return err
		}
		log.Trace(fmt.Sprintf(`rdpg.RDPG<%s>#waitForBDRNodes() nodes %+v`, ClusterID, nodes))
		switch ClusterID {
		case "rdpgmc":
			if len(nodes) > 2 {
				return nil
			}
		default: // rdpgsc*
			if len(nodes) > 1 {
				return nil
			}
		}
		time.Sleep(2 * time.Second)
	}
}

func (r *RDPG) registerConsulServices() (err error) {
	log.Trace(fmt.Sprintf(`rdpg.RDPG<%s>#registerConsulServices() Registering Consul Services...`, ClusterID))

	re := regexp.MustCompile(`^(rdpg(sc[0-9]+$))|(sc-([[:alnum:]|-])*m[0-9]+-c[0-9]+$)`)
	if !re.MatchString(ClusterID) {
		log.Trace(fmt.Sprintf(`rdpg.RDPG<%s>#registerConsulServices() Not a service cluster, skipping consul service registration.`, ClusterID))
		return
	}

	agent := r.ConsulClient.Agent()

	agent.ServiceRegister(&consulapi.AgentServiceRegistration{
		ID:   fmt.Sprintf("%s-haproxy", ClusterID),
		Name: "haproxy",
		Tags: []string{},
		Port: 5432, // TODO: Get write port from environment configuration.
		Check: &consulapi.AgentServiceCheck{
			HTTP:     fmt.Sprintf(`http://%s:%s@127.0.0.1:%s/health/ha_pb_pg`, rdpgdAdminUser, rdpgdAdminPass, rdpgdAdminPort),
			Interval: "10s",
			TTL:      "0s",
			Timeout:  "5s",
		},
	})

	agent.ServiceRegister(&consulapi.AgentServiceRegistration{
		ID:   fmt.Sprintf("%s-pgbouncer", ClusterID),
		Name: "pgbouncer",
		Tags: []string{},
		Port: 6432, // TODO: Get pgbouncer port from environment configuration.
		Check: &consulapi.AgentServiceCheck{
			HTTP:     fmt.Sprintf(`http://%s:%s@127.0.0.1:%s/health/pb`, rdpgdAdminUser, rdpgdAdminPass, rdpgdAdminPort),
			Interval: "10s",
			TTL:      "0s",
			Timeout:  "5s",
		},
	})

	agent.ServiceRegister(&consulapi.AgentServiceRegistration{
		ID:   fmt.Sprintf("%s-postgres", ClusterID),
		Name: "postgres",
		Tags: []string{},
		Port: 7432, // TODO: Get write port from environment configuration.
		Check: &consulapi.AgentServiceCheck{
			HTTP:     fmt.Sprintf(`http://%s:%s@127.0.0.1:%s/health/pg`, rdpgdAdminUser, rdpgdAdminPass, rdpgdAdminPort),
			Interval: "10s",
			TTL:      "0s",
			Timeout:  "5s",
		},
	})
	return
}

func (r *RDPG) registerConsulWatches() (err error) {
	log.Info(`rdpg.RDPG#registerConsulWatches() TODO: Registering Consul Watches...`)
	/*
	   "type": "service", "service": "haproxy", "handler": "/var/vcap/jobs/rdpgd-service/bin/consul-watch-notification"
	   "type": "service", "service": "postgres", "handler": "/var/vcap/jobs/rdpgd-service/bin/consul-watch-notification"
	*/
	return
}

func (r *RDPG) waitForWriteMasterIP() (err error) {
	log.Trace(fmt.Sprintf(`rdpg.RDPG<%s>#waitForWriteMasterIP() Waiting for Master IP to be set in Consul...`, ClusterID))
	cluster, err := NewCluster(ClusterID, r.ConsulClient)
	if err != nil {
		log.Error(fmt.Sprintf(`rdpg.RDPG<%s>#waitForWriteMasterIP() NewCluster() ! %s`, ClusterID, err))
		return err
	}
	for {
		n, err := cluster.GetWriteMaster()
		if err != nil {
			log.Error(fmt.Sprintf(`rdpg.RDPG<%s>#waitForWriteMasterIP() GetWriteMaster() ! %s`, ClusterID, err))
			return err
		}
		if n == nil {
			time.Sleep(1 * time.Second)
			continue
		}
		break
	}
	return
}

func (r *RDPG) bdrBootstrap() (err error) {
	_, err = r.bootstrapLock()
	if err != nil {
		log.Error(fmt.Sprintf(`rdpg.RDPG<%s>#Bootstrap() r.bootstrapLock() ! %s`, ClusterID, err))
		return
	}

	leader := false
	key := fmt.Sprintf(`rdpg/%s/bdr/join/ip`, ClusterID)
	bdrJoinIP, err = r.getValue(key)
	if err != nil {
		log.Error(fmt.Sprintf(`rdpg.RDPG<%s>#Bootstrap() kv.getValue(%s) ! %s ...`, ClusterID, key, err))
		return err
	}
	if len(bdrJoinIP) == 0 || bdrJoinIP == globals.MyIP {
		log.Trace(fmt.Sprintf(`rdpg.RDPG<%s>#Bootstrap() kv.getValue(%s) BDR Join IP has not been set`, ClusterID, key))
		leader = true
	} else {
		log.Trace(fmt.Sprintf(`rdpg.RDPG<%s>#Bootstrap() kv.getValue(%s) BDR Join Node IP has been set to %s`, ClusterID, key, bdrJoinIP))
		leader = false
	}

	if leader {
		err = r.bdrLeaderBootstrap()
		if err != nil {
			log.Error(fmt.Sprintf(`rdpg.RDPG<%s>#Bootstrap() r.bdrLeaderBootstrap() ! %s`, ClusterID, err))
		}
	} else {
		err = r.bdrNonLeaderBootstrap()
		if err != nil {
			log.Error(fmt.Sprintf(`rdpg.RDPG<%s>#Bootstrap() r.bdrNonLeaderBootstrap() ! %s`, ClusterID, err))
		}
	}
	return
}
