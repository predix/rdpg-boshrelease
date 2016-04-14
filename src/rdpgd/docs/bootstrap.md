# Bootstrap

This process will bootstrap RDPG databases and services. It will process initial bootstrap for all the roles, then enter into leader bootstrap is leader is not set, otherwise it will go into non-leader bootstrap.The detailed of sequence diagram is shown as below.

![Bootstrap Diagram](https://github.com/starkandwayne/rdpg-boshrelease/blob/ServiceMatrix/src/rdpgd/docs/img/Bootstrap.png)

During initial bootstrap, it will first create users, databases and extensions, then it will register to consul and wait for cluster nodes. It will require the consul lock and check if the leader is set, if not go to leader bootstrap, otherwise go to non-leader bootstrap.The consul lock will be released in (non-)leader bootstrap (the details of leader and non-leader bootstrap are described in the followings sub sections). Afterwards, it will reconfigure services(pgbdr,pgbouncer,haproxy) and register consul services for service cluster nodes, at last, it will also register consul watches.

## Leader Bootstrap

It will first create bdr group, and then release consul lock. After it release the consul lock, it will wait for other bdr nodes to join. After all the bdr nodes joined, the leader will initialize the schema which includes the following steps.

* Create schemas for rdpg db (rdpg,cfsb,tasks,backups,metrics, audit)

* Create tables under the schemas mentioned above

* Insert default Schedules (manager node: vacuum, backups,reconcile all the databases and reconcile the available databases; service node: pre-create dbs and decommission dbs)

* Config rdpg

* Insert default services to services table

* Insert default plans to the plans table

* Create function which can be called to disable databases

* Insert default plans to the plans table

* Create function which can be called to disable databases

At the end of leader bootstrap, it will set itself as writemaster.

## Non-Leader Bootstrap

During non-leader bootstrap, it will first join the bdr group. Once it joins the bdr group, it release the consul lock, then wait for other bdr nodes to join. It then wait for instances table be replicated on all the cluster nodes and the write master node be set.
