Reliable Distributed PostgreSQL (RDPG) BOSH release
====================================================

License
-------

MIT, see LICENSE file.

Usage: Configuration & Deployment
---------------------------------

We will walk through an example of using with Cloud Foundry.

Be sure to first target your BOSH Director:

```sh
bosh target $BOSH_HOST
```

If you are using bosh-lite you target like so:

```sh
bosh target 192.168.50.4 lite
```

Now clone this release and cd into the directory:

```sh
git clone https://.../rdpg-boshrelease.git
cd rdpg-boshrelease
```

If you intend on using a final release upload it like so:

```sh
bosh upload release releases/rdpg-1.yml
```

Next download your manifest file for the deployment targeted so we can edit it and add the release.

```sh
mkdir -p ~/workspace/manifests
bosh download manifest rdpg-development ~/workspace/manifests/rdpg.yml
bosh deployment ~/workspace/manifests/rdpg.yml
```

Alternatively, you can make your manifest. For example to prepare a manifest for bosh-lite (warden) using the 'centos' stemcell we would do the following:

```sh
STEMCELL_OS=centos ./rdpg-dev manifest warden
```

Edit the manifest file you downloaded (`~/workspace/manifests/rdpg.yml`) and add settings as follows.

Add to the list of known `releases:`

```yaml
releases:
#...
- name: rdpg
  version: latest
```

For consistency also add to the `releases:` section under `meta:`

```yaml
meta:
  environment: rdpg-development
  releases:
  - name: cf
    version: latest
  - name: rdpg
    version: latest
```

Add properties such as tags.

```yaml
properties:
# ... lots of properties ... at bottom put vv
  rdpg:
```

Now, for every `instances:` entry you wish to collocate this release with under `jobs:` add the following in the `templates:` section:

```yaml
  - name: rdpg
    release: rdpg
```

Now you can deploy,

```yaml
bosh -n deploy
```

Note that for each job you create in your release that you want to run on a Job VM you must add a `templates:` entry with the `name:` of the template and the `release:` from which it comes.

Deployments Blobs
-----------------

The script `./rdpg-dev blobs` is used to prepare the `blobs/` directory runs each package's `prepare` script from within the `blobs/` directory. This will run each package's `prepare` script, if it exists, which *should* download and prepare that package's required blobs (source tarballs, etc...) into the {package}/ directory.

Development
-----------

Download your manifest file for the deployment targeted.

Target your BOSH Director as explained above, if you already have a deploy be sure to also download your manifest as per above.

If this is your first time cloning the release repository for develompent first prepare all of the things:

```sh
./rdpg-dev prepare warden
```

This is equivalent to:

```sh
./rdpg-dev blobs
./rdpg-dev release
./rdpg-dev stemcell warden
./rdpg-dev manifest warden
```

Once these steps have all been completed

```sh
bosh -n deploy
```

More Information
----------------

For more information on the internal components used to manage PostgreSQL databases please refer to the documentation in `src/rdpgd/docs/*.md`

*There is also a large treasure trove of "How To" and similar documents in the Wiki in this repository.*


Debugging & QA
--------------

In order to gain access to one of the VMs, from a terminal `bosh ssh rdpg {VM Index}`, for example to ssh to the first node:

```sh
bosh ssh rdpg 0 # Hop on and have a look around...
```

If you want to destroy and recreate on specific node, say `rdpg/0`, you do the following:

```sh
bosh -n recreate rdpg 0 --force
```

See `docs/rdpg.md` for release specific information.

To run acceptance tests:

`bosh create release --force && bosh upload release && bosh -n deploy && bosh run errand acceptance_tests`

Pipeline
--------

This BOSH release is automatically built and tested upon every commit to master.

[![bosh release changes trigger deploy job](http://cl.ly/image/0O220s281l1L/bosh_release_changes_trigger_deploy_job.png)](http://ci.starkandwayne.com:8080/pipelines/rdpg-boshrelease)

The `deploy` job will:

1.	Run `bosh create release` to ensure that the BOSH release can be successfully created
2.	Upload the release to a bosh-lite running on AWS
3.	Generate a manifest and deploy a cluster (via the `./rdpg-dev manifest warden` manifest generator)
4.	Run the `acceptance_tests` errand

The `deploy` job is triggered when:

-	`jobs` or `packages` folders are modified - changing the BOSH release itself
-	`templates` folders are modified - changing how users might deploy the BOSH release
-	`src` folder is modified (see below for jobs that do this via Concourse)
-	`config/blobs.yml` is modified - changing the blobs that go into the BOSH release
