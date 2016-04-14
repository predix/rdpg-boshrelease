#!/bin/bash

# change to root of bosh release
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd $DIR/../..

cat > ~/.bosh_config << EOF
---
aliases:
  target:
    bosh-lite: ${bosh_target}
auth:
  ${bosh_target}:
    username: ${bosh_username}
    password: ${bosh_password}
EOF
bosh target ${bosh_target}

_bosh() {
  bosh -n $@
}


set -e
git submodule update --init --recursive --force

_bosh create release --force
set +e

_bosh upload release --rebase || echo "Continuing..."
set -e

cd ../
git clone https://github.com/cloudfoundry-community/postgres-smoke-tests-boshrelease.git
cd postgres-smoke-tests-boshrelease
set -e
git submodule update --init --recursive --force
_bosh create release --force
set +e
_bosh upload release --rebase || echo "Continuing..."
set -e

cd $DIR/../../

echo "Creating stub"

cat > ci/vsphere/templates/${environment}/stub.yml << EOF
---
meta:
  environment: ${bosh_deployment_name}
  stemcell:
    name: bosh-vsphere-esxi-centos-7-go_agent
    version: latest

director_uuid: ${bosh_uuid}

releases:
  - name: rdpg
    version: latest

properties:
  rdpgd_manager:
    backups_s3_access_key: ${backups_s3_access_key}
    backups_s3_secret_key: ${backups_s3_secret_key}
    backups_s3_bucket_name: ${backups_s3_bucket_name}
    # 'ENABLED' and 'DISABLED' are the valid values.
    backups_s3: "ENABLED"
  rdpgd_service:
    backups_s3_access_key: ${backups_s3_access_key}
    backups_s3_secret_key: ${backups_s3_secret_key}
    backups_s3_bucket_name: ${backups_s3_bucket_name}
    # 'ENABLED' and 'DISABLED' are the valid values.
    backups_s3: "ENABLED"
resource_pools:
- cloud_properties:
    datacenters:
    - clusters:
      - FD3:
          resource_pool: PCF-FD3
  name: rdpg

- cloud_properties:
    datacenters:
    - clusters:
      - FD1:
          resource_pool: PCF-FD1
  name: errand_a
compilation:
  cloud_properties:
    cpu: 1
    datacenters:
    - clusters:
      - FD3:
          resource_pool: PCF-FD3
    disk: 8096
    ram: 2048
  network: rdpg
  reuse_compilation_vms: true
  workers: 3
EOF

spruce merge templates/deployment.yml \
            templates/jobs.yml \
            ci/vsphere/templates/smoke_tests.yml \
            templates/infrastructure/vsphere.yml \
            ci/vsphere/templates/overwrites.yml \
            ci/vsphere/templates/${environment}/networks.yml \
            ci/vsphere/templates/${environment}/properties.yml \
            ci/vsphere/templates/${environment}/stub.yml > manifests/vsphere.yml

_bosh deployment manifests/vsphere.yml
_bosh deploy
