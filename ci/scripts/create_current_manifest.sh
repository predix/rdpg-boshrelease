#!/bin/bash

# change to root of bosh release
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd $DIR/../..

echo "Creating stub"

cat > ci/aws/templates/${environment}/stub.yml << EOF
---
meta:
  environment: ${bosh_deployment_name}
  stemcell:
    name: bosh-aws-xen-centos-7-go_agent
    version: latest

director_uuid: ${bosh_uuid}

releases:
  - name: rdpg
    version: $(cat ci/aws/releases/version)

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
EOF

spruce merge templates/deployment.yml \
            templates/jobs.yml \
            ci/aws/templates/smoke_tests.yml \
            templates/infrastructure/aws.yml \
            ci/aws/templates/overwrites.yml \
            ci/aws/templates/${environment}/networks.yml \
            ci/aws/templates/${environment}/properties.yml \
            ci/aws/templates/${environment}/stub.yml > manifests/aws.yml
