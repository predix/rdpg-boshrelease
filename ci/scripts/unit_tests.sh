#!/bin/bash

set -e

# change to root of bosh release
# BASH_SOURCE[0] is the full path to this script when being executed.
# dirname strips off the script name and returns the directory.
# Change directory to the rdpgd relative to this scripts directory.
cd "$(dirname "${BASH_SOURCE[0]}")/../../src/rdpgd"

export GOPATH="$PWD/Godeps/_workspace"
mkdir -p ${GOPATH}/src/github.com/starkandwayne
if ! [[ -L ${GOPATH}/src/github.com/starkandwayne/rdpgd ]]
    then ln -sf ${PWD} ${GOPATH}/src/github.com/starkandwayne/rdpgd
fi

echo "Running Go test..."
go test
echo "Complete running go test."

