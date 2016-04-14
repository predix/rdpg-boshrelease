#!/bin/bash

MY_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
TEST_DIR=$MY_DIR/../rdpg-service
CF_VERBOSE_OUTPUT=true
GINKGO_OPTS="\
    -r \
    -p \
    -v \
    -keepGoing=true \
    -randomizeSuites \
    -randomizeAllSpecs \
    -trace=true \
    -slowSpecThreshold=300 \
    -failOnPending"
