#!/bin/bash -e

set -o pipefail
THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export WORKSPACE=${WORKSPACE:-/tmp}
export INTERNAL_REPO=nexus.int.snowflakecomputing.com:8086

source $THIS_DIR/scripts/login_internal_docker.sh
gpg --quiet --batch --yes --decrypt --passphrase="$PARAMETERS_SECRET" --output $THIS_DIR/../.github/workflows/parameters_aws_auth_tests.json "$THIS_DIR/../.github/workflows/parameters_aws_auth_tests.json.gpg"

docker run \
  -v $(cd $THIS_DIR/.. && pwd):/mnt/host \
  -v $WORKSPACE:/mnt/workspace \
  --rm \
  nexus.int.snowflakecomputing.com:8086/docker/snowdrivers-test-external-browser-jdbc:1 \
  "/mnt/host/ci/container/test_authentication.sh"
