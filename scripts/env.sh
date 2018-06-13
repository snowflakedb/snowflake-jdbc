#!/bin/bash -e
#
# Set the environment variables for tests
#

set -o pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PARAMETER_FILE=$( cd "$DIR/.." && pwd)/parameters.json

[[ ! -e "$PARAMETER_FILE" ]] &&  echo "The parameter file doesn't exist: $PARAMETER_FILE" && exit 1

eval $(jq -r '.testconnection | to_entries | map("export \(.key)=\(.value|tostring)")|.[]' $PARAMETER_FILE)

if [[ -n "$TRAVIS_JOB_ID" ]]; then
    echo "==> Set the test schema to TRAVIS_JOB_${TRAVIS_JOB_ID}"
    export SNOWFLAKE_TEST_SCHEMA=TRAVIS_JOB_${TRAVIS_JOB_ID}
fi

echo "==> Test Connection Parameters"
env | grep SNOWFLAKE | grep -v PASSWORD
