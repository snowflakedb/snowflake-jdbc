#!/bin/bash -e
#
# Test JDBC for Linux
#
set -o pipefail
THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export WORKSPACE=${WORKSPACE:-/mnt/workspace}
export SOURCE_ROOT=${SOURCE_ROOT:-/mnt/host}

echo "[INFO] Download JDBC Integration test cases and libraries"
source ${THIS_DIR}/download_artifact.sh

echo "[INFO] Setting test parameters"
if [[ -f "$WORKSPACE/parameters.json" ]]; then
    echo "[INFO] Found parameter file in $WORKSPACE"
    PARAMETER_FILE=$WORKSPACE/parameters.json
else
    echo "[INFO] Use the default test parameters.json"
    PARAMETER_FILE=$SOURCE_ROOT/src/test/resources/parameters.json
fi
eval $(jq -r '.testconnection | to_entries | map("export \(.key)=\(.value|tostring)")|.[]' $PARAMETER_FILE)

export TARGET_SCHEMA_NAME=${RUNNER_TRACKING_ID//-/_}_${GITHUB_SHA}

env | grep SNOWFLAKE_ | grep -v PASS

cd $SOURCE_ROOT
mvn -DtravisIT \
    -DtravisTestCategory=net.snowflake.client.category.TestCategoryOthers \
    -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn \
    -Dnot-self-contained-jar \
    verify \
    --batch-mode --show-version
