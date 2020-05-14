#!/bin/bash -e
#
# Test JDBC for Linux
#
set -o pipefail
THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export WORKSPACE=${WORKSPACE:-/mnt/workspace}
export SOURCE_ROOT=${SOURCE_ROOT:-/mnt/host}

echo "[INFO] Download JDBC Integration test cases and libraries"
source $THIS_DIR/download_artifact.sh

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

echo "[INFO] Running Hang Web Server"
kill -9 $(ps -ewf | grep hang_webserver | grep -v grep | awk '{print $2}') || true
python3 $THIS_DIR/hang_webserver.py 12345&

env | grep SNOWFLAKE_ | grep -v PASS

IFS=','
read -ra CATEGORY <<< "$JDBC_TEST_CATEGORY" 

cd $SOURCE_ROOT
for c in "${CATEGORY[@]}"; do
    mvn -DjenkinsIT \
        -Djava.io.tmpdir=$WORKSPACE \
        -DtestCategory=net.snowflake.client.category.$c \
        -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn \
        -Dnot-self-contained-jar \
        verify \
        --batch-mode --show-version
done
IFS=' '
