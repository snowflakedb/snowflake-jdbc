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

if [[ -f "$WORKSPACE/parameters.json" ]]; then
    echo "[INFO] Found parameter file in $WORKSPACE"
    PARAMETER_FILE=$WORKSPACE/parameters.json
else
    echo "[INFO] Use the default test parameters.json"
    PARAMETER_FILE=$SOURCE_ROOT/src/test/resources/parameters.json
fi
eval $(jq -r '.testconnection | to_entries | map("export \(.key)=\(.value|tostring)")|.[]' $PARAMETER_FILE)
eval $(jq -r '.orgconnection | to_entries | map("export \(.key)=\(.value|tostring)")|.[]' $PARAMETER_FILE)


if [[ -n "$GITHUB_SHA" ]]; then
    # Github Action
    export TARGET_SCHEMA_NAME=${RUNNER_TRACKING_ID//-/_}_${GITHUB_SHA}

    function finish() {
        pushd $SOURCE_ROOT/ci/container >& /dev/null
            echo "[INFO] Drop schema $TARGET_SCHEMA_NAME"
            python3 drop_schema.py
        popd >& /dev/null
    }
    trap finish EXIT

    pushd $SOURCE_ROOT/ci/container >& /dev/null
        echo "[INFO] Create schema $TARGET_SCHEMA_NAME"
        if python3 create_schema.py; then
            export SNOWFLAKE_TEST_SCHEMA=$TARGET_SCHEMA_NAME
        else
            echo "[WARN] SNOWFLAKE_TEST_SCHEMA: $SNOWFLAKE_TEST_SCHEMA"
        fi
    popd >& /dev/null
fi

# we change password, create SSM_KNOWN_FILE
source $THIS_DIR/../log_analyze_setup.sh
if [[ "${ENABLE_CLIENT_LOG_ANALYZE}" == "true" ]]; then
    echo "[INFO] Log Analyze is enabled."

    setup_log_env

    if [[ "$SNOWFLAKE_TEST_HOST" == *"snowflake.reg"*".local"* && "$SNOWFLAKE_TEST_ACCOUNT" == "s3testaccount" && "$SNOWFLAKE_TEST_USER" == "snowman" && "$SNOWFLAKE_TEST_PASSWORD" == "test" ]]; then
        echo "[INFO] Run test with local instance. Will set a more complex password"

        python3 $THIS_DIR/change_snowflake_test_pwd.py
        export SNOWFLAKE_TEST_PASSWORD=$SNOWFLAKE_TEST_PASSWORD_NEW

        echo $SNOWFLAKE_TEST_PASSWORD >> $CLIENT_KNOWN_SSM_FILE_PATH
    else
        echo "[INFO] Not running test with local instance. Won't set a new password"
    fi
fi

env | grep SNOWFLAKE_ | grep -v PASS | sort

echo "[INFO] Running Hang Web Server"
kill -9 $(ps -ewf | grep hang_webserver | grep -v grep | awk '{print $2}') || true
python3 $THIS_DIR/hang_webserver.py 12345&

IFS=','
read -ra CATEGORY <<< "$JDBC_TEST_CATEGORY" 

cd $SOURCE_ROOT
for c in "${CATEGORY[@]}"; do
    c=$(echo $c | sed 's/ *$//g')
    if [[ "$is_old_driver" == "true" ]]; then
        pushd TestOnly >& /dev/null
            JDBC_VERSION=$(mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version --batch-mode | grep -v "[INFO]")
            echo "[INFO] Run JDBC $JDBC_VERSION tests"
            mvn -DjenkinsIT \
                -Djava.io.tmpdir=$WORKSPACE \
                -Djacoco.skip.instrument=false \
                -DtestCategory=net.snowflake.client.category.$c \
                -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn \
                verify \
                --batch-mode --show-version
        popd >& /dev/null
    elif [[ "$c" == "TestCategoryFips" ]]; then
        pushd FIPS >& /dev/null
            echo "[INFO] Run Fips tests"
            mvn -DjenkinsIT \
                -Djava.io.tmpdir=$WORKSPACE \
                -Djacoco.skip.instrument=false \
                -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn \
                -Dnot-self-contained-jar \
                verify \
                --batch-mode --show-version
        popd >& /dev/null
    else
        echo "[INFO] Run $c tests"
        mvn -DjenkinsIT \
            -Djava.io.tmpdir=$WORKSPACE \
            -Djacoco.skip.instrument=false \
            -DtestCategory=net.snowflake.client.category.$c \
            -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn \
            -Dnot-self-contained-jar $ADDITIONAL_MAVEN_PROFILE \
            verify \
            --batch-mode --show-version
    fi
done
IFS=' '
