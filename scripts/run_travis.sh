#!/bin/bash -e
#
# Run Travis Build and Tests
#
set -o pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR/..

function travis_fold_start() {
    local name=$1
    local message=$2
    echo "travis_fold:start:$name"
    tput setaf 3 || true
    echo $message
    tput sgr0 || true
    export travis_fold_name=$name
}

function travis_fold_end() {
    echo "travis_fold:end:$travis_fold_name"
    unset travis_fold_name
}

function finish {
    travis_fold_start drop_schema "Drop test schema"
    python $DIR/drop_schema.py 
    travis_fold_end
}

travis_fold_start pythonvenv "Set up Python Virtualenv (pyenv)"
pip install -U snowflake-connector-python
travis_fold_end

trap finish EXIT

source $DIR/env.sh
set MAVEN_OPTS="-Xmx1536m -XX:MaxPermSize=128m"

travis_fold_start create_schema "Create test schema"
python $DIR/create_schema.py
travis_fold_end

travis_fold_start build "Build and Test JDBC driver"
PARAMS=()
PARAMS+=("-DtravisIT")
# code coverage plugin jacoco did not work when running with self-contained-jar
PARAMS+=("-Dnot-self-contained-jar")
PARAMS+=("-DtravisTestCategory=net.snowflake.client.category.${TEST_CATEGORY}")
# suppress Download ... messages
PARAMS+=("-Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn")
echo "JDK Version: $TRAVIS_JDK_VERSION"
[[ -n "$JACOCO_COVERAGE" ]] && PARAMS+=("-Djacoco.skip.instrument=false")
# verify phase is after test/integration-test phase, which means both unit test 
# and integration test will be run
mvn "${PARAMS[@]}" verify --batch-mode --show-version

travis_fold_end
