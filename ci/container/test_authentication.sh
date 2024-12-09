#!/bin/bash -e

set -o pipefail

export WORKSPACE=${WORKSPACE:-/mnt/workspace}
export SOURCE_ROOT=${SOURCE_ROOT:-/mnt/host}
MVNW_EXE=$SOURCE_ROOT/mvnw

AUTH_PARAMETER_FILE=./.github/workflows/parameters_aws_auth_tests.json
eval $(jq -r '.authtestparams | to_entries | map("export \(.key)=\(.value|tostring)")|.[]' $AUTH_PARAMETER_FILE)

$MVNW_EXE -DjenkinsIT \
    -Djava.io.tmpdir=$WORKSPACE \
    -Djacoco.skip.instrument=true \
    -Dskip.unitTests=true \
    -DintegrationTestSuites=AuthenticationTestSuite \
    -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn \
    -Dnot-self-contained-jar \
    verify \
    --batch-mode --show-version
