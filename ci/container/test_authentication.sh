#!/bin/bash -e

set -o pipefail

export WORKSPACE=${WORKSPACE:-/mnt/workspace}
export SOURCE_ROOT=${SOURCE_ROOT:-/mnt/host}
# Prefer Maven installed in the test image over ./mvnw (Maven Central HTTP 429).
if [[ -x /usr/local/bin/mvn ]]; then
    MVN_EXE=/usr/local/bin/mvn
else
    MVN_EXE=$SOURCE_ROOT/mvnw
fi
echo "[INFO] Using Maven: $MVN_EXE"

source "$SOURCE_ROOT/ci/maven_jenkins_settings.sh"

AUTH_PARAMETER_FILE=./.github/workflows/parameters_aws_auth_tests.json
eval $(jq -r '.authtestparams | to_entries | map("export \(.key)=\(.value|tostring)")|.[]' $AUTH_PARAMETER_FILE)

$MVN_EXE $MVN_SETTINGS_ARG -DjenkinsIT \
    -Dnet.snowflake.jdbc.temporaryCredentialCacheDir=/mnt/workspace/abc \
    -Dnet.snowflake.jdbc.ocspResponseCacheDir=/mnt/workspace/abc \
    -Djava.io.tmpdir=$WORKSPACE \
    -Djacoco.skip.instrument=true \
    -Dskip.unitTests=true \
    -DintegrationTestSuites=AuthenticationTestSuite \
    -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn \
    -Dnot-self-contained-jar \
    -Denforcer.skip=true \
    clean verify \
    --batch-mode --show-version
