#!/bin/bash -e
#
# Build JDBC driver
#
set -o pipefail
THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
JDBC_ROOT=$(cd "${THIS_DIR}/../../" && pwd)

source "$JDBC_ROOT/ci/maven_jenkins_settings.sh"

cd $JDBC_ROOT
rm -f lib/*.jar
mvn $MVN_SETTINGS_ARG clean install -DskipTests --batch-mode --show-version

cd FIPS
rm -f lib/*.jar
mvn $MVN_SETTINGS_ARG clean install -DskipTests -Dsurefire.argLine="-Djavax.net.debug=ssl:handshake" -Dfailsafe.argLine="-Djavax.net.debug=ssl:handshake" --batch-mode --show-version
$THIS_DIR/upload_artifact.sh
