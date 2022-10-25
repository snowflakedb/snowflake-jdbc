#!/bin/bash -e
#
# Build JDBC driver
#
set -o pipefail
THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
JDBC_ROOT=$(cd "${THIS_DIR}/../../" && pwd)

cd $JDBC_ROOT
rm -f lib/*.jar
mvn clean install --batch-mode --show-version

cd FIPS
rm -f lib/*.jar
mvn clean install --batch-mode --show-version
$THIS_DIR/upload_artifact.sh
