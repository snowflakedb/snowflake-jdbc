#!/bin/bash
#
# Test certificate revocation validation using the revocation-validation framework.
#

set -o pipefail

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
JDBC_ROOT="$( dirname "${THIS_DIR}")"
WORKSPACE=${WORKSPACE:-${JDBC_ROOT}}

echo "[Info] Starting revocation validation tests"

# Detect JDBC version using Maven (reliable, handles property interpolation)
JDBC_VERSION=$(cd "$JDBC_ROOT" && mvn help:evaluate -Dexpression=project.version -q -DforceStdout 2>/dev/null)
if [ -z "$JDBC_VERSION" ]; then
    echo "[Error] Failed to determine JDBC version from pom.xml"
    exit 1
fi
echo "[Info] JDBC driver version: $JDBC_VERSION"

# Ensure parent POM is also in ~/.m2 (needed for Maven dependency resolution)
if [ ! -f "$HOME/.m2/repository/net/snowflake/snowflake-jdbc-parent/$JDBC_VERSION/"*.pom ]; then
    echo "[Info] Installing parent POM to local Maven repo..."
    if ! (cd "$JDBC_ROOT" && mvn install -f parent-pom.xml -Dmaven.test.skip=true -q --batch-mode); then
        echo "[Error] Failed to install parent POM"
        exit 1
    fi
fi

set -e

# Clone revocation-validation framework
REVOCATION_DIR="/tmp/revocation-validation"
REVOCATION_BRANCH="${REVOCATION_BRANCH:-main}"

rm -rf "$REVOCATION_DIR"
if [ -n "$GITHUB_USER" ] && [ -n "$GITHUB_TOKEN" ]; then
    git clone --depth 1 --branch "$REVOCATION_BRANCH" "https://${GITHUB_USER}:${GITHUB_TOKEN}@github.com/snowflakedb/revocation-validation.git" "$REVOCATION_DIR"
else
    git clone --depth 1 --branch "$REVOCATION_BRANCH" "https://github.com/snowflakedb/revocation-validation.git" "$REVOCATION_DIR"
fi

cd "$REVOCATION_DIR"

# Point the wrapper's pom.xml at the locally-built JDBC version (already in ~/.m2 from Build stage)
WRAPPER_POM="$REVOCATION_DIR/validation/clients/snowflake-jdbc/java/pom.xml"
sed -i.bak '/<artifactId>snowflake-jdbc<\/artifactId>/{n;s|<version>[^<]*</version>|<version>'"$JDBC_VERSION"'</version>|;}' "$WRAPPER_POM"
rm -f "${WRAPPER_POM}.bak"
echo "[Info] Updated wrapper to use JDBC $JDBC_VERSION"

echo "[Info] Running tests with Go $(go version | grep -oE 'go[0-9]+\.[0-9]+')..."

go run . \
    --client snowflake-jdbc \
    --output "${WORKSPACE}/revocation-results.json" \
    --output-html "${WORKSPACE}/revocation-report.html" \
    --log-level debug

EXIT_CODE=$?

if [ -f "${WORKSPACE}/revocation-results.json" ]; then
    echo "[Info] Results: ${WORKSPACE}/revocation-results.json"
fi
if [ -f "${WORKSPACE}/revocation-report.html" ]; then
    echo "[Info] Report: ${WORKSPACE}/revocation-report.html"
fi

exit $EXIT_CODE
