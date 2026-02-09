#!/bin/bash
#
# Test certificate revocation validation using the revocation-validation framework.
#

set -o pipefail

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
JDBC_ROOT="$( dirname "${THIS_DIR}")"
WORKSPACE=${WORKSPACE:-${JDBC_ROOT}}

echo "[Info] Starting revocation validation tests"

# Detect JDBC version from pom.xml (already installed to ~/.m2 by the Build stage)
JDBC_VERSION=$(grep -m1 '<version>' "$JDBC_ROOT/pom.xml" | sed 's/.*<version>\(.*\)<\/version>.*/\1/')
echo "[Info] JDBC driver version: $JDBC_VERSION"

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
