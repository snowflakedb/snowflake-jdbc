#!/usr/bin/env bash
#
# Generates a single CycloneDX SBOM (Software Bill of Materials) covering
# both shipped artifacts: the root snowflake-jdbc driver and the FIPS
# module. Both are declared as modules of the parent-pom.xml reactor, so
# cyclonedx-maven-plugin's makeAggregateBom goal produces one BOM with both
# products as top-level components, each with its own dependency graph.
#
# Uses the org.cyclonedx:cyclonedx-maven-plugin version pinned in
# parent-pom.xml (version.plugin.cyclonedx); it is not bound to any Maven
# lifecycle phase, so it is only run when this script invokes it explicitly.
#
set -euo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
JDBC_ROOT=$(cd "${DIR}/../../" && pwd)

SCRATCH_DIR=$(mktemp -d)
trap 'rm -rf "$SCRATCH_DIR"' EXIT

"$JDBC_ROOT/mvnw" -f "$JDBC_ROOT/parent-pom.xml" \
    --batch-mode --show-version \
    -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn \
    org.cyclonedx:cyclonedx-maven-plugin:makeAggregateBom \
    -DoutputFormat=json \
    -DoutputDirectory="$SCRATCH_DIR" \
    >&2

cat "$SCRATCH_DIR/bom.json"
