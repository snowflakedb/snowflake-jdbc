#!/usr/bin/env bash
#
# Sanity-checks the aggregate CycloneDX SBOM produced by
# ci/scripts/generate_sbom.sh. cyclonedx-maven-plugin has no "validate"
# goal, so this performs structural checks with jq instead: valid CycloneDX
# JSON, both shipped products present as top-level components, and each has
# its own non-empty dependency graph. Intended to run in CI right after
# generate_sbom.sh's stdout has been captured to a file, so a broken/empty
# SBOM fails the build.
#
# Usage: check_sbom.sh <path-to-bom.json>
#
set -euo pipefail

BOM_FILE="$1"
EXPECTED_COMPONENTS=("net.snowflake/snowflake-jdbc" "net.snowflake/snowflake-jdbc-fips")

if [[ ! -s "$BOM_FILE" ]]; then
    echo "[ERROR] SBOM file not found or empty: $BOM_FILE"
    exit 1
fi

if ! jq -e '.bomFormat == "CycloneDX" and (.specVersion | test("^1\\."))' "$BOM_FILE" >/dev/null; then
    echo "[ERROR] $BOM_FILE is not a recognizable CycloneDX document"
    exit 1
fi

for component in "${EXPECTED_COMPONENTS[@]}"; do
    group="${component%%/*}"
    name="${component##*/}"

    if ! jq -e --arg group "$group" --arg name "$name" \
            '.components[]? | select(.group == $group and .name == $name)' "$BOM_FILE" >/dev/null; then
        echo "[ERROR] $BOM_FILE does not contain expected component $component"
        exit 1
    fi

    ref=$(jq -r --arg group "$group" --arg name "$name" \
            '.components[] | select(.group == $group and .name == $name) | ."bom-ref"' "$BOM_FILE")
    depends_on_count=$(jq --arg ref "$ref" \
            '[.dependencies[]? | select(.ref == $ref) | .dependsOn[]?] | length' "$BOM_FILE")
    if [[ "$depends_on_count" -eq 0 ]]; then
        echo "[ERROR] $component has no dependencies listed in $BOM_FILE; SBOM generation likely broken for this product"
        exit 1
    fi
    echo "OK: $component present with $depends_on_count dependency(ies)."
done

component_count=$(jq '.components | length' "$BOM_FILE")
echo "OK: $BOM_FILE is a valid CycloneDX document listing $component_count component(s) total."
