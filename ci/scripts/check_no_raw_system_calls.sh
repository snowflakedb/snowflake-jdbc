#!/usr/bin/env bash
#
# Architectural guard: ensures no raw System.getProperty(), System.getenv(),
# or System.setProperty() calls exist in production code outside SnowflakeUtil.java.
#
# Use SnowflakeUtil.systemGetProperty() / systemGetEnv() / systemSetProperty()
# instead of raw calls to ensure proper SecurityException handling.
#
set -euo pipefail

SRC_DIR="${1:-src/main/java}"
WRAPPER_FILE="SnowflakeUtil.java"

violations=$(
    grep -rn --include='*.java' -E 'System\.(getProperty|getenv|setProperty)\s*\(' "$SRC_DIR" \
    | grep -v "/${WRAPPER_FILE}:" \
    || true
)

if [[ -n "$violations" ]]; then
    echo "ERROR: Found raw System.getProperty/getenv/setProperty calls outside ${WRAPPER_FILE}:"
    echo ""
    echo "$violations"
    echo ""
    echo "Replace with SnowflakeUtil.systemGetProperty() / systemGetEnv() / systemSetProperty()."
    exit 1
fi

echo "OK: No raw System.getProperty/getenv/setProperty calls found outside ${WRAPPER_FILE}."
