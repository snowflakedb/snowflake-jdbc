#!/usr/bin/env bash
#
# Architectural guard: ensures no raw System.getProperty() or System.getenv()
# calls exist in production code outside SnowflakeUtil.java.
#
# System.setProperty() is excluded — there is no wrapper for it; the convention
# is to wrap each call in a try-catch for SecurityException.
#
# Use SnowflakeUtil.systemGetProperty() / systemGetEnv() instead of raw calls.
#
set -euo pipefail

SRC_DIR="${1:-src/main/java}"
WRAPPER_FILE="SnowflakeUtil.java"

violations=$(
    grep -rn --include='*.java' -E 'System\.(getProperty|getenv)\s*\(' "$SRC_DIR" \
    | grep -v "/${WRAPPER_FILE}:" \
    || true
)

if [[ -n "$violations" ]]; then
    echo "ERROR: Found raw System.getProperty/getenv calls outside ${WRAPPER_FILE}:"
    echo ""
    echo "$violations"
    echo ""
    echo "Replace with SnowflakeUtil.systemGetProperty() / systemGetEnv()."
    exit 1
fi

echo "OK: No raw System.getProperty/getenv calls found outside ${WRAPPER_FILE}."
