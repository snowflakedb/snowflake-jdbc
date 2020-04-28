#!/bin/bash -e
#
# Test JDBC for Linux
#
set -o pipefail
THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

export TEST_ROOT=/mnt/workspace

echo "[INFO] Download JDBC Integration test cases and libraries"
source ${THIS_DIR}/download_artifact.sh

echo "[INFO] Run JDBC integration tests"
${THIS_DIR}/test_run.sh
