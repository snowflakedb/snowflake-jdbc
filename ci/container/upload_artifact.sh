#!/bin/bash -e
#
# Upload jar files to S3
#
set -o pipefail
THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
JDBC_ROOT=$(cd "${THIS_DIR}/../../" && pwd)
export GIT_BRANCH=${GIT_BRANCH:-origin/$(git rev-parse --abbrev-ref HEAD)}
export GIT_COMMIT=${GIT_COMMIT:-$(git rev-parse HEAD)}

BRANCH=$(basename ${GIT_BRANCH})

target_stage=s3://sfc-jenkins/repository/jdbc/$BRANCH/${GIT_COMMIT}
echo "[INFO] Uploading jar to $target_stage/"
aws s3 cp --only-show-errors $JDBC_ROOT/lib/ $target_stage/ --recursive --exclude "*" --include "*.jar"
aws s3 cp --only-show-errors $JDBC_ROOT/FIPS/lib $target_stage/ --recursive --exclude "*" --include "*.jar"

COMMIT_FILE=$(mktemp)
cat > $COMMIT_FILE <<COMMIT_FILE_CONTENTS
${GIT_COMMIT}
COMMIT_FILE_CONTENTS

latest_commit_file=$target_stage/latest_commit
echo "[INFO] updating ${latest_commit_file}"
aws s3 cp --only-show-errors $COMMIT_FILE $latest_commit_file
rm -f $COMMIT_FILE

