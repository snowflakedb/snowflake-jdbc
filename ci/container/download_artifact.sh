#!/bin/bash -e
#
# Download Artifact
#
set -o pipefail
THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
JDBC_ROOT=$(cd "${THIS_DIR}/../../" && pwd)

if [[ -z "$GITHUB_ACTIONS" ]] ;then
    export GIT_BRANCH=${GIT_BRANCH:-origin/$(git rev-parse --abbrev-ref HEAD)}

    BRANCH=$(basename ${GIT_BRANCH})

    # Place to hold downloaded library
    export LIB_DIR=$WORKSPACE/lib

    if [[ "$is_old_driver" != "true" ]]; then
        # Not Old Driver test
        mkdir -p $LIB_DIR
        pushd $LIB_DIR >& /dev/null
            base_stage=s3://sfc-eng-jenkins/repository/jdbc/${BRANCH}
            export GIT_COMMIT=${GIT_COMMIT:-$(aws s3 cp $base_stage/latest_commit -)}
            source_stage=$base_stage/${GIT_COMMIT}
            echo "[INFO] downloading ${source_stage}/"
            aws s3 cp --only-show-errors $source_stage/ . --recursive
        popd >& /dev/null
        mkdir -p /mnt/host/lib
        cp -p $LIB_DIR/*.jar /mnt/host/lib
    fi
else
    export GIT_BRANCH=origin/$(basename ${GITHUB_REF})
    export GIT_COMMIT=${GITHUB_SHA}
fi
