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

    mkdir -p $LIB_DIR
    pushd $LIB_DIR >& /dev/null
        base_stage=s3://sfc-jenkins/repository/jdbc/${BRANCH}
        git_latest_commit=$(aws s3 cp $base_stage/latest_commit -)
        export GIT_COMMIT=${GIT_COMMIT:-$git_latest_commit}
        source_stage=$base_stage/${GIT_COMMIT}
        echo "[INFO] downloading ${source_stage}/"
        aws s3 cp --only-show-errors $source_stage/ . --recursive
        if ! ls $LIB_DIR/*.jar; then
            if [[ "$BRANCH" == "master" ]]; then
                source_latest_stage=$base_stage/${git_latest_commit}
                echo "[WARN] failed to download jar files from $source_stage. Retrying from $source_latest_stage"
                source_stage=$source_latest_stage
                aws s3 cp --only-show-errors $source_stage/ . --recursive
            fi
        fi
        if ! ls $LIB_DIR/*.jar; then
            echo "[ERROR] No jar exists in $source_stage. Ensure the build job was success"
            exit 1
        fi
    popd >& /dev/null
    mkdir -p /mnt/host/lib
    cp -p $LIB_DIR/*.jar /mnt/host/lib
else
    export GIT_BRANCH=origin/$(basename ${GITHUB_REF})
    export GIT_COMMIT=${GITHUB_SHA}
fi
