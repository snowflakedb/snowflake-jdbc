#!/bin/bash -e
#
# Download Artifact
#
export GIT_BRANCH=${GIT_BRANCH:-origin/$(git rev-parse --abbrev-ref HEAD)}
export GIT_COMMIT=${GIT_COMMIT:-$(git rev-parse HEAD)}

BRANCH=$(basename ${GIT_BRANCH})

# Place to hold downloaded library
export LIB_DIR=$WORKSPACE/lib

mkdir -p $LIB_DIR
pushd $LIB_DIR >& /dev/null
    source_stage=s3://sfc-jenkins/repository/jdbc/${BRANCH}/${GIT_COMMIT}
    echo "[INFO] downloading ${source_stage}/"
    aws s3 cp --only-show-errors $source_stage/ . --recursive
popd >& /dev/null
mkdir -p /mnt/host/lib
cp -p $LIB_DIR/*.jar /mnt/host/lib
