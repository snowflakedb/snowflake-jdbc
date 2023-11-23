#!/usr/bin/env bash
set -e
#
# Build Docker images
#
set -o pipefail
THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $THIS_DIR/../_init.sh

cp -p $THIS_DIR/../../pom.xml $THIS_DIR
mkdir -p dependencies && cp -rp $THIS_DIR/../../dependencies/ $THIS_DIR/dependencies

for name in "${!TEST_IMAGE_NAMES[@]}"; do
    echo "Building $name"
    docker build \
        --progress=plain \
        --platform=linux/x86_64 \
        --pull \
        --file $THIS_DIR/Dockerfile.$(echo ${TEST_IMAGE_DOCKERFILES[$name]}) \
        --label snowflake \
        --label $DRIVER_NAME \
        $(echo ${TEST_IMAGE_BUILD_ARGS[$name]}) \
        --tag ${TEST_IMAGE_NAMES[$name]} .
done
