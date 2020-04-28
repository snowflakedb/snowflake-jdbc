#!/bin/bash -e
#
# Test ODBC
#
set -o pipefail
THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
JDBC_ROOT="$(cd "${THIS_DIR}/.." && pwd)"

source ${THIS_DIR}/_init.sh
source ${THIS_DIR}/scripts/login_internal_docker.sh

WORKSPACE=${WORKSPACE:-/tmp/jdbc_test_output}
[[ ! -d ${WORKSPACE} ]] && mkdir -p ${WORKSPACE}

if [[ -z "$GITHUB_ACTIONS" ]]; then
    export GIT_BRANCH=${client_git_branch:-origin/$(git rev-parse --abbrev-ref HEAD)}
    export GIT_COMMIT=${client_git_commit:-$(git rev-parse HEAD)}
else
    export GIT_BRANCH=origin/$(basename ${GITHUB_REF})
    export GIT_COMMIT=${GITHUB_SHA}
fi

# This is based on the assumption that the regression is running on the same
# host as the container's host
SF_REGRESS_GLOBAL_SERVICES_IP=${SF_REGRESS_GLOBAL_SERVICES_IP:-"snowflake.reg.local"}
SF_REGRESS_GLOBAL_SERVICES_PORT=${SF_REGRESS_GLOBAL_SERVICES_PORT:-8082}
echo "Use /sbin/ip"
IP_ADDR=$(/sbin/ip -4 addr show scope global dev eth0 | grep inet | awk '{print $2}' | cut -d / -f 1)

declare -A TARGET_TEST_IMAGES
if [[ -n "$TARGET_DOCKER_TEST_IMAGE" ]]; then
    echo "[INFO] TARGET_DOCKER_TEST_IMAGE: $TARGET_DOCKER_TEST_IMAGE"
    IMAGE_NAME=${TEST_IMAGE_NAMES[$TARGET_DOCKER_TEST_IMAGE]}
    if [[ -z "$IMAGE_NAME" ]]; then
        echo "[ERROR] The target platform $TARGET_DOCKER_TEST_IMAGE doesn't exist. Check $THIS_DIR/_init.sh"
        exit 1
    fi
    TARGET_TEST_IMAGES=([$TARGET_DOCKER_TEST_IMAGE]=$IMAGE_NAME)
else
    echo "[ERROR] Set TARGET_DOCKER_TEST_IMAGE to the docker image name to run the test"
    for name in "${!TEST_IMAGE_NAMES[@]}"; do
        echo "  " $name
    done
    exit 2
fi

nslookup ${SF_REGRESS_GLOBAL_SERVICES_IP}

for name in "${!TARGET_TEST_IMAGES[@]}"; do
    echo "[INFO] Building $DRIVER_NAME on $name"
    docker pull "${TEST_IMAGE_NAMES[$name]}"
    docker container run \
        --rm \
        -v ${JDBC_ROOT}:/mnt/host \
        -v ${WORKSPACE}:/mnt/workspace \
        -e LOCAL_USER_ID=$(id -u ${USER}) \
        -e AWS_ACCESS_KEY_ID \
        -e AWS_SECRET_ACCESS_KEY \
        -e JOB_NAME \
        -e BUILD_NUMBER \
        -e GIT_BRANCH \
        -e GIT_COMMIT \
        -e SF_REGRESS_GLOBAL_SERVICES_IP \
        -e SF_REGRESS_GLOBAL_SERVICES_PORT \
        --add-host=${SF_REGRESS_GLOBAL_SERVICES_IP}:${IP_ADDR} \
        ${TEST_IMAGE_NAMES[$name]} \
        /mnt/host/ci/container/test_component.sh
done
