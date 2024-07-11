#!/bin/bash -e
#
# Test JDBC
#
set -o pipefail
THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
JDBC_ROOT="$(cd "${THIS_DIR}/.." && pwd)"

source $THIS_DIR/_init.sh
source $THIS_DIR/scripts/login_internal_docker.sh
source $THIS_DIR/scripts/set_git_info.sh

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

if [[ -z "$JDBC_TEST_CATEGORY" ]]; then
    echo "[ERROR] Set JDBC_TEST_CATEGORY to the JDBC test category."
    find $THIS_DIR/../src/test/java -type f -exec grep -E "^import net.snowflake.client.category" {} \; | sort | uniq | awk -F. '{print $NF}' | awk -F\; '{print $1}'
    exit 2
fi

for name in "${!TARGET_TEST_IMAGES[@]}"; do
    echo "[INFO] Testing $DRIVER_NAME on $name"
    # docker pull "${TEST_IMAGE_NAMES[$name]}"
    docker container run \
        --rm \
        -v $JDBC_ROOT:/mnt/host \
        -v $WORKSPACE:/mnt/workspace \
        -e LOCAL_USER_ID=$(id -u ${USER}) \
        -e TERM=xterm \
        -e GIT_COMMIT \
        -e GIT_BRANCH \
        -e GIT_URL \
        -e AWS_ACCESS_KEY_ID \
        -e AWS_SECRET_ACCESS_KEY \
        -e GITHUB_ACTIONS \
        -e GITHUB_SHA \
        -e GITHUB_REF \
        -e RUNNER_TRACKING_ID \
        -e JOB_NAME \
        -e BUILD_NUMBER \
        -e JDBC_TEST_CATEGORY \
        -e ADDITIONAL_MAVEN_PROFILE \
        -e is_old_driver \
        --add-host=snowflake.reg.local:${IP_ADDR} \
        --add-host=s3testaccount.reg.local:${IP_ADDR} \
        --add-host=azureaccount.reg.local:${IP_ADDR} \
        --add-host=gcpaccount.reg.local:${IP_ADDR} \
        --add-host=wrongaccount.reg.local:${IP_ADDR} \
        ${TEST_IMAGE_NAMES[$name]} \
        /mnt/host/ci/container/test_component.sh
done
