#!/usr/local/bin/env bash
set -e

export PLATFORM=$(echo $(uname) | tr '[:upper:]' '[:lower:]')
export INTERNAL_REPO=nexus.int.snowflakecomputing.com:8086
if [[ -z "$GITHUB_ACTIONS" ]]; then
    # Use the internal Docker Registry
    export DOCKER_REGISTRY_NAME=$INTERNAL_REPO/docker
    export WORKSPACE=${WORKSPACE:-/tmp}
else
    # Use Docker Hub
    export DOCKER_REGISTRY_NAME=snowflakedb
    export WORKSPACE=$GITHUB_WORKSPACE
fi
mkdir -p $WORKSPACE

export DRIVER_NAME=jdbc

# Test Images
TEST_IMAGE_VERSION=1

declare -A TEST_IMAGE_NAMES=(
    [$DRIVER_NAME-centos7-openjdk8]=$DOCKER_REGISTRY_NAME/client-$DRIVER_NAME-centos7-openjdk8-test:$TEST_IMAGE_VERSION
    [$DRIVER_NAME-centos7-openjdk11]=$DOCKER_REGISTRY_NAME/client-$DRIVER_NAME-centos7-openjdk11-test:$TEST_IMAGE_VERSION
    [$DRIVER_NAME-centos7-openjdk17]=$DOCKER_REGISTRY_NAME/client-$DRIVER_NAME-centos7-openjdk17-test:$TEST_IMAGE_VERSION
    [$DRIVER_NAME-centos7-openjdk21]=$DOCKER_REGISTRY_NAME/client-$DRIVER_NAME-centos7-openjdk21-test:$TEST_IMAGE_VERSION
)
export TEST_IMAGE_NAMES

declare -A TEST_IMAGE_DOCKERFILES=(
    [$DRIVER_NAME-centos7-openjdk8]=jdbc-centos7-openjdk-test
    [$DRIVER_NAME-centos7-openjdk11]=jdbc-centos7-openjdk-test
    [$DRIVER_NAME-centos7-openjdk17]=jdbc-centos7-openjdk-test
    [$DRIVER_NAME-centos7-openjdk21]=jdbc-centos7-openjdk-test
)

declare -A TEST_IMAGE_BUILD_ARGS=(
    [$DRIVER_NAME-centos7-openjdk8]="--target jdbc-centos7-openjdk-yum --build-arg=JDK_PACKAGE=java-1.8.0-openjdk-devel"
    [$DRIVER_NAME-centos7-openjdk11]="--target jdbc-centos7-openjdk-yum --build-arg=JDK_PACKAGE=java-11-openjdk-devel" # pragma: allowlist secret
    [$DRIVER_NAME-centos7-openjdk17]="--target jdbc-centos7-openjdk17"
    [$DRIVER_NAME-centos7-openjdk21]="--target jdbc-centos7-openjdk21"
)

