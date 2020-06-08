#!/bin/bash -e
#
# Login the Docker Hub
#
echo "[INFO] Login the Docker Hub"
if [[ -z "$DOCKER_HUB_USER" ]] || [[ -z "$DOCKER_HUB_TOKEN" ]]; then
    echo "[ERROR] Set DOCKER_HUB_USER and DOCKER_HUB_TOKEN to push the images to the Docker Hub"
    exit 1
fi
docker login --username "$DOCKER_HUB_USER" --password "$DOCKER_HUB_TOKEN"
