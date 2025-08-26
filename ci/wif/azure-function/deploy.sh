#!/bin/bash

set -e

RESOURCE_GROUP="automated-driver-tests"
FUNCTION_APP_NAME="drivers-wif-e2e"
REGISTRY_NAME="driverswife2e"
IMAGE_NAME="wif-azure-function"
TAG="latest"

echo "Deploying updated Azure Function..."

echo "Building Docker image..."
# Build from parent directory to include shared folder in Docker context
cd ..
docker build --platform linux/amd64 -f azure-function/Dockerfile -t ${IMAGE_NAME}:${TAG} .
cd azure-function

echo "Tagging image for Azure Container Registry..."
docker tag ${IMAGE_NAME}:${TAG} ${REGISTRY_NAME}.azurecr.io/${IMAGE_NAME}:${TAG}

echo "Logging into Azure Container Registry..."
az acr login --name ${REGISTRY_NAME}

echo "Pushing image to registry..."
docker push ${REGISTRY_NAME}.azurecr.io/${IMAGE_NAME}:${TAG}

echo "Updating Function App to use latest image..."
az functionapp config set \
  --name ${FUNCTION_APP_NAME} \
  --resource-group ${RESOURCE_GROUP} \
  --linux-fx-version "DOCKER|${REGISTRY_NAME}.azurecr.io/${IMAGE_NAME}:${TAG}"

echo "Setting memory configuration..."
az functionapp config appsettings set \
  --name ${FUNCTION_APP_NAME} \
  --resource-group ${RESOURCE_GROUP} \
  --settings WEBSITE_MEMORY_LIMIT_MB=4096

echo "Restarting Function App..."
az functionapp restart \
  --name ${FUNCTION_APP_NAME} \
  --resource-group ${RESOURCE_GROUP}
