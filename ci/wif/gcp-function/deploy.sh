#!/bin/bash

set -e

FUNCTION_NAME="drivers-wif-e2e-gcp"
DEFAULT_REGION="us-central1"
MEMORY="2048MB"
TIMEOUT="540s"
RUNTIME="java17"
ENTRY_POINT="com.snowflake.wif.gcp.WifGcpFunctionE2e"

# Check if required parameters are provided
if [ $# -lt 2 ]; then
    echo "ERROR: Missing required parameters"
    echo "Usage: $0 <PROJECT_ID> <SERVICE_ACCOUNT> [REGION]"
    echo "  PROJECT_ID: Google Cloud Project ID"
    echo "  SERVICE_ACCOUNT: Service account email to use for the function"
    echo "  REGION: (Optional) GCP region (default: $DEFAULT_REGION)"
    exit 1
fi

PROJECT_ID=$1
SERVICE_ACCOUNT=$2
REGION=${3:-$DEFAULT_REGION}

echo "==============================================="
echo "Deploying GCP Function for WIF E2E Testing"
echo "==============================================="
echo "Function Name: $FUNCTION_NAME"
echo "Project ID: $PROJECT_ID"
echo "Region: $REGION"
echo "Runtime: $RUNTIME"
echo "Memory: $MEMORY"
echo "Timeout: $TIMEOUT"
echo "Entry Point: $ENTRY_POINT"
echo "Service Account: $SERVICE_ACCOUNT"
echo "==============================================="

# Check if gcloud is installed and authenticated
if ! command -v gcloud &> /dev/null; then
    echo "ERROR: gcloud CLI is not installed. Please install Google Cloud SDK."
    exit 1
fi

# Check if user is authenticated
if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | grep -q .; then
    echo "ERROR: No active gcloud authentication found. Please run 'gcloud auth login'"
    exit 1
fi

# Set the project
echo "Setting project to: $PROJECT_ID"
gcloud config set project $PROJECT_ID

# Enable required APIs
echo "Enabling required Google Cloud APIs..."
gcloud services enable cloudfunctions.googleapis.com
gcloud services enable cloudbuild.googleapis.com
gcloud services enable logging.googleapis.com

echo "Building the function..."
mvn clean package -q

if [ ! -f "target/deploy/${FUNCTION_NAME}.jar" ]; then
    echo "ERROR: JAR file not found at target/deploy/${FUNCTION_NAME}.jar"
    echo "Make sure Maven build completed successfully."
    exit 1
fi

echo "JAR file created successfully: target/deploy/${FUNCTION_NAME}.jar"

echo "Deploying function to GCP..."

DEPLOY_CMD="gcloud functions deploy $FUNCTION_NAME \
    --runtime=$RUNTIME \
    --region=$REGION \
    --source=target/deploy \
    --entry-point=$ENTRY_POINT \
    --memory=$MEMORY \
    --timeout=$TIMEOUT \
    --trigger-http \
    --service-account=$SERVICE_ACCOUNT"

DEPLOY_CMD="$DEPLOY_CMD \
    --set-env-vars LOG_EXECUTION_ID=true,JAVA_TOOL_OPTIONS=\"-XX:+UseParallelGC -XX:MaxRAMPercentage=70 -Xms512m\" \
    --max-instances=10 \
    --min-instances=0"

eval $DEPLOY_CMD

if [ $? -eq 0 ]; then
    echo "Function deployed successfully!"
else
    echo "Function deployment failed!"
    exit 1
fi
