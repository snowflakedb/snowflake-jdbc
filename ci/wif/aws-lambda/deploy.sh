#!/bin/bash

set -e

# Check if AWS CLI is installed (for credentials)
if ! command -v aws &> /dev/null; then
    echo "Error: AWS CLI is not installed. Please install it first for credential management."
    echo "Visit: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html"
    exit 1
fi

# Check AWS credentials and get account ID
if ! ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text 2>/dev/null); then
    echo "Error: AWS credentials not configured. Please run 'aws configure' first."
    exit 1
fi

echo "AWS Account ID: $ACCOUNT_ID"

# Default values
FUNCTION_NAME="drivers-wif-automated-tests"
REGION="us-west-2"
RUNTIME="java17"
TIMEOUT="480"
MEMORY_SIZE="3000"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --function-name)
            FUNCTION_NAME="$2"
            shift 2
            ;;
        --region)
            REGION="$2"
            shift 2
            ;;
        --timeout)
            TIMEOUT="$2"
            shift 2
            ;;
        --memory-size)
            MEMORY_SIZE="$2"
            shift 2
            ;;
        --help)
            echo "Usage: $0 [options]"
            echo "Options:"
            echo "  --function-name FUNCTION    Lambda function name (default: drivers-wif-e2e)"
            echo "  --region REGION             AWS region (default: us-west-2)"
            echo "  --timeout TIMEOUT           Function timeout in seconds (default: 900)"
            echo "  --memory-size MEMORY        Memory size in MB (default: 3008)"
            echo "  --help                      Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help to see available options"
            exit 1
            ;;
    esac
done

echo "=== AWS Lambda Maven Deployment ==="
echo "Function Name: $FUNCTION_NAME"
echo "Region: $REGION"
echo "Runtime: $RUNTIME"
echo "Timeout: $TIMEOUT seconds"
echo "Memory: $MEMORY_SIZE MB"
echo

# Build JAR using Maven
echo "Building Lambda function JAR using Maven..."
mvn clean package

if [ $? -ne 0 ]; then
    echo "Error: Maven build failed"
    exit 1
fi


# Verify JAR file exists
JAR_FILE="target/aws-lambda-1.0-SNAPSHOT.jar"
if [ ! -f "$JAR_FILE" ]; then
    echo "Error: JAR file not found: $JAR_FILE"
    echo "Make sure Maven build completed successfully"
    exit 1
fi

echo "JAR file size: $(ls -lh $JAR_FILE | awk '{print $5}')"

# Deploy Lambda function using AWS CLI
echo "Deploying Lambda function using AWS CLI..."
echo "Function: $FUNCTION_NAME"
echo "Runtime: $RUNTIME"
echo "Handler: com.snowflake.wif.aws.WifLambdaFunctionE2e::handleRequest"
echo "Timeout: $TIMEOUT seconds"
echo "Memory: $MEMORY_SIZE MB"
echo "Role: arn:aws:iam::${ACCOUNT_ID}:role/${FUNCTION_NAME}"

# Check if function already exists
echo "Checking if function exists..."
if aws lambda get-function --function-name "$FUNCTION_NAME" --region "$REGION" &>/dev/null; then
    echo "Function exists - updating code..."
    aws lambda update-function-code \
        --function-name "$FUNCTION_NAME" \
        --zip-file "fileb://$JAR_FILE" \
        --region "$REGION" \
        --cli-connect-timeout 60 \
        --cli-read-timeout 300
    
    echo "Updating function configuration..."
    aws lambda update-function-configuration \
        --function-name "$FUNCTION_NAME" \
        --timeout "$TIMEOUT" \
        --memory-size "$MEMORY_SIZE" \
        --environment "Variables={JAVA_TOOL_OPTIONS=-XX:+TieredCompilation -XX:TieredStopAtLevel=1}" \
        --region "$REGION"
else
    echo "Creating new function..."
    aws lambda create-function \
        --function-name "$FUNCTION_NAME" \
        --runtime "$RUNTIME" \
        --handler "com.snowflake.wif.aws.WifLambdaFunctionE2e::handleRequest" \
        --role "arn:aws:iam::${ACCOUNT_ID}:role/${FUNCTION_NAME}" \
        --zip-file "fileb://$JAR_FILE" \
        --timeout "$TIMEOUT" \
        --memory-size "$MEMORY_SIZE" \
        --environment "Variables={JAVA_TOOL_OPTIONS=-XX:+TieredCompilation -XX:TieredStopAtLevel=1}" \
        --description "WIF E2E Testing Function" \
        --region "$REGION" \
        --cli-connect-timeout 60 \
        --cli-read-timeout 300
fi

DEPLOY_EXIT_CODE=$?
echo "AWS CLI exit code: $DEPLOY_EXIT_CODE"

if [ $DEPLOY_EXIT_CODE -eq 0 ]; then
    echo "=== Deployment Successful ==="
else
    echo "=== Deployment Failed ==="
    exit 1
fi
