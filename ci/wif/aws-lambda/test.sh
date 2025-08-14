#!/bin/bash -e

set -o pipefail

# AWS Lambda Function E2E test script
# This script contains AWS-specific test functions extracted from ci/test_wif.sh

run_aws_function() {
  echo "Running AWS Lambda Function E2E test..."
  
  if [[ -z "$AWS_ACCESS_KEY" ]] || [[ -z "$AWS_SECRET_ACCESS_KEY" ]]; then
    echo "Error: AWS_ACCESS_KEY and AWS_SECRET_ACCESS_KEY environment variables must be set"
    return 1
  fi
  
  local url="${AWS_FUNCTION_BASE_URL}?SNOWFLAKE_TEST_WIF_HOST=${SNOWFLAKE_TEST_WIF_HOST_AWS}"
  url="${url}&SNOWFLAKE_TEST_WIF_ACCOUNT=${SNOWFLAKE_TEST_WIF_ACCOUNT}"
  url="${url}&SNOWFLAKE_TEST_WIF_PROVIDER=AWS"
  url="${url}&BRANCH=${BRANCH}"
  
  local http_code
  http_code=$(curl -s -o /dev/null -w "%{http_code}" \
    --aws-sigv4 "aws:amz:us-west-2:lambda" \
    --user "${AWS_ACCESS_KEY}:${AWS_SECRET_ACCESS_KEY}" \
    "$url")
  
  if [[ "$http_code" == "200" ]]; then
    echo "AWS Lambda Function test passed"
    return 0
  else
    echo "AWS Lambda Function test failed with HTTP $http_code"
    return 1
  fi
}

# Main execution
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  # Script is being executed directly
  run_aws_function
  exit $?
fi
