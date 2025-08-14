#!/bin/bash -e

set -o pipefail

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

run_aws_function() {
  if [[ -z "$WIF_E2E_AWS_ACCESS_KEY" ]] || [[ -z "$WIF_E2E_AWS_SECRET_ACCESS_KEY" ]]; then
    echo "Error: WIF_E2E_AWS_ACCESS_KEY and WIF_E2E_AWS_SECRET_ACCESS_KEY environment variables must be set"
    return 1
  fi
  
  # Clear potentially conflicting AWS environment variables
  unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN
  unset AWS_PROFILE AWS_CONFIG_FILE AWS_SHARED_CREDENTIALS_FILE

  # Set AWS credentials for CLI
  export AWS_ACCESS_KEY_ID="$WIF_E2E_AWS_ACCESS_KEY"
  export AWS_SECRET_ACCESS_KEY="$WIF_E2E_AWS_SECRET_ACCESS_KEY"
  export AWS_DEFAULT_REGION="us-west-2"
  export AWS_REGION="us-west-2"
  
  # Check AWS CLI version to determine the correct command format
  local aws_version
  aws_version=$(aws --version 2>&1 | head -n1)

  local payload_json="{\"queryStringParameters\":{\"SNOWFLAKE_TEST_WIF_HOST\":\"${SNOWFLAKE_TEST_WIF_HOST_AWS}\",\"SNOWFLAKE_TEST_WIF_ACCOUNT\":\"${SNOWFLAKE_TEST_WIF_ACCOUNT}\",\"SNOWFLAKE_TEST_WIF_PROVIDER\":\"AWS\",\"BRANCH\":\"${BRANCH}\"}}"
  local function_name="drivers-wif-automated-tests"
  
  local cli_response_file="/tmp/aws_cli_response_$$.json"
  local cli_error_file="/tmp/aws_cli_error_$$.txt"
    
  # Use different command based on AWS CLI version
  if [[ "$aws_version" =~ aws-cli/2\. ]]; then
    # AWS CLI v2 - needs --cli-binary-format flag
    echo "Using AWS CLI v2 format"
    aws lambda invoke \
      --function-name "$function_name" \
      --region "us-west-2" \
      --cli-binary-format raw-in-base64-out \
      --payload "$payload_json" \
      --cli-read-timeout 1000 \
      --cli-connect-timeout 60 \
      "$cli_response_file" >/dev/null 2>"$cli_error_file"
  else
    # AWS CLI v1 - no --cli-binary-format flag needed, but include timeouts
    echo "Using AWS CLI v1 format"
    aws lambda invoke \
      --function-name "$function_name" \
      --region "us-west-2" \
      --payload "$payload_json" \
      --cli-read-timeout 1000 \
      --cli-connect-timeout 60 \
      "$cli_response_file" >/dev/null 2>"$cli_error_file"
  fi
  
  local invoke_result=$?
  if [[ $invoke_result -eq 0 ]]; then

    if [[ -f "$cli_response_file" ]]; then
      # Check if the response indicates success (HTTP 200)
      if grep -q '"statusCode":200' "$cli_response_file" 2>/dev/null; then
        echo "AWS Lambda Function test passed (HTTP 200)"
        rm -f "$cli_response_file" "$cli_error_file"
        return 0
      else
        echo "AWS Lambda Function test failed (non-200 status)"
        rm -f "$cli_response_file" "$cli_error_file"
        return 1
      fi
    else
      echo "No response file found"
      rm -f "$cli_response_file" "$cli_error_file"
      return 1
    fi
  else
    echo "AWS CLI invocation failed"
    if [[ -f "$cli_error_file" ]]; then
      echo "CLI Error:"
      cat "$cli_error_file"
    fi
    rm -f "$cli_response_file" "$cli_error_file"
    return 1
  fi
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  run_aws_function
  exit $?
fi
