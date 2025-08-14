#!/bin/bash -e

set -o pipefail

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

setup_curl() {
  if curl --help 2>/dev/null | grep -q "\-\-aws-sigv4"; then
    echo "curl"
    return 0
  fi
  
  if [[ "$(uname -s)" == "Linux" ]]; then
    local curl_dir="$THIS_DIR/curl-new"
    mkdir -p "$curl_dir"
    local curl_binary="$curl_dir/curl-amd64"
    
    if [[ ! -f "$curl_binary" ]]; then
      if curl -sL "https://github.com/moparisthebest/static-curl/releases/download/v7.85.0/curl-amd64" -o "$curl_binary" 2>/dev/null; then
        chmod +x "$curl_binary"
      else
        echo "curl"
        return 0
      fi
    fi
    
    if [[ -x "$curl_binary" ]] && "$curl_binary" --help 2>/dev/null | grep -q "\-\-aws-sigv4"; then
      echo "$curl_binary"
      return 0
    fi
  fi
  
  echo "curl"
}

run_aws_function() {
  echo "Running AWS Lambda E2E test..."
  if [[ -z "$AWS_ACCESS_KEY" ]] || [[ -z "$AWS_SECRET_ACCESS_KEY" ]]; then
    echo "Error: AWS_ACCESS_KEY and AWS_SECRET_ACCESS_KEY environment variables must be set"
    return 1
  fi
  
  local curl_cmd
  curl_cmd=$(setup_curl)
  
  local url="${AWS_FUNCTION_BASE_URL}?SNOWFLAKE_TEST_WIF_HOST=${SNOWFLAKE_TEST_WIF_HOST_AWS}"
  url="${url}&SNOWFLAKE_TEST_WIF_ACCOUNT=${SNOWFLAKE_TEST_WIF_ACCOUNT}"
  url="${url}&SNOWFLAKE_TEST_WIF_PROVIDER=AWS"
  url="${url}&BRANCH=${BRANCH}"
  
  local http_code
  http_code=$("$curl_cmd" -s -o /dev/null -w "%{http_code}" \
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

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  run_aws_function
  exit $?
fi
