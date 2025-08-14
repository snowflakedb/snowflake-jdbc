#!/bin/bash -e

set -o pipefail

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

setup_curl() {
  echo "Checking curl --aws-sigv4 support..." >&2
  if curl --help 2>/dev/null | grep -q "\-\-aws-sigv4"; then
    echo "System curl supports --aws-sigv4" >&2
    echo "curl"
    return 0
  fi
  
  echo "System curl doesn't support --aws-sigv4, checking if Linux..." >&2
  if [[ "$(uname -s)" == "Linux" ]]; then
    echo "Running on Linux, attempting to download newer curl..." >&2
    local curl_dir="$THIS_DIR/curl-new"
    mkdir -p "$curl_dir"
    local curl_binary="$curl_dir/curl-amd64"
    
    if [[ ! -f "$curl_binary" ]]; then
      echo "Downloading curl to $curl_binary..." >&2
      if curl -sL "https://github.com/moparisthebest/static-curl/releases/download/v7.85.0/curl-amd64" -o "$curl_binary"; then
        chmod +x "$curl_binary"
        echo "Downloaded curl successfully" >&2
      else
        echo "Failed to download curl, using system curl" >&2
        echo "curl"
        return 0
      fi
    else
      echo "Using existing downloaded curl at $curl_binary" >&2
    fi
    
    if [[ -x "$curl_binary" ]] && "$curl_binary" --help 2>/dev/null | grep -q "\-\-aws-sigv4"; then
      echo "Downloaded curl supports --aws-sigv4, using it" >&2
      echo "$curl_binary"
      return 0
    else
      echo "Downloaded curl doesn't support --aws-sigv4, using system curl" >&2
    fi
  else
    echo "Not running on Linux, using system curl" >&2
  fi
  
  echo "curl"
}

run_aws_function() {
  if [[ -z "$AWS_ACCESS_KEY" ]] || [[ -z "$AWS_SECRET_ACCESS_KEY" ]]; then
    echo "Error: AWS_ACCESS_KEY and AWS_SECRET_ACCESS_KEY environment variables must be set"
    return 1
  fi
  
  local curl_cmd
  curl_cmd=$(setup_curl)
  echo "Using curl command: $curl_cmd"
  echo "Curl version: $("$curl_cmd" --version | head -1)"
  
  local url="${AWS_FUNCTION_BASE_URL}?SNOWFLAKE_TEST_WIF_HOST=${SNOWFLAKE_TEST_WIF_HOST_AWS}"
  url="${url}&SNOWFLAKE_TEST_WIF_ACCOUNT=${SNOWFLAKE_TEST_WIF_ACCOUNT}"
  url="${url}&SNOWFLAKE_TEST_WIF_PROVIDER=AWS"
  url="${url}&BRANCH=${BRANCH}"
  
  local http_code
  echo "Executing curl with --aws-sigv4..."
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
