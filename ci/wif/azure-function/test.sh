#!/bin/bash -e

set -o pipefail

# Azure Function E2E test script
# This script contains Azure-specific test functions extracted from ci/test_wif.sh

run_azure_function() {
  echo "Running Azure Function E2E test..."
  
  local url="${AZURE_FUNCTION_BASE_URL}?code=${AZURE_FUNCTION_CODE}"
  url="${url}&BRANCH=${BRANCH}"
  url="${url}&SNOWFLAKE_TEST_WIF_HOST=${SNOWFLAKE_TEST_WIF_HOST_AZURE}"
  url="${url}&SNOWFLAKE_TEST_WIF_ACCOUNT=${SNOWFLAKE_TEST_WIF_ACCOUNT}"
  url="${url}&SNOWFLAKE_TEST_WIF_PROVIDER=AZURE"
 
  local http_code
  http_code=$(curl -s -o /dev/null -w "%{http_code}" "$url")
  
  if [[ "$http_code" == "200" ]]; then
    echo "Azure Function test passed"
    return 0
  else
    echo "Azure Function test failed with HTTP $http_code"
    return 1
  fi
}

# Main execution
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  # Script is being executed directly
  run_azure_function
  exit $?
fi
