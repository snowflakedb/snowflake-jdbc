#!/bin/bash -e

set -o pipefail

generate_gcp_jwt() {
  # For ID token request, aud should be Google's token endpoint, target_audience is the service
  local target_audience="${GCP_FUNCTION_BASE_URL}"
  
  local iat=$(date +%s)
  local exp=$((iat + 3600))  # 1 hour expiration
  
  # Create JWT header
  local header='{"alg":"RS256","typ":"JWT"}'
  local header_b64=$(echo -n "$header" | base64 -w 0 | tr -d '=' | tr '/+' '_-')
  
  # Create JWT payload for ID token reques
  local payload="{\"iss\":\"$GCP_SERVICE_ACCOUNT\",\"aud\":\"https://oauth2.googleapis.com/token\",\"target_audience\":\"$target_audience\",\"exp\":$exp,\"iat\":$iat}"
  local payload_b64=$(echo -n "$payload" | base64 -w 0 | tr -d '=' | tr '/+' '_-')
  
  # Create signature using private key from file
  local to_sign="$header_b64.$payload_b64"
  local signature=$(echo -n "$to_sign" | openssl dgst -sha256 -sign "$RSA_GCP_FUNCTION_KEY" | base64 -w 0 | tr -d '=' | tr '/+' '_-')
  
  echo "$header_b64.$payload_b64.$signature"
}

get_gcp_id_token() {
  local jwt="$1"
  
  local response=$(curl -s -X POST https://oauth2.googleapis.com/token \
    -H "Content-Type: application/x-www-form-urlencoded" \
    -d "grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer&assertion=$jwt")
  
  echo "$response" | grep -o '"id_token":"[^"]*"' | cut -d'"' -f4
}

run_gcp_function() {
  echo "Running GCP Cloud Function E2E test..."
  
  local jwt=$(generate_gcp_jwt)
  if [[ -z "$jwt" ]]; then
    echo "Error: Failed to generate JWT"
    return 1
  fi
  
  local id_token=$(get_gcp_id_token "$jwt")
  
  if [[ -z "$id_token" ]]; then
    echo "Error: Failed to get GCP ID token"
    return 1
  fi
  
  local url="${GCP_FUNCTION_BASE_URL}?SNOWFLAKE_TEST_WIF_HOST=${SNOWFLAKE_TEST_WIF_HOST_GCP}"
  url="${url}&SNOWFLAKE_TEST_WIF_ACCOUNT=${SNOWFLAKE_TEST_WIF_ACCOUNT}"
  url="${url}&SNOWFLAKE_TEST_WIF_PROVIDER=GCP"
  url="${url}&BRANCH=${BRANCH}"
  url="${url}&IS_GCP_FUNCTION=true"
  
  local http_code
  http_code=$(curl -s -o /dev/null -w "%{http_code}" --max-time 1200 \
    -H "Authorization: Bearer $id_token" \
    "$url")
  
  if [[ "$http_code" == "200" ]]; then
    echo "GCP Cloud Function test passed"
    return 0
  else
    echo "GCP Cloud Function test failed with HTTP $http_code"
    return 1
  fi
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  run_gcp_function
  exit $?
fi
