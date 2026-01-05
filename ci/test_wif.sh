#!/bin/bash -e

set -o pipefail

export THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export RSA_KEY_PATH_AWS_AZURE="$THIS_DIR/wif/parameters/rsa_wif_aws_azure"
export RSA_KEY_PATH_GCP="$THIS_DIR/wif/parameters/rsa_wif_gcp"
export RSA_GCP_FUNCTION_KEY="$THIS_DIR/wif/parameters/rsa_gcp_function"
export PARAMETERS_FILE_PATH="$THIS_DIR/wif/parameters/parameters_wif.json"
export PARAMETERS_FUNCTIONS_FILE_PATH="$THIS_DIR/wif/parameters/parameters_wif_function.json"

run_tests_and_set_result() {
  local provider="$1"
  local host="$2"
  local snowflake_host="$3"
  local rsa_key_path="$4"
  local snowflake_user="$5"
  local impersonation_path="$6"
  local snowflake_user_for_impersonation="$7"

  ssh -i "$rsa_key_path" -o IdentitiesOnly=yes -p 443 "$host" env BRANCH="$BRANCH" SNOWFLAKE_TEST_WIF_HOST="$snowflake_host" SNOWFLAKE_TEST_WIF_PROVIDER="$provider" SNOWFLAKE_TEST_WIF_ACCOUNT="$SNOWFLAKE_TEST_WIF_ACCOUNT" SNOWFLAKE_TEST_WIF_USERNAME="$snowflake_user" SNOWFLAKE_TEST_WIF_IMPERSONATION_PATH="$impersonation_path" SNOWFLAKE_TEST_WIF_USERNAME_IMPERSONATION="$snowflake_user_for_impersonation" bash << EOF
      set -e
      set -o pipefail
      docker run \
        --rm \
        --cpus=1 \
        -m 2g \
        -e BRANCH \
        -e SNOWFLAKE_TEST_WIF_PROVIDER \
        -e SNOWFLAKE_TEST_WIF_HOST \
        -e SNOWFLAKE_TEST_WIF_ACCOUNT \
        -e SNOWFLAKE_TEST_WIF_USERNAME \
        -e SNOWFLAKE_TEST_WIF_IMPERSONATION_PATH \
        -e SNOWFLAKE_TEST_WIF_USERNAME_IMPERSONATION \
        snowflakedb/client-jdbc-centos7-openjdk17-test:1 \
          bash -c "
            echo 'Running tests on branch: \$BRANCH'
            mkdir -p /tmp/maven-repo /tmp/workspace
            chmod 755 /tmp/maven-repo /tmp/workspace
            if [[ \"\$BRANCH\" =~ ^PR-[0-9]+\$ ]]; then
              curl -L https://github.com/snowflakedb/snowflake-jdbc/archive/refs/pull/\$(echo \$BRANCH | cut -d- -f2)/head.tar.gz | tar -xz
              mv snowflake-jdbc-* snowflake-jdbc
            else
              curl -L https://github.com/snowflakedb/snowflake-jdbc/archive/refs/heads/\$BRANCH.tar.gz | tar -xz
              mv snowflake-jdbc-\$BRANCH snowflake-jdbc
            fi
            cd snowflake-jdbc
            bash ci/wif/test_wif.sh
          "
EOF
  local status=$?

  if [[ $status -ne 0 ]]; then
    echo "$provider tests failed with exit status: $status"
    EXIT_STATUS=1
  else
    echo "$provider tests passed"
  fi
}

get_branch() {
  local branch
  if [[ -n "${GIT_BRANCH}" ]]; then
    # Jenkins
    branch="${GIT_BRANCH}"
  else
    branch=$(git rev-parse --abbrev-ref HEAD)
    if [[ "$branch" == "HEAD" ]]; then
      branch=$(git name-rev --name-only HEAD | sed 's#^remotes/origin/##;s#^origin/##')
    fi
  fi
  echo "$branch"
}

run_azure_function() {
  if ! bash "$THIS_DIR/wif/azure-function/test.sh"; then
    EXIT_STATUS=1
  fi
}

run_aws_function() {
  if ! bash "$THIS_DIR/wif/aws-lambda/test.sh"; then
    EXIT_STATUS=1
  fi
}

run_gcp_function() {
  if ! bash "$THIS_DIR/wif/gcp-function/test.sh"; then
    EXIT_STATUS=1
  fi
}

setup_parameters() {
  source "$THIS_DIR/scripts/setup_gpg.sh"
  gpg --quiet --batch --yes --decrypt --passphrase="$PARAMETERS_SECRET" --output "$RSA_KEY_PATH_AWS_AZURE" "${RSA_KEY_PATH_AWS_AZURE}.gpg"
  gpg --quiet --batch --yes --decrypt --passphrase="$PARAMETERS_SECRET" --output "$RSA_KEY_PATH_GCP" "${RSA_KEY_PATH_GCP}.gpg"
  gpg --quiet --batch --yes --decrypt --passphrase="$PARAMETERS_SECRET" --output "$RSA_GCP_FUNCTION_KEY" "${RSA_GCP_FUNCTION_KEY}.gpg"
  chmod 600 "$RSA_KEY_PATH_AWS_AZURE"
  chmod 600 "$RSA_KEY_PATH_GCP"
  chmod 600 "$RSA_GCP_FUNCTION_KEY"
  gpg --quiet --batch --yes --decrypt --passphrase="$PARAMETERS_SECRET" --output "$PARAMETERS_FILE_PATH" "${PARAMETERS_FILE_PATH}.gpg"
  eval $(jq -r '.wif | to_entries | map("export \(.key)=\(.value|tostring)")|.[]' $PARAMETERS_FILE_PATH)
  gpg --quiet --batch --yes --decrypt --passphrase="$PARAMETERS_SECRET" --output "$PARAMETERS_FUNCTIONS_FILE_PATH" "${PARAMETERS_FUNCTIONS_FILE_PATH}.gpg"
  eval $(jq -r '.wif | to_entries | map("export \(.key)=\(.value|tostring)")|.[]' $PARAMETERS_FUNCTIONS_FILE_PATH)
}

BRANCH=$(get_branch)
export BRANCH
setup_parameters

# Run tests for all cloud providers
EXIT_STATUS=0
set +e  # Don't exit on first failure

# WIF E2E tests on functions
run_aws_function
run_azure_function
run_gcp_function
# WIF E2E tests on VMs
run_tests_and_set_result "AZURE" "$HOST_AZURE" "$SNOWFLAKE_TEST_WIF_HOST_AZURE" "$RSA_KEY_PATH_AWS_AZURE" "$SNOWFLAKE_TEST_WIF_USERNAME_AZURE" "$SNOWFLAKE_TEST_WIF_IMPERSONATION_PATH_AZURE" "$SNOWFLAKE_TEST_WIF_USERNAME_AZURE_IMPERSONATION"
run_tests_and_set_result "AWS" "$HOST_AWS" "$SNOWFLAKE_TEST_WIF_HOST_AWS" "$RSA_KEY_PATH_AWS_AZURE" "$SNOWFLAKE_TEST_WIF_USERNAME_AWS" "$SNOWFLAKE_TEST_WIF_IMPERSONATION_PATH_AWS" "$SNOWFLAKE_TEST_WIF_USERNAME_AWS_IMPERSONATION"
run_tests_and_set_result "GCP" "$HOST_GCP" "$SNOWFLAKE_TEST_WIF_HOST_GCP" "$RSA_KEY_PATH_GCP" "$SNOWFLAKE_TEST_WIF_USERNAME_GCP" "$SNOWFLAKE_TEST_WIF_IMPERSONATION_PATH_GCP" "$SNOWFLAKE_TEST_WIF_USERNAME_GCP_IMPERSONATION"

set -e  # Re-enable exit on error
echo "Exit status: $EXIT_STATUS"
exit $EXIT_STATUS
