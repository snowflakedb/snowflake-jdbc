#!/bin/bash -e

set -o pipefail
export THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export RSA_KEY_PATH="$THIS_DIR/wif/parameters/rsa_wif"
export PARAMETERS_FILE_PATH="$THIS_DIR/wif/parameters/parameters_wif.json"

run_wif_tests() {
  local cloud_provider="$1"
  local host="$2"
  local snowflake_host="$3"

ssh -i "$RSA_KEY_PATH" -o IdentitiesOnly=yes "$host" env BRANCH="$BRANCH" SNOWFLAKE_TEST_WIF_HOST="$snowflake_host" SNOWFLAKE_TEST_WIF_PROVIDER="$cloud_provider" SNOWFLAKE_TEST_WIF_ACCOUNT="$SNOWFLAKE_TEST_WIF_ACCOUNT" bash << EOF
    set -e
    set -o pipefail
    docker run \
      --rm \
      -e BRANCH \
      -e SNOWFLAKE_TEST_WIF_PROVIDER \
      -e SNOWFLAKE_TEST_WIF_HOST \
      -e SNOWFLAKE_TEST_WIF_ACCOUNT \
      snowflakedb/client-jdbc-centos7-openjdk17-test:1 \
        bash -c "
          echo 'Running tests on branch: \$BRANCH'
          mkdir -p /tmp/maven-repo /tmp/workspace
          chmod 755 /tmp/maven-repo /tmp/workspace
          if [[ \"\$BRANCH\" == PR-* ]]; then
            ID=\$(echo \$BRANCH | cut -d'-' -f2)
            curl -L https://github.com/snowflakedb/snowflake-jdbc/archive/refs/pull/\$ID/head.tar.gz | tar -xz
            mv snowflake-jdbc-* snowflake-jdbc
          else
            curl -L https://github.com/snowflakedb/snowflake-jdbc/archive/refs/heads/\$BRANCH.tar.gz | tar -xz
            mv snowflake-jdbc-\$BRANCH snowflake-jdbc
          fi
          cd snowflake-jdbc
          bash ci/wif/test_wif.sh
        "
EOF
}

run_tests_and_set_result() {
  local provider="$1"
  local host="$2"
  local snowflake_host="$3"

  run_wif_tests "$provider" "$host" "$snowflake_host"
  local status=$?

  if [[ $status -ne 0 ]]; then
    echo "$provider tests failed with exit status: $status"
    EXIT_STATUS=1
  else
    echo "$provider tests passed"
  fi
}

gpg --quiet --batch --yes --decrypt --passphrase="$PARAMETERS_SECRET" --output "$RSA_KEY_PATH" "${RSA_KEY_PATH}.gpg"
chmod 600 "$RSA_KEY_PATH"
gpg --quiet --batch --yes --decrypt --passphrase="$PARAMETERS_SECRET" --output "$PARAMETERS_FILE_PATH" "${PARAMETERS_FILE_PATH}.gpg"
eval $(jq -r '.wif | to_entries | map("export \(.key)=\(.value|tostring)")|.[]' $PARAMETERS_FILE_PATH)

# Run tests for all cloud providers
EXIT_STATUS=0
set +e  # Don't exit on first failure
run_tests_and_set_result "AZURE" "$HOST_AZURE" "$SNOWFLAKE_TEST_WIF_HOST_AZURE"
run_tests_and_set_result "AWS" "$HOST_AWS" "$SNOWFLAKE_TEST_WIF_HOST_AWS"

set -e  # Re-enable exit on error
echo "Exit status: $EXIT_STATUS"
exit $EXIT_STATUS
