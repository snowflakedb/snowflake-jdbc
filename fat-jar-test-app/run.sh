#!/bin/bash -ex
#
# Test fat jar by running a sample app that connects to Snowflake
#
set -o pipefail
THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
JDBC_ROOT="$(cd "${THIS_DIR}/.." && pwd)"

mkdir -p $THIS_DIR/target

# Setup GPG home directory if running in GitHub Actions
if [[ -n "$GITHUB_ACTIONS" ]]; then
    source $JDBC_ROOT/ci/scripts/setup_gpg.sh

    # Select the encrypted files based on CLOUD_PROVIDER
    if [[ "$CLOUD_PROVIDER" == "AZURE" ]]; then
        ENCODED_PARAMETERS_FILE=.github/workflows/parameters_azure.json.gpg
        ENCODED_RSA_KEY_FILE=.github/workflows/rsa_keys/rsa_key_jdbc_azure.p8.gpg
    elif [[ "$CLOUD_PROVIDER" == "GCP" ]]; then
        ENCODED_PARAMETERS_FILE=.github/workflows/parameters_gcp.json.gpg
        ENCODED_RSA_KEY_FILE=.github/workflows/rsa_keys/rsa_key_jdbc_gcp.p8.gpg
    elif [[ "$CLOUD_PROVIDER" == "AWS" ]]; then
        ENCODED_PARAMETERS_FILE=.github/workflows/parameters_aws.json.gpg
        ENCODED_RSA_KEY_FILE=.github/workflows/rsa_keys/rsa_key_jdbc_aws.p8.gpg
    else
        echo "[ERROR] Unknown cloud provider: $CLOUD_PROVIDER"
        exit 1
    fi

    # Decrypt parameters file
    echo "[INFO] Decrypting parameters file for $CLOUD_PROVIDER"
    gpg --quiet --batch --yes --decrypt --passphrase="$PARAMETERS_SECRET" \
        --output "$JDBC_ROOT/parameters.json" "$JDBC_ROOT/$ENCODED_PARAMETERS_FILE"

    # Decrypt RSA key file
    echo "[INFO] Decrypting RSA key file for $CLOUD_PROVIDER"
    gpg --quiet --batch --yes --decrypt --passphrase="$JDBC_PRIVATE_KEY_SECRET" \
        --output "$THIS_DIR/target/rsa_key_jdbc.p8" "$JDBC_ROOT/$ENCODED_RSA_KEY_FILE"

    # Parse parameters.json and export as SNOWFLAKE_TEST_* environment variables
    echo "[INFO] Setting up environment variables from parameters.json"
    eval $(jq -r '.testconnection | to_entries | map("export \(.key)=\(.value|tostring)")|.[]' "$JDBC_ROOT/parameters.json")

    # Set RSA key authentication
    export SNOWFLAKE_TEST_PRIVATE_KEY_FILE="$THIS_DIR/target/rsa_key_jdbc.p8"
    export SNOWFLAKE_TEST_AUTHENTICATOR="SNOWFLAKE_JWT"

    # Print env vars (excluding sensitive ones)
    env | grep SNOWFLAKE_ | grep -v -E "(PASS|KEY|SECRET|TOKEN)" | sort
fi

echo "[INFO] Building fat jar"
cd "$JDBC_ROOT"
./mvnw package -DskipTests -Dmaven.test.skip=true --quiet

echo "[INFO] Compiling fat jar test app"
cd "$THIS_DIR"
../mvnw compile

echo "[INFO] Getting slf4j"
wget https://repo1.maven.org/maven2/org/slf4j/slf4j-api/2.0.17/slf4j-api-2.0.17.jar -P target

echo "[INFO] Running fat jar test app"
java -cp "target/classes:$JDBC_ROOT/target/snowflake-jdbc.jar:$THIS_DIR/target/slf4j-api-2.0.17.jar" net.snowflake.FatJarTestApp