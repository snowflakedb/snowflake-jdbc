#!/bin/bash
set -e # Exit immediately if a command exits with a non-zero status.
set -o pipefail # Added for robustness with pipes

# --- Script Configuration ---
# Get the directory where the script is located, resolving symlinks and relative paths.
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)

# Specific directory for certificates and keystores
CERT_DIR="$SCRIPT_DIR/certs"

echo "DEBUG: Script is running from: '$PWD'"
echo "DEBUG: Resolved SCRIPT_DIR: '$SCRIPT_DIR'"
echo "DEBUG: Resolved CERT_BASE_DIR: '$CERT_BASE_DIR'"
echo "DEBUG: Resolved CERT_DIR: '$CERT_DIR'"

# Certificate validity period (in days)
DAYS_VALID=3650 # ~10 years
# Key bit length for RSA keys
KEY_BITS=2048

# Passwords for keystores
CLIENT_KEYSTORE_PASSWORD="client_password"
TRUST_STORE_PASSWORD="changeit"

# Aliases for keystore entries
CLIENT_KEY_ALIAS="client_leaf_key"
TRUST_STORE_ALIAS_AMAZON_ROOT_CA1_SELF_SIGNED="amazonrootca1_self_signed_for_ts"

# --- Define Paths for ALL Certificates in this Scenario ---
STARFIELD_CLASS2_ROOT_KEY="$CERT_DIR/starfield_class2_root.key"
STARFIELD_CLASS2_ROOT_CRT="$CERT_DIR/starfield_class2_root.crt"
STARFIELD_CLASS2_ROOT_CSR="$CERT_DIR/starfield_class2_root.csr"
STARFIELD_CLASS2_ROOT_SRL="$CERT_DIR/starfield_class2_root.srl"
STARFIELD_CLASS2_ROOT_SUBJ="/OU=Starfield Class 2 Certification Authority/O=Starfield Technologies, Inc./C=US"

STARFIELD_G2_ROOT_KEY="$CERT_DIR/starfield_g2_root.key"
STARFIELD_G2_ROOT_CRT="$CERT_DIR/starfield_g2_root.crt"
STARFIELD_G2_ROOT_CSR="$CERT_DIR/starfield_g2_root.csr"
STARFIELD_G2_ROOT_SRL="$CERT_DIR/starfield_g2_root.srl"
STARFIELD_G2_ROOT_SUBJ="/C=US/ST=Arizona/L=Scottsdale/O=Starfield Technologies, Inc./CN=Starfield Services Root Certificate Authority - G2"

AMAZON_ROOT_CA1_COMMON_KEY="$CERT_DIR/amazon_root_ca1_common.key"
AMAZON_ROOT_CA1_SUBJ="/C=US/O=Amazon/CN=Amazon Root CA 1"

AMAZON_ROOT_CA1_CHAIN_CRT="$CERT_DIR/amazon_root_ca1_chain.crt"
AMAZON_ROOT_CA1_CHAIN_CSR="$CERT_DIR/amazon_root_ca1_chain.csr"
AMAZON_ROOT_CA1_CHAIN_SRL="$CERT_DIR/amazon_root_ca1_chain.srl"
AMAZON_ROOT_CA1_CHAIN_KEY="$AMAZON_ROOT_CA1_COMMON_KEY" # This variable's definition is fine, but we'll bypass its usage for CAkey

AMAZON_RSA_M02_INTERMEDIATE_KEY="$CERT_DIR/amazon_rsa_m02_intermediate.key"
AMAZON_RSA_M02_INTERMEDIATE_CSR="$CERT_DIR/amazon_rsa_m02_intermediate.csr"
AMAZON_RSA_M02_INTERMEDIATE_CRT="$CERT_DIR/amazon_rsa_m02_intermediate.crt"
AMAZON_RSA_M02_INTERMEDIATE_SRL="$CERT_DIR/amazon_rsa_m02_intermediate.srl"
AMAZON_RSA_M02_SUBJ="/C=US/O=Amazon/CN=Amazon RSA 2048 M02"

SNOWFLAKE_LEAF_KEY="$CERT_DIR/snowflake_leaf.key"
SNOWFLAKE_LEAF_CSR="$CERT_DIR/snowflake_leaf.csr"
SNOWFLAKE_LEAF_CRT="$CERT_DIR/snowflake_leaf.crt"
SNOWFLAKE_LEAF_SUBJ="/CN=*.prod3.us-west-2.snowflakecomputing.com"
SNOWFLAKE_LEAF_SANS="DNS:*.prod3.us-west-2.snowflakecomputing.com,DNS:*.us-west-2.snowflakecomputing.com,DNS:*.global.snowflakecomputing.com,DNS:*.snowflakecomputing.com,DNS:*.prod3.us-west-2.aws.snowflakecomputing.com"

AMAZON_ROOT_CA1_TRUST_STORE_CRT="$CERT_DIR/amazon_root_ca1_trust_store.crt"
AMAZON_ROOT_CA1_TRUST_STORE_CSR="$CERT_DIR/amazon_root_ca1_trust_store.csr"
AMAZON_ROOT_CA1_TRUST_STORE_SRL="$CERT_DIR/amazon_root_ca1_trust_store.srl"

CLIENT_KEYSTORE_FILE="$CERT_DIR/client_keystore.p12"
TRUST_STORE_FILE="$CERT_DIR/truststore.jks"

EXT_CONF_CA="$CERT_DIR/ext_config_ca.cnf"
LEAF_EXT_CONF="$CERT_DIR/leaf_ext_conf.cnf"


# --- Cleanup Previous Run ---
echo "--- Cleaning up existing test certificates and keystores in '$CERT_BASE_DIR'..."
rm -rf "$CERT_BASE_DIR"
mkdir -p "$CERT_DIR"
echo "--- Current CERT_DIR after creation: '$CERT_DIR'"
echo "--- Clean up complete."


# --- 1. Generate Starfield Class 2 Root (Root of the Chain) ---
echo "--- Starting generation of Starfield Class 2 Root: $STARFIELD_CLASS2_ROOT_CRT"
echo "--- Generating private key for Starfield Class 2 Root at: '$STARFIELD_CLASS2_ROOT_KEY'"
openssl genrsa -out "$STARFIELD_CLASS2_ROOT_KEY" "$KEY_BITS"
echo "--- Finished generating private key for Starfield Class 2 Root."
cat > "$EXT_CONF_CA" << EOF
[ v3_ca ]
basicConstraints=CA:TRUE
keyUsage=critical,digitalSignature,cRLSign,keyCertSign
EOF
openssl req -new -key "$STARFIELD_CLASS2_ROOT_KEY" -out "$STARFIELD_CLASS2_ROOT_CSR" \
    -subj "$STARFIELD_CLASS2_ROOT_SUBJ"
echo "--- Signing self-signed Starfield Class 2 Root using key: '$STARFIELD_CLASS2_ROOT_KEY'"
openssl x509 -req -in "$STARFIELD_CLASS2_ROOT_CSR" -signkey "$STARFIELD_CLASS2_ROOT_KEY" -out "$STARFIELD_CLASS2_ROOT_CRT" -days "$DAYS_VALID" \
    -extfile "$EXT_CONF_CA" -extensions v3_ca
echo "--- Finished generating Starfield Class 2 Root."


# --- 2. Generate Starfield G2 Root (Cert 3 in chain) ---
echo "--- Starting generation of Starfield G2 Root: $STARFIELD_G2_ROOT_CRT"
echo "--- Generating private key for Starfield G2 Root at: '$STARFIELD_G2_ROOT_KEY'"
openssl genrsa -out "$STARFIELD_G2_ROOT_KEY" "$KEY_BITS"
openssl req -new -key "$STARFIELD_G2_ROOT_KEY" -out "$STARFIELD_G2_ROOT_CSR" \
    -subj "$STARFIELD_G2_ROOT_SUBJ"
echo "--- Signing Starfield G2 Root using CA private key: '$STARFIELD_CLASS2_ROOT_KEY'"
echo 01 > "$STARFIELD_CLASS2_ROOT_SRL"
openssl x509 -req -in "$STARFIELD_G2_ROOT_CSR" -CA "$STARFIELD_CLASS2_ROOT_CRT" -CAkey "$STARFIELD_CLASS2_ROOT_KEY" -CAcreateserial \
    -out "$STARFIELD_G2_ROOT_CRT" -days "$DAYS_VALID" -sha256 \
    -extfile "$EXT_CONF_CA" -extensions v3_ca
echo "--- Finished generating Starfield G2 Root."


# --- 3. Generate COMMON Private Key for Amazon Root CA 1 Entity ---
echo "--- Generating COMMON Private Key for Amazon Root CA 1 Entity at: '$AMAZON_ROOT_CA1_COMMON_KEY'"
openssl genrsa -out "$AMAZON_ROOT_CA1_COMMON_KEY" "$KEY_BITS"
echo "--- Finished generating common private key for Amazon Root CA 1."


# --- 4. Generate Self-Signed Amazon Root CA 1 (FOR TRUST STORE) ---
echo "--- Starting generation of Self-Signed Amazon Root CA 1 (for Trust Store): $AMAZON_ROOT_CA1_TRUST_STORE_CRT"
cat > "$EXT_CONF_CA" << EOF
[ v3_ca ]
basicConstraints=CA:TRUE
keyUsage=critical,digitalSignature,cRLSign,keyCertSign
EOF
openssl req -new -key "$AMAZON_ROOT_CA1_COMMON_KEY" -out "$AMAZON_ROOT_CA1_TRUST_STORE_CSR" \
    -subj "$AMAZON_ROOT_CA1_SUBJ"
echo "--- Signing self-signed Amazon Root CA 1 (Trust Store) using key: '$AMAZON_ROOT_CA1_COMMON_KEY'"
openssl x509 -req -in "$AMAZON_ROOT_CA1_TRUST_STORE_CSR" -signkey "$AMAZON_ROOT_CA1_COMMON_KEY" -out "$AMAZON_ROOT_CA1_TRUST_STORE_CRT" -days "$DAYS_VALID" \
    -extfile "$EXT_CONF_CA" -extensions v3_ca
echo "--- Finished generating self-signed Amazon Root CA 1 (for Trust Store)."


# --- 5. Generate Amazon Root CA 1 (FOR CHAIN - signed by Starfield G2, uses COMMON_KEY) ---
echo "--- Starting generation of Amazon Root CA 1 (FOR CHAIN): $AMAZON_ROOT_CA1_CHAIN_CRT"
openssl req -new -key "$AMAZON_ROOT_CA1_COMMON_KEY" -out "$AMAZON_ROOT_CA1_CHAIN_CSR" \
    -subj "$AMAZON_ROOT_CA1_SUBJ"
echo "--- Signing Amazon Root CA 1 (Chain) using CA private key: '$STARFIELD_G2_ROOT_KEY'"
echo 01 > "$STARFIELD_G2_ROOT_SRL"
openssl x509 -req -in "$AMAZON_ROOT_CA1_CHAIN_CSR" -CA "$STARFIELD_G2_ROOT_CRT" -CAkey "$STARFIELD_G2_ROOT_KEY" -CAcreateserial \
    -out "$AMAZON_ROOT_CA1_CHAIN_CRT" -days "$DAYS_VALID" -sha256 \
    -extfile "$EXT_CONF_CA" -extensions v3_ca
echo "--- Finished generating Amazon Root CA 1 (chain version)."


# --- 6. Generate Amazon RSA 2048 M02 (Intermediate) ---
echo "--- Starting generation of Amazon RSA 2048 M02: $AMAZON_RSA_M02_INTERMEDIATE_CRT"
echo "--- Generating private key for Amazon RSA M02 at: '$AMAZON_RSA_M02_INTERMEDIATE_KEY'"
openssl genrsa -out "$AMAZON_RSA_M02_INTERMEDIATE_KEY" "$KEY_BITS"
openssl req -new -key "$AMAZON_RSA_M02_INTERMEDIATE_KEY" -out "$AMAZON_RSA_M02_INTERMEDIATE_CSR" \
    -subj "$AMAZON_RSA_M02_SUBJ"
# >>> FIX: Directly use the expanded path for the CA private key here <<<
echo "--- Signing Amazon RSA M02 using CA private key: '$AMAZON_ROOT_CA1_COMMON_KEY' (bypassing variable issue)" # Debugging
echo 01 > "$AMAZON_ROOT_CA1_CHAIN_SRL"
openssl x509 -req -in "$AMAZON_RSA_M02_INTERMEDIATE_CSR" -CA "$AMAZON_ROOT_CA1_CHAIN_CRT" -CAkey "$AMAZON_ROOT_CA1_COMMON_KEY" -CAcreateserial \
    -out "$AMAZON_RSA_M02_INTERMEDIATE_CRT" -days "$DAYS_VALID" -sha256 \
    -extfile "$EXT_CONF_CA" -extensions v3_ca
echo "--- Finished generating Amazon RSA 2048 M02."


# --- 7. Generate Snowflake Leaf ---
echo "--- Starting generation of Snowflake Leaf: $SNOWFLAKE_LEAF_CRT"
echo "--- Generating private key for Snowflake Leaf at: '$SNOWFLAKE_LEAF_KEY'"
openssl genrsa -out "$SNOWFLAKE_LEAF_KEY" "$KEY_BITS"
openssl req -new -key "$SNOWFLAKE_LEAF_KEY" -out "$SNOWFLAKE_LEAF_CSR" \
    -subj "$SNOWFLAKE_LEAF_SUBJ"

cat > "$LEAF_EXT_CONF" << EOF
[ v3_leaf_cert ]
basicConstraints=CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment # Corrected KeyUsage
extendedKeyUsage=serverAuth
subjectAltName=${SNOWFLAKE_LEAF_SANS}
EOF
echo 01 > "$AMAZON_RSA_M02_INTERMEDIATE_SRL"
openssl x509 -req -in "$SNOWFLAKE_LEAF_CSR" -CA "$AMAZON_RSA_M02_INTERMEDIATE_CRT" -CAkey "$AMAZON_RSA_M02_INTERMEDIATE_KEY" -CAcreateserial \
    -out "$SNOWFLAKE_LEAF_CRT" -days "$DAYS_VALID" -sha256 \
    -extfile "$LEAF_EXT_CONF" -extensions v3_leaf_cert
echo "--- Finished generating Snowflake Leaf."


# --- 8. Create Client Keystore (PKCS12) ---
echo "--- Creating client keystore: $CLIENT_KEYSTORE_FILE"
openssl pkcs12 -export \
    -in "$SNOWFLAKE_LEAF_CRT" \
    -inkey "$SNOWFLAKE_LEAF_KEY" \
    -name "$CLIENT_KEY_ALIAS" \
    -out "$CLIENT_KEYSTORE_FILE" \
    -passout pass:"$CLIENT_KEYSTORE_PASSWORD" \
    -chain \
    -CAfile <(cat "$AMAZON_RSA_M02_INTERMEDIATE_CRT" "$AMAZON_ROOT_CA1_CHAIN_CRT" "$STARFIELD_G2_ROOT_CRT" "$STARFIELD_CLASS2_ROOT_CRT")
echo "--- Client keystore created."


# --- 9. Create Trust Store (JKS) ---
echo "--- Creating trust store: $TRUST_STORE_FILE"
keytool -import -trustcacerts -alias "$TRUST_STORE_ALIAS_AMAZON_ROOT_CA1_SELF_SIGNED" -file "$AMAZON_ROOT_CA1_TRUST_STORE_CRT" \
    -keystore "$TRUST_STORE_FILE" -storepass "$TRUST_STORE_PASSWORD" -noprompt
echo "--- Trust store created: Contains ONLY Self-Signed Amazon Root CA 1 (for trust store)."


# --- Final Verification ---
echo "--- Generation Complete ---"
echo "All certificates and keystores are located in a '$CERT_DIR' directory under your project's test resources."
echo ""
echo "Verifying client keystore content:"
keytool -list -v -keystore "$CLIENT_KEYSTORE_FILE" -storetype PKCS12 -storepass "$CLIENT_KEYSTORE_PASSWORD" | grep "Alias name"
echo ""
echo "Verifying trust store content:"
keytool -list -v -keystore "$TRUST_STORE_FILE" -storepass "$TRUST_STORE_PASSWORD" | grep "Alias name"


# --- Cleanup Temporary Files ---
echo "--- Cleaning up temporary OpenSSL config files..."
rm "$EXT_CONF_CA" "$LEAF_EXT_CONF"
echo "--- Cleanup of temporary files complete."

echo "You can now run your Java tests."