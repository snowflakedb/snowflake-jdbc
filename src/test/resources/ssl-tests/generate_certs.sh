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
TRUST_STORE_ALIAS_ROOT_CA1_SELF_SIGNED="rootca1_self_signed_for_ts"

# --- Define Paths for ALL Certificates in this Scenario ---
ST_CLASS2_ROOT_KEY="$CERT_DIR/st_class2_root.key"
ST_CLASS2_ROOT_CRT="$CERT_DIR/st_class2_root.crt"
ST_CLASS2_ROOT_CSR="$CERT_DIR/st_class2_root.csr"
ST_CLASS2_ROOT_SRL="$CERT_DIR/st_class2_root.srl"
ST_CLASS2_ROOT_SUBJ="/OU=ST Class 2 Certification Authority/O=ST Technologies, Inc./C=US"

ST_G2_ROOT_KEY="$CERT_DIR/st_g2_root.key"
ST_G2_ROOT_CRT="$CERT_DIR/st_g2_root.crt"
ST_G2_ROOT_CSR="$CERT_DIR/st_g2_root.csr"
ST_G2_ROOT_SRL="$CERT_DIR/st_g2_root.srl"
ST_G2_ROOT_SUBJ="/C=US/ST=Arizona/L=Scottsdale/O=ST Technologies, Inc./CN=ST Services Root Certificate Authority - G2"

AMZ_ROOT_CA1_COMMON_KEY="$CERT_DIR/amz_root_ca1_common.key"
AMZ_ROOT_CA1_SUBJ="/C=US/O=Amz/CN=Amz Root CA 1"

AMZ_ROOT_CA1_CHAIN_CRT="$CERT_DIR/amz_root_ca1_chain.crt"
AMZ_ROOT_CA1_CHAIN_CSR="$CERT_DIR/amz_root_ca1_chain.csr"
AMZ_ROOT_CA1_CHAIN_SRL="$CERT_DIR/amz_root_ca1_chain.srl"
AMZ_ROOT_CA1_CHAIN_KEY="$AMZ_ROOT_CA1_COMMON_KEY" # This variable's definition is fine, but we'll bypass its usage for CAkey

AMZ_RSA_M02_INTERMEDIATE_KEY="$CERT_DIR/amz_rsa_m02_intermediate.key"
AMZ_RSA_M02_INTERMEDIATE_CSR="$CERT_DIR/amz_rsa_m02_intermediate.csr"
AMZ_RSA_M02_INTERMEDIATE_CRT="$CERT_DIR/amz_rsa_m02_intermediate.crt"
AMZ_RSA_M02_INTERMEDIATE_SRL="$CERT_DIR/amz_rsa_m02_intermediate.srl"
AMZ_RSA_M02_SUBJ="/C=US/O=Amz/CN=Amz RSA 2048 M02"

LEAF_KEY="$CERT_DIR/leaf.key"
LEAF_CSR="$CERT_DIR/leaf.csr"
LEAF_CRT="$CERT_DIR/leaf.crt"
LEAF_SUBJ="/CN=*.prod3.us-west-2.exampledata.com"
LEAF_SANS="DNS:*.prod3.us-west-2.exampledata.com,DNS:*.us-west-2.exampledata.com,DNS:*.global.exampledata.com,DNS:*.exampledata.com,DNS:*.prod3.us-west-2.cloud.exampledata.com"

AMZ_ROOT_CA1_TRUST_STORE_CRT="$CERT_DIR/amz_root_ca1_trust_store.crt"
AMZ_ROOT_CA1_TRUST_STORE_CSR="$CERT_DIR/amz_root_ca1_trust_store.csr"
AMZ_ROOT_CA1_TRUST_STORE_SRL="$CERT_DIR/amz_root_ca1_trust_store.srl"

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


# --- 1. Generate ST Class 2 Root (Root of the Chain) ---
echo "--- Starting generation of ST Class 2 Root: $ST_CLASS2_ROOT_CRT"
echo "--- Generating private key for ST Class 2 Root at: '$ST_CLASS2_ROOT_KEY'"
openssl genrsa -out "$ST_CLASS2_ROOT_KEY" "$KEY_BITS"
echo "--- Finished generating private key for ST Class 2 Root."
cat > "$EXT_CONF_CA" << EOF
[ v3_ca ]
basicConstraints=CA:TRUE
keyUsage=critical,digitalSignature,cRLSign,keyCertSign
EOF
openssl req -new -key "$ST_CLASS2_ROOT_KEY" -out "$ST_CLASS2_ROOT_CSR" \
    -subj "$ST_CLASS2_ROOT_SUBJ"
echo "--- Signing self-signed ST Class 2 Root using key: '$ST_CLASS2_ROOT_KEY'"
openssl x509 -req -in "$ST_CLASS2_ROOT_CSR" -signkey "$ST_CLASS2_ROOT_KEY" -out "$ST_CLASS2_ROOT_CRT" -days "$DAYS_VALID" \
    -extfile "$EXT_CONF_CA" -extensions v3_ca
echo "--- Finished generating ST Class 2 Root."


# --- 2. Generate ST G2 Root (Cert 3 in chain) ---
echo "--- Starting generation of ST G2 Root: $ST_G2_ROOT_CRT"
echo "--- Generating private key for ST G2 Root at: '$ST_G2_ROOT_KEY'"
openssl genrsa -out "$ST_G2_ROOT_KEY" "$KEY_BITS"
openssl req -new -key "$ST_G2_ROOT_KEY" -out "$ST_G2_ROOT_CSR" \
    -subj "$ST_G2_ROOT_SUBJ"
echo "--- Signing ST G2 Root using CA private key: '$ST_CLASS2_ROOT_KEY'"
echo 01 > "$ST_CLASS2_ROOT_SRL"
openssl x509 -req -in "$ST_G2_ROOT_CSR" -CA "$ST_CLASS2_ROOT_CRT" -CAkey "$ST_CLASS2_ROOT_KEY" -CAcreateserial \
    -out "$ST_G2_ROOT_CRT" -days "$DAYS_VALID" -sha256 \
    -extfile "$EXT_CONF_CA" -extensions v3_ca
echo "--- Finished generating ST G2 Root."


# --- 3. Generate COMMON Private Key for Amz Root CA 1 Entity ---
echo "--- Generating COMMON Private Key for Amz Root CA 1 Entity at: '$AMZ_ROOT_CA1_COMMON_KEY'"
openssl genrsa -out "$AMZ_ROOT_CA1_COMMON_KEY" "$KEY_BITS"
echo "--- Finished generating common private key for Amz Root CA 1."


# --- 4. Generate Self-Signed Amz Root CA 1 (FOR TRUST STORE) ---
echo "--- Starting generation of Self-Signed Amz Root CA 1 (for Trust Store): $AMZ_ROOT_CA1_TRUST_STORE_CRT"
cat > "$EXT_CONF_CA" << EOF
[ v3_ca ]
basicConstraints=CA:TRUE
keyUsage=critical,digitalSignature,cRLSign,keyCertSign
EOF
openssl req -new -key "$AMZ_ROOT_CA1_COMMON_KEY" -out "$AMZ_ROOT_CA1_TRUST_STORE_CSR" \
    -subj "$AMZ_ROOT_CA1_SUBJ"
echo "--- Signing self-signed Amz Root CA 1 (Trust Store) using key: '$AMZ_ROOT_CA1_COMMON_KEY'"
openssl x509 -req -in "$AMZ_ROOT_CA1_TRUST_STORE_CSR" -signkey "$AMZ_ROOT_CA1_COMMON_KEY" -out "$AMZ_ROOT_CA1_TRUST_STORE_CRT" -days "$DAYS_VALID" \
    -extfile "$EXT_CONF_CA" -extensions v3_ca
echo "--- Finished generating self-signed Amz Root CA 1 (for Trust Store)."


# --- 5. Generate Amz Root CA 1 (FOR CHAIN - signed by ST G2, uses COMMON_KEY) ---
echo "--- Starting generation of Amz Root CA 1 (FOR CHAIN): $AMZ_ROOT_CA1_CHAIN_CRT"
openssl req -new -key "$AMZ_ROOT_CA1_COMMON_KEY" -out "$AMZ_ROOT_CA1_CHAIN_CSR" \
    -subj "$AMZ_ROOT_CA1_SUBJ"
echo "--- Signing Amz Root CA 1 (Chain) using CA private key: '$ST_G2_ROOT_KEY'"
echo 01 > "$ST_G2_ROOT_SRL"
openssl x509 -req -in "$AMZ_ROOT_CA1_CHAIN_CSR" -CA "$ST_G2_ROOT_CRT" -CAkey "$ST_G2_ROOT_KEY" -CAcreateserial \
    -out "$AMZ_ROOT_CA1_CHAIN_CRT" -days "$DAYS_VALID" -sha256 \
    -extfile "$EXT_CONF_CA" -extensions v3_ca
echo "--- Finished generating Amz Root CA 1 (chain version)."


# --- 6. Generate Amz RSA 2048 M02 (Intermediate) ---
echo "--- Starting generation of Amz RSA 2048 M02: $AMZ_RSA_M02_INTERMEDIATE_CRT"
echo "--- Generating private key for Amz RSA M02 at: '$AMZ_RSA_M02_INTERMEDIATE_KEY'"
openssl genrsa -out "$AMZ_RSA_M02_INTERMEDIATE_KEY" "$KEY_BITS"
openssl req -new -key "$AMZ_RSA_M02_INTERMEDIATE_KEY" -out "$AMZ_RSA_M02_INTERMEDIATE_CSR" \
    -subj "$AMZ_RSA_M02_SUBJ"
# >>> FIX: Directly use the expanded path for the CA private key here <<<
echo "--- Signing Amz RSA M02 using CA private key: '$AMZ_ROOT_CA1_COMMON_KEY' (bypassing variable issue)" # Debugging
echo 01 > "$AMZ_ROOT_CA1_CHAIN_SRL"
openssl x509 -req -in "$AMZ_RSA_M02_INTERMEDIATE_CSR" -CA "$AMZ_ROOT_CA1_CHAIN_CRT" -CAkey "$AMZ_ROOT_CA1_COMMON_KEY" -CAcreateserial \
    -out "$AMZ_RSA_M02_INTERMEDIATE_CRT" -days "$DAYS_VALID" -sha256 \
    -extfile "$EXT_CONF_CA" -extensions v3_ca
echo "--- Finished generating Amz RSA 2048 M02."


# --- 7. Generate Leaf ---
echo "--- Starting generation of Leaf: $LEAF_CRT"
echo "--- Generating private key for Leaf at: '$LEAF_KEY'"
openssl genrsa -out "$LEAF_KEY" "$KEY_BITS"
openssl req -new -key "$LEAF_KEY" -out "$LEAF_CSR" \
    -subj "$LEAF_SUBJ"

cat > "$LEAF_EXT_CONF" << EOF
[ v3_leaf_cert ]
basicConstraints=CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment # Corrected KeyUsage
extendedKeyUsage=serverAuth
subjectAltName=${LEAF_SANS}
EOF
echo 01 > "$AMZ_RSA_M02_INTERMEDIATE_SRL"
openssl x509 -req -in "$LEAF_CSR" -CA "$AMZ_RSA_M02_INTERMEDIATE_CRT" -CAkey "$AMZ_RSA_M02_INTERMEDIATE_KEY" -CAcreateserial \
    -out "$LEAF_CRT" -days "$DAYS_VALID" -sha256 \
    -extfile "$LEAF_EXT_CONF" -extensions v3_leaf_cert
echo "--- Finished generating Leaf."


# --- 8. Create Client Keystore (PKCS12) ---
echo "--- Creating client keystore: $CLIENT_KEYSTORE_FILE"
openssl pkcs12 -export \
    -in "$LEAF_CRT" \
    -inkey "$LEAF_KEY" \
    -name "$CLIENT_KEY_ALIAS" \
    -out "$CLIENT_KEYSTORE_FILE" \
    -passout pass:"$CLIENT_KEYSTORE_PASSWORD" \
    -chain \
    -CAfile <(cat "$AMZ_RSA_M02_INTERMEDIATE_CRT" "$AMZ_ROOT_CA1_CHAIN_CRT" "$ST_G2_ROOT_CRT" "$ST_CLASS2_ROOT_CRT")
echo "--- Client keystore created."


# --- 9. Create Trust Store (JKS) ---
echo "--- Creating trust store: $TRUST_STORE_FILE"
keytool -import -trustcacerts -alias "$TRUST_STORE_ALIAS_ROOT_CA1_SELF_SIGNED" -file "$AMZ_ROOT_CA1_TRUST_STORE_CRT" \
    -keystore "$TRUST_STORE_FILE" -storepass "$TRUST_STORE_PASSWORD" -noprompt
echo "--- Trust store created: Contains ONLY Self-Signed Amz Root CA 1 (for trust store)."


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