#!/usr/bin/env python
#
# Snowflake test utils
#
import os
import sys

def get_test_schema():
    return os.getenv("TARGET_SCHEMA_NAME", "LOCAL_reg_1")


def init_connection_params():
    params = {
        'account': os.getenv("SNOWFLAKE_TEST_ACCOUNT"),
        'user': os.getenv("SNOWFLAKE_TEST_USER"),
        'database': os.getenv("SNOWFLAKE_TEST_DATABASE"),
        'role': os.getenv("SNOWFLAKE_TEST_ROLE"),
    }
    
    private_key_file = os.getenv("SNOWFLAKE_TEST_PRIVATE_KEY_FILE")
    if private_key_file:
        workspace = os.getenv("WORKSPACE")
        if workspace:
            key_path = os.path.join(workspace, private_key_file)
        else:
            key_path = private_key_file
            
        try:
            from cryptography.hazmat.primitives import serialization
            from cryptography.hazmat.primitives.serialization import load_pem_private_key
            from cryptography.hazmat.backends import default_backend
            
            with open(key_path, 'rb') as key_file:
                pem_data = key_file.read()
            
            private_key_pwd = os.getenv("SNOWFLAKE_TEST_PRIVATE_KEY_PWD")
            private_key_pwd_bytes = None
            if private_key_pwd:
                private_key_pwd_bytes = private_key_pwd.encode('utf-8')
                
            private_key_obj = load_pem_private_key(pem_data, password=private_key_pwd_bytes, backend=default_backend())
            der_data = private_key_obj.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            )
            
            params['private_key'] = der_data
            params['authenticator'] = 'SNOWFLAKE_JWT'
            
            if private_key_pwd:
                params['private_key_pwd'] = private_key_pwd
                
        except Exception as e:
            print(f"ERROR: Failed to read private key file {key_path}: {e}")
            sys.exit(1)
    else:
        params['password'] = os.getenv("SNOWFLAKE_TEST_PASSWORD")
    host = os.getenv("SNOWFLAKE_TEST_HOST")
    if host:
        params['host'] = host
    port = os.getenv("SNOWFLAKE_TEST_PORT")
    if port:
        params['port'] = port
    protocol = os.getenv("SNOWFLAKE_TEST_PROTOCOL")
    if protocol:
        params['protocol'] = protocol
    warehouse = os.getenv("SNOWFLAKE_TEST_WAREHOUSE")
    if warehouse:
        params['warehouse'] = warehouse

    return params
