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
            params['private_key_file'] = workspace + "/" + private_key_file
        else:
            params['private_key_file'] = private_key_file
            
        params['authenticator'] = 'SNOWFLAKE_JWT'
        private_key_pwd = os.getenv("SNOWFLAKE_TEST_PRIVATE_KEY_PWD")
        if private_key_pwd:
            params['private_key_file_pwd'] = private_key_pwd
    else:
        # Fall back to password authentication
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
