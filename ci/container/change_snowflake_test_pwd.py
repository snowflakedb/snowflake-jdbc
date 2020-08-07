#!/usr/bin/env python
#
# Set a complex password for test user snowman
#

import os
import sys
import snowflake.connector

params = {
    'account': '',
    'user': os.getenv("SNOWFLAKE_TEST_USER"),
    'password': os.getenv("SNOWFLAKE_TEST_PASSWORD"),
    'database': os.getenv("SNOWFLAKE_TEST_DATABASE"),
    'role': os.getenv("SNOWFLAKE_TEST_ROLE"),
    'host': os.getenv("SNOWFLAKE_TEST_HOST"),
    'port': os.getenv("SNOWFLAKE_TEST_PORT"),
    'protocol': os.getenv("SNOWFLAKE_TEST_PROTOCOL"),
}

for account in ["testaccount", "s3testaccount", "azureaccount", "gcpaccount"]:
   params['account'] = account
   conn = snowflake.connector.connect(**params)
   conn.cursor().execute("use role accountadmin")
   cmd = "alter user set password = '{}'".format(os.getenv("SNOWFLAKE_TEST_PASSWORD_NEW"))
   print(cmd)
   conn.cursor().execute(cmd)
   conn.close()

