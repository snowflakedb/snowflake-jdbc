#!/usr/bin/env python
#
# Drop test schema
#
import os
import sys
import snowflake.connector

test_schema = None

if 'TRAVIS_JOB_ID' in os.environ:
    job_id = os.getenv('TRAVIS_JOB_ID')
    test_schema = 'TRAVIS_JOB_{0}'.format(job_id)
elif 'APPVEYOR_BUILD_ID' in os.environ:
    job_id = os.getenv('APPVEYOR_BUILD_ID')
    test_schema = 'APPVEYOR_BUILD_{0}'.format(job_id)
else:
    test_schema = os.getenv('SNOWFLAKE_TEST_SCHEMA')

params = {
    'account': os.getenv("SNOWFLAKE_TEST_ACCOUNT"),
    'user': os.getenv("SNOWFLAKE_TEST_USER"),
    'password': os.getenv("SNOWFLAKE_TEST_PASSWORD"),
    'database': os.getenv("SNOWFLAKE_TEST_DATABASE"),
    'role': os.getenv("SNOWFLAKE_TEST_ROLE"),
}
host=os.getenv("SNOWFLAKE_TEST_HOST")
if host:
    params['host'] = host
port=os.getenv("SNOWFLAKE_TEST_PORT")
if port:
    params['port'] = port
protocol=os.getenv("SNOWFLAKE_TEST_PROTOCOL")
if protocol:
    params['protocol'] = protocol

if not test_schema.lower() in ['testschema', 'public']:
    con = snowflake.connector.connect(**params)
    cmd = "drop schema if exists {0}".format(test_schema)
    print(cmd)
    con.cursor().execute(cmd)

sys.exit(0)
