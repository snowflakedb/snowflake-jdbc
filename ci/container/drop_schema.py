#!/usr/bin/env python
#
# Create test schema
#
import os
import sys
import snowflake.connector

import sf_test_utils

test_schema = sf_test_utils.get_test_schema()
if not test_schema:
    sys.exit(0)

params = sf_test_utils.init_connection_params()

con = snowflake.connector.connect(**params)
con.cursor().execute("drop schema if exists {0}".format(test_schema))

sys.exit(0)
