/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.jdbc;

import java.sql.SQLException;

public interface SnowflakeCallableStatement {
  String getQueryID() throws SQLException;
}
