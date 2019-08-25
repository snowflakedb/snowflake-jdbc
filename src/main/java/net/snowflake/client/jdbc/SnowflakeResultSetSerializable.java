/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface SnowflakeResultSetSerializable
{
  /**
   * Get ResultSet for the ResultSet chunk so that the user can access its data.
   *
   * @return a list of ResultSetChunk
   */
  ResultSet getResultSet() throws SQLException;
}
