/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * This interface defines Snowflake specific APIs to access the data
 * wrapped in the result set serializable object.
 */
public interface SnowflakeResultSetSerializable
{
  /**
   * Get ResultSet from the ResultSet Serializable object so that the user can
   * access the data.
   *
   * @return a ResultSet which represents for the data wrapped in the object
   */
  ResultSet getResultSet() throws SQLException;
}
