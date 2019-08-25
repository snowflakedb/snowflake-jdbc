/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import java.util.List;
import java.sql.SQLException;

/**
 * This interface defines Snowflake specific APIs for ResultSet
 */
public interface SnowflakeResultSet
{
  /**
   * @return the Snowflake query ID of the query which generated this result set
   */
  String getQueryID() throws SQLException;

  /**
   * Get a list of ResultSetSerializables for the ResultSet in order to parallel processing
   *
   * @return a list of ResultSetSerializables
   */
  List<SnowflakeResultSetSerializable> getResultSetSerializables() throws SQLException;
}
