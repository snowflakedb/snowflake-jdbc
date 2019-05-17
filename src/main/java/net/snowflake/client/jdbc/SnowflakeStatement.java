/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import java.sql.SQLException;
import java.util.List;
import java.sql.Statement;

/**
 * This interface defines Snowflake specific APIs for Statement
 */
public interface SnowflakeStatement
{
  /**
   * @return the Snowflake query ID of the latest executed query
   */
  String getQueryID() throws SQLException;

  /**
   * @return the Snowflake query IDs of the latest executed batch queries
   */
  List<String> getBatchQueryIDs() throws SQLException;
}
