/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import java.sql.SQLException;
import java.sql.ResultSet;

/**
 * This interface defines Snowflake specific APIs for ResultSet
 */
public interface SnowflakeResultSet
{
  /**
   * @return the Snowflake query ID of the query which generated this result set
   */
  String getQueryID() throws SQLException;
}
