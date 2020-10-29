/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

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

  /**
   * Set statement level parameter
   *
   * @param name  parameter name
   * @param value parameter value
   */
  void setParameter(String name, Object value) throws SQLException;

  /**
   * Execute SQL query asynchronously
   *
   * @param sql sql statement
   * @return ResultSet
   * @throws SQLException if @link{#executeQueryInternal(String, Map)} throws an exception
   */
  ResultSet executeAsyncQuery(String sql) throws SQLException;
}
