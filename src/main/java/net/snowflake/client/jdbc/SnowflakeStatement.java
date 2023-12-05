/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import net.snowflake.client.core.SFBaseResultSet;

/** This interface defines Snowflake specific APIs for Statement */
public interface SnowflakeStatement {
  /**
   * @return the Snowflake query ID of the latest executed query (even failed one) or null when the
   *     last query ID is not available
   */
  String getQueryID() throws SQLException;

  /**
   * @return the Snowflake query IDs of the latest executed batch queries
   */
  List<String> getBatchQueryIDs() throws SQLException;

  /**
   * Set statement level parameter
   *
   * @param name parameter name
   * @param value parameter value
   */
  void setParameter(String name, Object value) throws SQLException;

  void setBatchID(String batchID);

  /**
   * Execute SQL query asynchronously
   *
   * @param sql sql statement
   * @return ResultSet
   * @throws SQLException if @link{#executeQueryInternal(String, Map)} throws an exception
   */
  ResultSet executeAsyncQuery(String sql) throws SQLException;

  /**
   * This method exposes SFBaseResultSet to the sub-classes of SnowflakeStatementV1.java. This is
   * required as SnowflakeStatementV1 doesn't directly expose ResultSet to the sub-classes making it
   * challenging to get additional information from the previously executed query.
   *
   * @param resultSet
   * @throws SQLException
   */
  void resultSetMetadataHandler(SFBaseResultSet resultSet) throws SQLException;
}
