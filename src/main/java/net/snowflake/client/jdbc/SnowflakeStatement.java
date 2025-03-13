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
   * @throws SQLException if an error is encountered
   */
  String getQueryID() throws SQLException;

  /**
   * @return the Snowflake query IDs of the latest executed batch queries
   * @throws SQLException if an error is encountered
   */
  List<String> getBatchQueryIDs() throws SQLException;

  /**
   * Set statement level parameter
   *
   * @param name parameter name
   * @param value parameter value
   * @throws SQLException if an error is encountered
   */
  void setParameter(String name, Object value) throws SQLException;

  /**
   * Set batch ID
   *
   * @param batchID the batch ID
   */
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
   * @param resultSet SFBaseResultSet
   * @throws SQLException if an error is encountered
   */
  void resultSetMetadataHandler(SFBaseResultSet resultSet) throws SQLException;

  /**
   * Sets the query timeout when running an async query.
   *
   * @param seconds The number of seconds until timeout.
   * @throws SQLException if an error is encountered
   */
  void setAsyncQueryTimeout(int seconds) throws SQLException;
}
