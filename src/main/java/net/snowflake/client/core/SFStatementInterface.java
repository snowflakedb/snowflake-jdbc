/*
 * Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import net.snowflake.client.jdbc.SnowflakeBaseResultSet;

public interface SFStatementInterface {

  SnowflakeBaseResultSet createResultSet(SFBaseResultSet resultSet, Statement statement)
      throws SQLException;

  /**
   * Add a statement parameter
   *
   * <p>Make sure a property is not added more than once and the number of properties does not
   * exceed limit.
   *
   * @param propertyName property name
   * @param propertyValue property value
   * @throws SFException if too many parameters for a statement
   */
  void addProperty(String propertyName, Object propertyValue) throws SFException;

  /**
   * Describe a statement
   *
   * @param sql statement
   * @return metadata of statement including result set metadata and binding information
   * @throws SQLException if connection is already closed
   * @throws SFException if result set is null
   */
  SFStatementMetaData describe(String sql) throws SFException, SQLException;

  /**
   * Execute sql
   *
   * @param sql sql statement.
   * @param parametersBinding parameters to bind
   * @param caller the JDBC interface method that called this method, if any
   * @return whether there is result set or not
   * @throws SQLException if failed to execute sql
   * @throws SFException exception raised from Snowflake components
   * @throws SQLException if SQL error occurs
   */
  SFBaseResultSet execute(
      String sql, Map<String, ParameterBindingDTO> parametersBinding, CallingMethod caller)
      throws SQLException, SFException;

  void close();

  void cancel() throws SFException, SQLException;

  void executeSetProperty(String sql);

  boolean hasChildren();

  SFSessionInterface getSession();

  /**
   * Sets the result set to the next one, if available.
   *
   * @param current What to do with the current result. One of Statement.CLOSE_CURRENT_RESULT,
   *     Statement.CLOSE_ALL_RESULTS, or Statement.KEEP_CURRENT_RESULT
   * @return true if there is a next result and it's a result set false if there are no more
   *     results, or there is a next result and it's an update count
   * @throws SQLException if something fails while getting the next result
   */
  boolean getMoreResults(int current) throws SQLException;

  SFBaseResultSet getResultSet();

  enum CallingMethod {
    EXECUTE,
    EXECUTE_UPDATE,
    EXECUTE_QUERY
  }
}
