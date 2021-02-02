/*
 * Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

public abstract class SFBaseStatement {
  protected static final int MAX_STATEMENT_PARAMETERS = 1000;
  static final SFLogger logger = SFLoggerFactory.getLogger(SFBaseStatement.class);
  // statement level parameters
  protected final Map<String, Object> statementParametersMap = new HashMap<>();
  // timeout in seconds
  protected int queryTimeout = 0;

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
  public void addProperty(String propertyName, Object propertyValue) throws SFException {
    statementParametersMap.put(propertyName, propertyValue);

    // for query timeout, we implement it on client side for now
    if ("query_timeout".equalsIgnoreCase(propertyName)) {
      queryTimeout = (Integer) propertyValue;
    }

    // check if the number of session properties exceed limit
    if (statementParametersMap.size() > MAX_STATEMENT_PARAMETERS) {
      throw new SFException(ErrorCode.TOO_MANY_STATEMENT_PARAMETERS, MAX_STATEMENT_PARAMETERS);
    }
  }

  /**
   * Describe a statement
   *
   * @param sql statement
   * @return metadata of statement including result set metadata and binding information
   * @throws SQLException if connection is already closed
   * @throws SFException if result set is null
   */
  public abstract SFStatementMetaData describe(String sql) throws SFException, SQLException;

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
  public abstract SFBaseResultSet execute(
      String sql, Map<String, ParameterBindingDTO> parametersBinding, CallingMethod caller)
      throws SQLException, SFException;

  /**
   * Execute sql asynchronously
   *
   * @param sql sql statement.
   * @param parametersBinding parameters to bind
   * @param caller the JDBC interface method that called this method, if any
   * @return whether there is result set or not
   * @throws SQLException if failed to execute sql
   * @throws SFException exception raised from Snowflake components
   * @throws SQLException if SQL error occurs
   */
  public abstract SFBaseResultSet asyncExecute(
      String sql, Map<String, ParameterBindingDTO> parametersBinding, CallingMethod caller)
      throws SQLException, SFException;

  public abstract void close();

  public abstract void cancel() throws SFException, SQLException;

  public void executeSetProperty(final String sql) {
    logger.debug("setting property");

    // tokenize the sql
    String[] tokens = sql.split("\\s+");

    if (tokens.length < 2) {
      return;
    }

    if ("sort".equalsIgnoreCase(tokens[1])) {
      if (tokens.length >= 3 && "on".equalsIgnoreCase(tokens[2])) {
        logger.debug("setting sort on");

        this.getSFBaseSession().setSessionPropertyByKey("sort", true);
      } else {
        logger.debug("setting sort off");
        this.getSFBaseSession().setSessionPropertyByKey("sort", false);
      }
    }
  }

  public abstract boolean hasChildren();

  public abstract SFBaseSession getSFBaseSession();

  public abstract SFBaseResultSet getResultSet();

  public abstract boolean getMoreResults(int current) throws SQLException;

  public enum CallingMethod {
    EXECUTE,
    EXECUTE_UPDATE,
    EXECUTE_QUERY
  }
}
