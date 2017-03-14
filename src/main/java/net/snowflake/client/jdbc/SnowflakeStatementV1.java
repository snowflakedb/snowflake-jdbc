/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import net.snowflake.client.core.ResultUtil;
import net.snowflake.client.core.SFBaseResultSet;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SFStatement;
import net.snowflake.client.core.SFStatementType;
import net.snowflake.common.core.SqlState;

import java.sql.ResultSetMetaData;
import java.util.List;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Map;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

/**
 * Snowflake statement
 *
 * @author jhuang
 */
public class SnowflakeStatementV1 implements Statement
{

  static final SFLogger logger = SFLoggerFactory.getLogger(SnowflakeStatementV1.class);

  private SnowflakeConnectionV1 connection;

  /*
   * The maximum number of rows this statement ( should return (0 => all rows).
   */
  private int maxRows = 0;

  private ResultSet resultSet = null;

  private ResultSet currentResultSet = null;

  private int fetchSize = 50;

  private Boolean isClosed = false;

  private int updateCount = -1;

  // TODO: escape processing for sql statement
  private boolean escapeProcessing = false;

  // timeout in seconds
  private int queryTimeout = 0;

  // max field size limited to 16MB
  private int maxFieldSize = 16777216;

  private SFStatement sfStatement;

  public SnowflakeStatementV1(SnowflakeConnectionV1 conn)
  {
    logger.debug(
               " public SnowflakeStatement(SnowflakeConnectionV1 conn)");

    connection = conn;
    sfStatement = new SFStatement(conn.getSfSession());
  }

  /**
   * Execute SQL query
   *
   * @param sql sql statement
   * @return ResultSet
   * @throws java.sql.SQLException if @link{#executeQueryInternal(String, Map)} throws an exception
   */
  @Override
  public ResultSet executeQuery(String sql) throws SQLException
  {
    return executeQueryInternal(sql, null);

    /*
    try
    {
      return new SnowflakeResultSetV1(sfStatement.executeQuery(sql));
    }
    catch (SFException ex)
    {
      throw new SnowflakeSQLException(ex.getCause(),
          ex.getSqlState(), ex.getVendorCode(), ex.getParams());
    }
    */
  }

  /**
   * Execute an update statement
   *
   * @param sql sql statement
   * @return number of rows updated
   * @throws java.sql.SQLException if @link{#executeUpdateInternal(String, Map)} throws exception
   */
  @Override
  public int executeUpdate(String sql) throws SQLException
  {
    return executeUpdateInternal(sql, null);
  }

  public int executeUpdateInternal(String sql,
                                   Map<String, Map<String, Object>> parameterBindings)
          throws SQLException
  {
    if (parameterBindings != null)
    {
      for (Map.Entry<String, Map<String, Object>> parameterBindingEntry :
          parameterBindings.entrySet())
      {
        if (parameterBindingEntry.getValue().get("value") instanceof String)
          sfStatement.setValue(parameterBindingEntry.getKey(),
              (String) parameterBindingEntry.getValue().get("value"),
              (String) parameterBindingEntry.getValue().get("type"));
        else
          sfStatement.setValues(parameterBindingEntry.getKey(),
              (List<String>) parameterBindingEntry.getValue().get("value"),
              (String) parameterBindingEntry.getValue().get("type"));
      }
    }

    SFBaseResultSet sfResultSet = null;
    try
    {
      sfResultSet = sfStatement.execute(sql);
      sfResultSet.setSession(this.connection.getSfSession());
    }
    catch (SFException ex)
    {
      throw new SnowflakeSQLException(ex.getCause(),
          ex.getSqlState(), ex.getVendorCode(), ex.getParams());
    }

    // sum up the number of rows updated based on the type of statements
    SnowflakeResultSetV1 rset = new SnowflakeResultSetV1(sfResultSet, this);

    updateCount = ResultUtil.calculateUpdateCount(rset, rset.getStatementTypeId());

    return updateCount;
  }

  /**
   * Internal method for executing a query with bindings accepted.
   *
   * @param sql sql statement
   * @param parameterBindings parameters bindings
   * @return query result set
   * @throws SQLException if @link{SFStatement.execute(String)} throws exception
   */
  protected ResultSet executeQueryInternal(
      String sql,
      Map<String, Map<String, Object>> parameterBindings)
      throws SQLException
  {
    resetState();

    if (parameterBindings != null)
    {
      for (Map.Entry<String, Map<String, Object>> parameterBindingEntry :
          parameterBindings.entrySet())
      {
        if (parameterBindingEntry.getValue().get("value") instanceof String)
          sfStatement.setValue(parameterBindingEntry.getKey(),
              (String) parameterBindingEntry.getValue().get("value"),
              (String) parameterBindingEntry.getValue().get("type"));
        else
          sfStatement.setValues(parameterBindingEntry.getKey(),
              (List<String>) parameterBindingEntry.getValue().get("value"),
              (String) parameterBindingEntry.getValue().get("type"));
      }
    }

    SFBaseResultSet sfResultSet = null;
    try
    {
      sfResultSet = sfStatement.execute(sql);
      sfResultSet.setSession(this.connection.getSfSession());
    }
    catch (SFException ex)
    {
      throw new SnowflakeSQLException(ex.getCause(),
          ex.getSqlState(), ex.getVendorCode(), ex.getParams());
    }

    resultSet = new SnowflakeResultSetV1(sfResultSet, this);

    // Fix a bug with getMoreResults returning true after a client
    // calling executeQuery which has returned the result set already
    return getResultSet();
  }

  /**
   * Internal method for describing a query with bindings accepted.
   *
   * @param sql sql statement
   * @param parameterBindings parameter bindings
   * @return query result set metadata
   * @throws SQLException if failed to construct snowflake result set metadata
   */
  protected ResultSetMetaData describeQueryInternal(
      String sql,
      Map<String, Map<String, Object>> parameterBindings)
      throws SQLException
  {
    if (parameterBindings != null)
    {
      for (Map.Entry<String, Map<String, Object>> parameterBindingEntry :
          parameterBindings.entrySet())
      {
        if (parameterBindingEntry.getValue().get("value") instanceof String)
          sfStatement.setValue(parameterBindingEntry.getKey(),
              (String) parameterBindingEntry.getValue().get("value"),
              (String) parameterBindingEntry.getValue().get("type"));
        else
          sfStatement.setValues(parameterBindingEntry.getKey(),
              (List<String>) parameterBindingEntry.getValue().get("value"),
              (String) parameterBindingEntry.getValue().get("type"));
      }
    }

    try
    {
      return new SnowflakeResultSetMetaDataV1(
          sfStatement.describe(sql).getResultSetMetaData());
    }
    catch (SFException ex)
    {
      throw new SnowflakeSQLException(ex.getCause(),
          ex.getSqlState(), ex.getVendorCode(), ex.getParams());
    }
  }

  /**
   * A method to check if a sql is file upload statement with consideration for
   * potential comments in front of put keyword.
   *
   * @param sql sql statement
   * @return true if sql is file upload statement
   */
  public boolean isFileTransfer(String sql)
  {
    if (sql == null)
    {
      return false;
    }

    String trimmedSql = sql.trim();

    // skip commenting prefixed with //
    while (trimmedSql.startsWith("//"))
    {
      logger.debug("skipping // comments in: \n{}", trimmedSql);

      if (trimmedSql.indexOf('\n') > 0)
      {
        trimmedSql = trimmedSql.substring(trimmedSql.indexOf('\n'));
        trimmedSql = trimmedSql.trim();
      }
      else
      {
        break;
      }

      logger.debug("New sql after skipping // comments: \n{}",
                               trimmedSql);

    }

    // skip commenting enclosed with /* */
    while (trimmedSql.startsWith("/*"))
    {
      logger.debug("skipping /* */ comments in: \n{}", trimmedSql);

      if (trimmedSql.indexOf("*/") > 0)
      {
        trimmedSql = trimmedSql.substring(trimmedSql.indexOf("*/") + 2);
        trimmedSql = trimmedSql.trim();
      }
      else
      {
        break;

      }
      logger.debug("New sql after skipping /* */ comments: \n{}",
                                trimmedSql);

    }

    return (trimmedSql.length() >= 4
            && (trimmedSql.toLowerCase().startsWith("put ")
                || trimmedSql.toLowerCase().startsWith("get ")));
  }

  /**
   * Sanity check query text
   * @param sql input sql
   * @throws SQLException if sql statement is not valid
   */
  void sanityCheckQuery(String sql) throws SQLException
  {
    if (sql == null || sql.isEmpty())
    {
      throw new SnowflakeSQLException(SqlState.SQL_STATEMENT_NOT_YET_COMPLETE,
          ErrorCode.INVALID_SQL.getMessageCode(), sql);

    }
  }

  /**
   * Execute sql
   *
   * @param sql sql statement
   * @return whether there is result set or not
   * @throws java.sql.SQLException if @link{#executeQuery(String)} throws exception
   */
  @Override
  public boolean execute(String sql) throws SQLException
  {
    sanityCheckQuery(sql);

    connection.injectedDelay();

    logger.debug("execute: " + sql);

    String trimmedSql = sql.trim();

    if (trimmedSql.length() >= 20
        && trimmedSql.toLowerCase().startsWith(
        "set-sf-property"))
    {
      executeSetProperty(sql);
      return false;
    }
    else
    {
      SnowflakeResultSetV1 rSet = (SnowflakeResultSetV1) executeQuery(sql);
      if (connection.getSfSession().isExecuteReturnCountForDML()){
        if (SFStatementType.isDML(rSet.getStatementTypeId()) ||
            SFStatementType.isDDL(rSet.getStatementTypeId())){
          updateCount =
          ResultUtil.calculateUpdateCount(rSet, rSet.getStatementTypeId());
          resultSet = null;
          currentResultSet = null;
          return false;
        }
      }
      return true;
    }
  }


  @Override
  public boolean execute(String sql, int autoGeneratedKeys)
          throws SQLException
  {
    logger.debug(
               "public int execute(String sql, int autoGeneratedKeys)");
    
    if (autoGeneratedKeys == Statement.NO_GENERATED_KEYS)
    {
      return execute(sql);
    }
    else
    {
      throw new SQLFeatureNotSupportedException();
    }
  }

  @Override
  public boolean execute(String sql, int[] columnIndexes) throws SQLException
  {
    logger.debug(
               "public boolean execute(String sql, int[] columnIndexes)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean execute(String sql, String[] columnNames) throws SQLException
  {
    logger.debug(
               "public boolean execute(String sql, String[] columnNames)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int[] executeBatch() throws SQLException
  {
    logger.debug("public int[] executeBatch()");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int executeUpdate(String sql, int autoGeneratedKeys)
          throws SQLException
  {
    logger.debug(
               "public int executeUpdate(String sql, int autoGeneratedKeys)");
    
    if (autoGeneratedKeys == Statement.NO_GENERATED_KEYS)
    {
      return executeUpdate(sql);
    }
    else
    {
      throw new SQLFeatureNotSupportedException();
    }
  }

  @Override
  public int executeUpdate(String sql, int[] columnIndexes)
          throws SQLException
  {
    logger.debug(
               "public int executeUpdate(String sql, int[] columnIndexes)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int executeUpdate(String sql, String[] columnNames)
          throws SQLException
  {
    logger.debug(
               "public int executeUpdate(String sql, String[] columnNames)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Connection getConnection() throws SQLException
  {
    logger.debug("public Connection getConnection()");

    return connection;
  }

  @Override
  public int getFetchDirection() throws SQLException
  {
    logger.debug("public int getFetchDirection()");

    return ResultSet.FETCH_FORWARD;
  }

  @Override
  public int getFetchSize() throws SQLException
  {
    logger.debug("public int getFetchSize()");

    return fetchSize;
  }

  @Override
  public ResultSet getGeneratedKeys() throws SQLException
  {
    logger.debug("public ResultSet getGeneratedKeys()");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getMaxFieldSize() throws SQLException
  {
    logger.debug("public int getMaxFieldSize()");

    return maxFieldSize;
  }

  @Override
  public int getMaxRows() throws SQLException
  {
    logger.debug("public int getMaxRows()");

    return maxRows;
  }

  @Override
  public boolean getMoreResults() throws SQLException
  {
    logger.debug("public boolean getMoreResults()");

    if (currentResultSet != null)
    {
      currentResultSet.close();
      currentResultSet = null;
    }

    if (resultSet != null)
    {
      currentResultSet = resultSet;
      resultSet = null;
      return true;
    }

    return false;
  }

  @Override
  public boolean getMoreResults(int current) throws SQLException
  {
    logger.debug("public boolean getMoreResults(int current)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getQueryTimeout() throws SQLException
  {
    logger.debug("public int getQueryTimeout()");

    return this.queryTimeout;
  }

  @Override
  public ResultSet getResultSet() throws SQLException
  {
    logger.debug("public ResultSet getResultSet()");

    if (currentResultSet == null)
    {
      currentResultSet = resultSet;
      resultSet = null;
    }

    return currentResultSet;
  }

  @Override
  public int getResultSetConcurrency() throws SQLException
  {
    logger.debug("public int getResultSetConcurrency()");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getResultSetHoldability() throws SQLException
  {
    logger.debug("public int getResultSetHoldability()");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getResultSetType() throws SQLException
  {
    logger.debug("public int getResultSetType()");

    return ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override
  public int getUpdateCount() throws SQLException
  {
    logger.debug("public int getUpdateCount()");

    return updateCount;
  }

  @Override
  public SQLWarning getWarnings() throws SQLException
  {
    logger.debug("public SQLWarning getWarnings()");

    return null;
  }

  @Override
  public boolean isClosed() throws SQLException
  {
    logger.debug("public boolean isClosed()");

    return isClosed;
  }

  @Override
  public boolean isPoolable() throws SQLException
  {
    logger.debug("public boolean isPoolable()");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setCursorName(String name) throws SQLException
  {
    logger.debug("public void setCursorName(String name)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setEscapeProcessing(boolean enable) throws SQLException
  {
    logger.debug("public void setEscapeProcessing(boolean enable)");

    escapeProcessing = true;
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException
  {
    logger.debug("public void setFetchDirection(int direction)");

    if (direction != ResultSet.FETCH_FORWARD)
    {
      throw new SQLFeatureNotSupportedException();
    }
  }

  @Override
  public void setFetchSize(int rows) throws SQLException
  {
    logger.debug("public void setFetchSize(int rows), rows={}", rows);

    fetchSize = rows;
  }

  @Override
  public void setMaxFieldSize(int max) throws SQLException
  {
    logger.debug("public void setMaxFieldSize(int max)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setMaxRows(int max) throws SQLException
  {
    logger.debug("public void setMaxRows(int max)");

    this.maxRows = max;
    try
    {
      if (this.sfStatement != null)
        this.sfStatement.addProperty("rows_per_resultset", max);
    }
    catch (SFException ex)
    {
      throw new SnowflakeSQLException(ex.getCause(),
          ex.getSqlState(), ex.getVendorCode(), ex.getParams());
    }
  }

  @Override
  public void setPoolable(boolean poolable) throws SQLException
  {
    logger.debug("public void setPoolable(boolean poolable)");

    if (poolable)
    {
      throw new SQLFeatureNotSupportedException();
    }
  }

  @Override
  public void setQueryTimeout(int seconds) throws SQLException
  {
    logger.debug("public void setQueryTimeout(int seconds)");

    this.queryTimeout = seconds;
    try
    {
      if (this.sfStatement != null)
        this.sfStatement.addProperty("query_timeout", seconds);
    }
    catch (SFException ex)
    {
      throw new SnowflakeSQLException(ex.getCause(),
          ex.getSqlState(), ex.getVendorCode(), ex.getParams());
    }
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException
  {
    logger.debug("public boolean isWrapperFor(Class<?> iface)");

    return iface.isInstance(this);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException
  {
    logger.debug("public <T> T unwrap(Class<T> iface)");

    if (!iface.isInstance(this))
    {
      throw new SQLException(
              this.getClass().getName() + " not unwrappable from " + iface
              .getName());
    }
    return (T) this;
  }

  //@Override
  public void closeOnCompletion() throws SQLException
  {
    logger.debug("public void closeOnCompletion()");

    throw new SQLFeatureNotSupportedException();
  }

  //@Override
  public boolean isCloseOnCompletion() throws SQLException
  {
    logger.debug("public boolean isCloseOnCompletion()");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void close() throws SQLException
  {
    logger.debug("public void close()");

    currentResultSet = null;
    resultSet = null;
    isClosed = true;

    sfStatement.close();
  }

  @Override
  public void cancel() throws SQLException
  {
    logger.debug("public void cancel()");

    try
    {
      sfStatement.cancel();
    }
    catch (SFException ex)
    {
      throw new SnowflakeSQLException(ex, ex.getSqlState(),
          ex.getVendorCode(), ex.getParams());
    }
  }

  @Override
  public void clearWarnings() throws SQLException
  {
    logger.debug("public void clearWarnings()");

    // warnings are not tracked yet.
  }

  @Override
  public void addBatch(String sql) throws SQLException
  {
    logger.debug("public void addBatch(String sql)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void clearBatch() throws SQLException
  {
    logger.debug("public void clearBatch()");

    throw new SQLFeatureNotSupportedException();
  }

  public void executeSetProperty(final String sql)
  {
    logger.debug("setting property");

    // tokenize the sql
    String[] tokens = sql.split("\\s+");

    if (tokens == null || tokens.length < 2)
    {
      return;
    }

    if ("tracing".equalsIgnoreCase(tokens[1]))
    {
      if (tokens.length >= 3)
      {
        /*connection.tracingLevel = Level.parse(tokens[2].toUpperCase());
        if (connection.tracingLevel != null)
        {
          Logger snowflakeLogger = Logger.getLogger("net.snowflake");
          snowflakeLogger.setLevel(connection.tracingLevel);
        }*/
      }
    }
    else
    {
      this.sfStatement.executeSetProperty(sql);
    }
  }

  public SFStatement getSfStatement()
  {
    return sfStatement;
  }

  public void setUpdateCount(int updateCount)
  {
    this.updateCount = updateCount;
  }

  private void resetState()
  {
    resultSet = null;
    currentResultSet = null;

    isClosed = false;
    updateCount = -1;
  }

}
