/*
 * Copyright (c) 2012-2018 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import net.snowflake.client.core.ParameterBindingDTO;
import net.snowflake.client.core.ResultUtil;
import net.snowflake.client.core.SFBaseResultSet;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SFStatement;

import java.sql.BatchUpdateException;
import java.util.ArrayList;
import java.util.List;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Map;

import net.snowflake.client.core.StmtUtil;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

/**
 * Snowflake statement
 *
 * @author jhuang
 */
class SnowflakeStatementV1 implements Statement
{

  static final SFLogger logger = SFLoggerFactory.getLogger(SnowflakeStatementV1.class);

  static final int NO_UPDATES = -1;

  protected SnowflakeConnectionV1 connection;

  /*
   * The maximum number of rows this statement ( should return (0 => all rows).
   */
  private int maxRows = 0;

  // result set currently in use
  private ResultSet resultSet = null;

  private int fetchSize = 50;

  private Boolean isClosed = false;

  private int updateCount = NO_UPDATES;

  // TODO: escape processing for sql statement
  private boolean escapeProcessing = false;

  // timeout in seconds
  private int queryTimeout = 0;

  // max field size limited to 16MB
  private final int maxFieldSize = 16777216;

  protected SFStatement sfStatement;

  /** batch of sql strings added by addBatch */
  protected final List<BatchEntry> batch = new ArrayList<>();

  protected SQLWarning sqlWarnings;

  SnowflakeStatementV1(SnowflakeConnectionV1 conn)
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

  int executeUpdateInternal(String sql,
                            Map<String, ParameterBindingDTO> parameterBindings)
          throws SQLException
  {
    if (StmtUtil.checkStageManageCommand(sql) != null)
    {
      throw new SnowflakeSQLException(ErrorCode.
          UNSUPPORTED_STATEMENT_TYPE_IN_EXECUTION_API,
          sql.length() > 20 ? sql.substring(0, 20) + "..." : sql);
    }

    SFBaseResultSet sfResultSet = null;
    try
    {
      sfResultSet = sfStatement.execute(sql, parameterBindings,
          SFStatement.CallingMethod.EXECUTE_UPDATE);
      sfResultSet.setSession(this.connection.getSfSession());
      updateCount = ResultUtil.calculateUpdateCount(sfResultSet);
      resultSet = null;
    }
    catch (SFException ex)
    {
      throw new SnowflakeSQLException(ex.getCause(),
          ex.getSqlState(), ex.getVendorCode(), ex.getParams());
    }

    if (getUpdateCount() == NO_UPDATES)
    {
      throw new SnowflakeSQLException(ErrorCode.
          UNSUPPORTED_STATEMENT_TYPE_IN_EXECUTION_API,
          sql.length() > 20 ? sql.substring(0, 20) + "..." : sql);

    }

    return getUpdateCount();
  }

  /**
   * Internal method for executing a query with bindings accepted.
   *
   * @param sql sql statement
   * @param parameterBindings parameters bindings
   * @return query result set
   * @throws SQLException if @link{SFStatement.execute(String)} throws exception
   */
   ResultSet executeQueryInternal(
      String sql,
      Map<String, ParameterBindingDTO> parameterBindings)
      throws SQLException
  {
    SFBaseResultSet sfResultSet = null;
    try
    {
      sfResultSet = sfStatement.execute(sql, parameterBindings,
          SFStatement.CallingMethod.EXECUTE_QUERY);
      sfResultSet.setSession(this.connection.getSfSession());
    }
    catch (SFException ex)
    {
      throw new SnowflakeSQLException(ex.getCause(),
          ex.getSqlState(), ex.getVendorCode(), ex.getParams());
    }

    resultSet = new SnowflakeResultSetV1(sfResultSet, this);

    return getResultSet();
  }

  /**
   * Execute sql
   *
   * @param sql sql statement
   * @param parameterBindings a map of binds to use for this query
   * @return whether there is result set or not
   * @throws java.sql.SQLException if @link{#executeQuery(String)} throws exception
   */
  boolean executeInternal(String sql,
                          Map<String, ParameterBindingDTO> parameterBindings)
                          throws SQLException
  {
    connection.injectedDelay();

    logger.debug("execute: {}", sql);

    String trimmedSql = sql.trim();

    if (trimmedSql.length() >= 20
        && trimmedSql.toLowerCase().startsWith("set-sf-property"))
    {
      // deprecated: sfsql
      executeSetProperty(sql);
      return false;
    }

    SFBaseResultSet sfResultSet = null;
    try
    {
      sfResultSet = sfStatement.execute(sql, parameterBindings,
          SFStatement.CallingMethod.EXECUTE);
      sfResultSet.setSession(this.connection.getSfSession());
      resultSet = new SnowflakeResultSetV1(sfResultSet, this);

      // Legacy behavior treats update counts as result sets for single-
      // statement execute, so we only treat update counts as update counts
      // if JDBC_EXECUTE_RETURN_COUNT_FOR_DML is set, or if a statement
      // is multi-statement
      if (!sfResultSet.getStatementType().isGenerateResultSet() &&
          (connection.getSfSession().isExecuteReturnCountForDML() ||
           sfStatement.hasChildren()))
      {
        updateCount = ResultUtil.calculateUpdateCount(sfResultSet);
        resultSet = null;
        return false;
      }

      updateCount = NO_UPDATES;
      return true;
    }
    catch (SFException ex)
    {
      throw new SnowflakeSQLException(ex.getCause(),
          ex.getSqlState(), ex.getVendorCode(), ex.getParams());
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
    return executeInternal(sql, null);
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

  /**
   * Batch Execute. If one of the commands in the batch failed, JDBC will
   * continuing processing and throw BatchUpdateException after all commands
   * are processed.
   * @return
   * @throws SQLException
   */
  @Override
  public int[] executeBatch() throws SQLException
  {
    logger.debug("public int[] executeBatch()");

    return executeBatchInternal();
  }

  /**
   * This method will iterate through batch and provide sql and bindings to
   * underlying SFStatement to get result set.
   *
   * Note, array binds use a different code path since only one network
   * roundtrip in the array bind execution case.
   *
   * @return
   * @throws SQLException
   */
  int[] executeBatchInternal() throws SQLException
  {
    if (isClosed)
    {
      throw new SnowflakeSQLException(ErrorCode.STATEMENT_CLOSED);
    }

    SQLException exceptionReturned = null;
    int updateCounts[] = new int[batch.size()];

    for (int i=0; i<batch.size(); i++)
    {
      try
      {
        updateCounts[i] = this.executeUpdateInternal(
            batch.get(i).getSql(), batch.get(i).getParameterBindings());
      }
      catch(SQLException e)
      {
        exceptionReturned = exceptionReturned == null ? e : exceptionReturned;
        updateCounts[i] = EXECUTE_FAILED;
      }
    }

    if (exceptionReturned != null)
    {
      throw new BatchUpdateException(exceptionReturned.getLocalizedMessage(),
          exceptionReturned.getSQLState(),
          exceptionReturned.getErrorCode(),
          updateCounts,
          exceptionReturned);
    }

    return updateCounts;
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

    return getMoreResults(Statement.CLOSE_CURRENT_RESULT);
  }

  @Override
  public boolean getMoreResults(int current) throws SQLException
  {
    logger.debug("public boolean getMoreResults(int current)");

    // clean up the current result set, if it exists
    if (resultSet != null &&
        (current == Statement.CLOSE_CURRENT_RESULT ||
        current == Statement.CLOSE_ALL_RESULTS))
    {
      resultSet.close();
    }


    boolean hasResultSet = sfStatement.getMoreResults(current);
    SFBaseResultSet sfResultSet = sfStatement.getResultSet();

    if (hasResultSet) // result set returned
    {
      sfResultSet.setSession(this.connection.getSfSession());
      resultSet = new SnowflakeResultSetV1(sfResultSet, this);
      updateCount = NO_UPDATES;
      return true;
    }
    else if (sfResultSet != null) // update count returned
    {
      resultSet = null;
      try
      {
        updateCount = ResultUtil.calculateUpdateCount(sfResultSet);
      }
      catch (SFException ex)
      {
        throw new SnowflakeSQLException(ex);
      }
      return false;
    }
    else // no more results
    {
      updateCount = NO_UPDATES;
      return false;
    }
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

    return resultSet;
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

    return sqlWarnings;
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

  /**
   * Sets a parameter at the statement level. Used for internal testing.
   * @throws Exception
   */
  public void setParameter(String name, Object value) throws Exception
  {
    logger.debug("public void setParameter");

    try
    {
      if (this.sfStatement != null)
      {
        this.sfStatement.addProperty(name, value);
      }
    }
    catch (SFException ex)
    {
      throw new SnowflakeSQLException(ex);
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

  @Override
  public void closeOnCompletion() throws SQLException
  {
    logger.debug("public void closeOnCompletion()");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isCloseOnCompletion() throws SQLException
  {
    logger.debug("public boolean isCloseOnCompletion()");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void close() throws SQLException
  {
    logger.debug("public void close()");

    resultSet = null;
    isClosed = true;
    batch.clear();

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

    sqlWarnings = null;
  }

  @Override
  public void addBatch(String sql) throws SQLException
  {
    logger.debug("public void addBatch(String sql)");

    if (isClosed)
    {
      throw new SnowflakeSQLException(ErrorCode.STATEMENT_CLOSED);
    }

    batch.add(new BatchEntry(sql, null));
  }

  @Override
  public void clearBatch() throws SQLException
  {
    logger.debug("public void clearBatch()");

    if (isClosed)
    {
      throw new SnowflakeSQLException(ErrorCode.STATEMENT_CLOSED);
    }

    batch.clear();
  }

  private void executeSetProperty(final String sql)
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

  protected void appendWarning(SQLWarning w)
  {
    if (sqlWarnings == null)
    {
      sqlWarnings = w;
    }
    else
    {
      sqlWarnings.setNextWarning(w);
    }
  }

  final class BatchEntry
  {
    private final String sql;

    private final Map<String, ParameterBindingDTO> parameterBindings;

    BatchEntry(String sql,
               Map<String, ParameterBindingDTO> parameterBindings)
    {
      this.sql = sql;
      this.parameterBindings = parameterBindings;
    }

    public String getSql()
    {
      return sql;
    }

    public Map<String, ParameterBindingDTO> getParameterBindings()
    {
      return parameterBindings;
    }
  }
}
