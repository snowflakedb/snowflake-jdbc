/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import static net.snowflake.client.jdbc.ErrorCode.FEATURE_UNSUPPORTED;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import net.snowflake.client.core.*;
import net.snowflake.client.log.ArgSupplier;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.client.util.SecretDetector;
import net.snowflake.client.util.VariableTypeArray;
import net.snowflake.common.core.SqlState;

/** Snowflake statement */
class SnowflakeStatementV1 implements Statement, SnowflakeStatement {
  static final SFLogger logger = SFLoggerFactory.getLogger(SnowflakeStatementV1.class);

  private static final long NO_UPDATES = -1;

  protected final SnowflakeConnectionV1 connection;

  protected final int resultSetType;
  protected final int resultSetConcurrency;
  protected final int resultSetHoldability;

  /*
   * The maximum number of rows this statement ( should return (0 => all rows).
   */
  private int maxRows = 0;

  // Refer to all open resultSets from this statement
  private final Set<ResultSet> openResultSets = ConcurrentHashMap.newKeySet();

  // result set currently in use
  private ResultSet resultSet = null;

  private int fetchSize = 50;

  private Boolean isClosed = false;

  private long updateCount = NO_UPDATES;

  // timeout in seconds
  private int queryTimeout = 0;

  // max field size limited to 16MB
  private final int maxFieldSize = 16777216;

  SFBaseStatement sfBaseStatement;

  private boolean poolable;

  /** Snowflake query ID from the latest executed query */
  private String queryID;

  /** Snowflake query IDs from the latest executed batch */
  private List<String> batchQueryIDs = new LinkedList<>();

  /** batch of sql strings added by addBatch */
  protected final List<BatchEntry> batch = new ArrayList<>();

  private SQLWarning sqlWarnings;

  /**
   * Construct SnowflakeStatementV1
   *
   * @param connection connection object
   * @param resultSetType result set type: ResultSet.TYPE_FORWARD_ONLY.
   * @param resultSetConcurrency result set concurrency: ResultSet.CONCUR_READ_ONLY.
   * @param resultSetHoldability result set holdability: ResultSet.CLOSE_CURSORS_AT_COMMIT
   * @throws SQLException if any SQL error occurs.
   */
  SnowflakeStatementV1(
      SnowflakeConnectionV1 connection,
      int resultSetType,
      int resultSetConcurrency,
      int resultSetHoldability)
      throws SQLException {
    logger.debug(" public SnowflakeStatement(SnowflakeConnectionV1 conn)");

    this.connection = connection;

    if (resultSetType != ResultSet.TYPE_FORWARD_ONLY) {
      throw new SQLFeatureNotSupportedException(
          String.format("ResultSet type %d is not supported.", resultSetType),
          FEATURE_UNSUPPORTED.getSqlState(),
          FEATURE_UNSUPPORTED.getMessageCode());
    }

    if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
      throw new SQLFeatureNotSupportedException(
          String.format("ResultSet concurrency %d is not supported.", resultSetConcurrency),
          FEATURE_UNSUPPORTED.getSqlState(),
          FEATURE_UNSUPPORTED.getMessageCode());
    }

    if (resultSetHoldability != ResultSet.CLOSE_CURSORS_AT_COMMIT) {
      throw new SQLFeatureNotSupportedException(
          String.format("ResultSet holdability %d is not supported.", resultSetHoldability),
          FEATURE_UNSUPPORTED.getSqlState(),
          FEATURE_UNSUPPORTED.getMessageCode());
    }

    this.resultSetType = resultSetType;
    this.resultSetConcurrency = resultSetConcurrency;
    this.resultSetHoldability = resultSetHoldability;

    sfBaseStatement = (connection != null) ? connection.getHandler().getSFStatement() : null;
  }

  protected void raiseSQLExceptionIfStatementIsClosed() throws SQLException {
    if (isClosed) {
      throw new SnowflakeSQLException(ErrorCode.STATEMENT_CLOSED);
    }
  }

  /**
   * Execute SQL query
   *
   * @param sql sql statement
   * @return ResultSet
   * @throws SQLException if @link{#executeQueryInternal(String, Map)} throws an exception
   */
  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    raiseSQLExceptionIfStatementIsClosed();
    return executeQueryInternal(sql, false, null);
  }

  /**
   * Execute SQL query asynchronously
   *
   * @param sql sql statement
   * @return ResultSet
   * @throws SQLException if @link{#executeQueryInternal(String, Map)} throws an exception
   */
  public ResultSet executeAsyncQuery(String sql) throws SQLException {
    raiseSQLExceptionIfStatementIsClosed();
    return executeQueryInternal(sql, true, null);
  }

  /**
   * Execute an update statement
   *
   * @param sql sql statement
   * @return number of rows updated
   * @throws SQLException if @link{#executeUpdateInternal(String, Map)} throws exception
   */
  @Override
  public int executeUpdate(String sql) throws SQLException {
    return (int) this.executeLargeUpdate(sql);
  }

  /**
   * Execute an update statement returning the number of affected rows in long
   *
   * @param sql sql statement
   * @return number of rows updated in long
   * @throws SQLException if @link{#executeUpdateInternal(String, Map)} throws exception
   */
  @Override
  public long executeLargeUpdate(String sql) throws SQLException {
    return executeUpdateInternal(sql, null, true);
  }

  long executeUpdateInternal(
      String sql, Map<String, ParameterBindingDTO> parameterBindings, boolean updateQueryRequired)
      throws SQLException {
    raiseSQLExceptionIfStatementIsClosed();

    /* If sql command is a staging command that has parameter binding, throw an exception because parameter binding
    is not supported for staging commands. */
    if (StmtUtil.checkStageManageCommand(sql) != null && parameterBindings != null) {
      throw new SnowflakeSQLLoggedException(
          connection.getSFBaseSession(),
          ErrorCode.UNSUPPORTED_STATEMENT_TYPE_IN_EXECUTION_API,
          StmtUtil.truncateSQL(sql));
    }

    SFBaseResultSet sfResultSet;
    try {
      sfResultSet =
          sfBaseStatement.execute(
              sql, parameterBindings, SFBaseStatement.CallingMethod.EXECUTE_UPDATE);
      sfResultSet.setSession(this.connection.getSFBaseSession());
      updateCount = ResultUtil.calculateUpdateCount(sfResultSet);
      queryID = sfResultSet.getQueryId();
    } catch (SFException ex) {
      throw new SnowflakeSQLException(
          ex.getCause(), ex.getSqlState(), ex.getVendorCode(), ex.getParams());
    } finally {
      if (resultSet != null) {
        openResultSets.add(resultSet);
      }
      resultSet = null;
    }

    if (updateCount == NO_UPDATES && updateQueryRequired) {
      throw new SnowflakeSQLLoggedException(
          connection.getSFBaseSession(),
          ErrorCode.UNSUPPORTED_STATEMENT_TYPE_IN_EXECUTION_API,
          StmtUtil.truncateSQL(sql));
    }

    return updateCount;
  }

  /**
   * Internal method for executing a query with bindings accepted.
   *
   * @param sql sql statement
   * @param asyncExec execute query asynchronously
   * @param parameterBindings parameters bindings
   * @return query result set
   * @throws SQLException if @link{SFStatement.execute(String)} throws exception
   */
  ResultSet executeQueryInternal(
      String sql, boolean asyncExec, Map<String, ParameterBindingDTO> parameterBindings)
      throws SQLException {
    SFBaseResultSet sfResultSet;
    try {
      if (asyncExec) {
        if (!connection.getHandler().supportsAsyncQuery()) {
          throw new SQLFeatureNotSupportedException(
              "Async execution not supported in current context.");
        }
        sfResultSet =
            sfBaseStatement.asyncExecute(
                sql, parameterBindings, SFBaseStatement.CallingMethod.EXECUTE_QUERY);
      } else {
        sfResultSet =
            sfBaseStatement.execute(
                sql, parameterBindings, SFBaseStatement.CallingMethod.EXECUTE_QUERY);
      }

      sfResultSet.setSession(this.connection.getSFBaseSession());
    } catch (SFException ex) {
      throw new SnowflakeSQLException(
          ex.getCause(), ex.getSqlState(), ex.getVendorCode(), ex.getParams());
    }

    if (resultSet != null) {
      openResultSets.add(resultSet);
    }

    if (asyncExec) {
      resultSet = connection.getHandler().createAsyncResultSet(sfResultSet, this);
    } else {
      resultSet = connection.getHandler().createResultSet(sfResultSet, this);
    }

    return getResultSet();
  }

  /**
   * Execute sql
   *
   * @param sql sql statement
   * @param parameterBindings a map of binds to use for this query
   * @return whether there is result set or not
   * @throws SQLException if @link{#executeQuery(String)} throws exception
   */
  boolean executeInternal(String sql, Map<String, ParameterBindingDTO> parameterBindings)
      throws SQLException {
    raiseSQLExceptionIfStatementIsClosed();
    connection.injectedDelay();

    logger.debug("execute: {}", (ArgSupplier) () -> SecretDetector.maskSecrets(sql));

    String trimmedSql = sql.trim();

    if (trimmedSql.length() >= 20 && trimmedSql.toLowerCase().startsWith("set-sf-property")) {
      // deprecated: sfsql
      executeSetProperty(sql);
      return false;
    }

    SFBaseResultSet sfResultSet;
    try {
      sfResultSet =
          sfBaseStatement.execute(sql, parameterBindings, SFBaseStatement.CallingMethod.EXECUTE);
      sfResultSet.setSession(this.connection.getSFBaseSession());
      if (resultSet != null) {
        openResultSets.add(resultSet);
      }
      resultSet = connection.getHandler().createResultSet(sfResultSet, this);
      queryID = sfResultSet.getQueryId();

      // Legacy behavior treats update counts as result sets for single-
      // statement execute, so we only treat update counts as update counts
      // if CLIENT_SFSQL is not set, or if a statement
      // is multi-statement
      if (!sfResultSet.getStatementType().isGenerateResultSet()
          && (!connection.getSFBaseSession().isSfSQLMode() || sfBaseStatement.hasChildren())) {
        updateCount = ResultUtil.calculateUpdateCount(sfResultSet);
        if (resultSet != null) {
          openResultSets.add(resultSet);
        }
        resultSet = null;
        return false;
      }

      updateCount = NO_UPDATES;
      return true;
    } catch (SFException ex) {
      throw new SnowflakeSQLException(
          ex.getCause(), ex.getSqlState(), ex.getVendorCode(), ex.getParams());
    }
  }

  /** @return the query ID of the latest executed query */
  public String getQueryID() {
    // return the queryID for the query executed last time
    return queryID;
  }

  /** @return the query IDs of the latest executed batch queries */
  public List<String> getBatchQueryIDs() {
    return Collections.unmodifiableList(batchQueryIDs);
  }

  /**
   * Execute sql
   *
   * @param sql sql statement
   * @return whether there is result set or not
   * @throws SQLException if @link{#executeQuery(String)} throws exception
   */
  @Override
  public boolean execute(String sql) throws SQLException {
    return executeInternal(sql, null);
  }

  @Override
  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    logger.debug("execute(String sql, int autoGeneratedKeys)");

    if (autoGeneratedKeys == Statement.NO_GENERATED_KEYS) {
      return execute(sql);
    } else {
      throw new SnowflakeLoggedFeatureNotSupportedException(connection.getSFBaseSession());
    }
  }

  @Override
  public boolean execute(String sql, int[] columnIndexes) throws SQLException {
    logger.debug("execute(String sql, int[] columnIndexes)");

    throw new SnowflakeLoggedFeatureNotSupportedException(connection.getSFBaseSession());
  }

  @Override
  public boolean execute(String sql, String[] columnNames) throws SQLException {
    logger.debug("execute(String sql, String[] columnNames)");

    throw new SnowflakeLoggedFeatureNotSupportedException(connection.getSFBaseSession());
  }

  /**
   * Batch Execute. If one of the commands in the batch failed, JDBC will continuing processing and
   * throw BatchUpdateException after all commands are processed.
   *
   * @return an array of update counts
   * @throws SQLException if any error occurs.
   */
  @Override
  public int[] executeBatch() throws SQLException {
    logger.debug("int[] executeBatch()");
    return executeBatchInternal(false).intArr;
  }

  /**
   * Batch Execute. If one of the commands in the batch failed, JDBC will continuing processing and
   * throw BatchUpdateException after all commands are processed.
   *
   * @return an array of update counts
   * @throws SQLException if any error occurs.
   */
  @Override
  public long[] executeLargeBatch() throws SQLException {
    logger.debug("executeBatch()");
    return executeBatchInternal(true).longArr;
  }

  /**
   * This method will iterate through batch and provide sql and bindings to underlying SFStatement
   * to get result set.
   *
   * <p>Note, array binds use a different code path since only one network roundtrip in the array
   * bind execution case.
   *
   * @return the number of updated rows
   * @throws SQLException raises if statement is closed or any db error occurs
   */
  VariableTypeArray executeBatchInternal(boolean isLong) throws SQLException {
    raiseSQLExceptionIfStatementIsClosed();

    SQLException exceptionReturned = null;
    VariableTypeArray updateCounts;
    if (isLong) {
      long[] arr = new long[batch.size()];
      updateCounts = new VariableTypeArray(null, arr);
    } else {
      int size = batch.size();
      int[] arr = new int[size];
      updateCounts = new VariableTypeArray(arr, null);
    }
    batchQueryIDs.clear();
    for (int i = 0; i < batch.size(); i++) {
      BatchEntry b = batch.get(i);
      try {
        long cnt = this.executeUpdateInternal(b.getSql(), b.getParameterBindings(), false);
        if (cnt == NO_UPDATES) {
          // in executeBatch we set updateCount to SUCCESS_NO_INFO
          // for successful query with no updates
          cnt = SUCCESS_NO_INFO;
        }
        if (isLong) {
          updateCounts.longArr[i] = cnt;
        } else if (cnt <= Integer.MAX_VALUE) {
          updateCounts.intArr[i] = (int) cnt;
        } else {
          throw new SnowflakeSQLLoggedException(
              connection.getSFBaseSession(),
              ErrorCode.EXECUTE_BATCH_INTEGER_OVERFLOW.getMessageCode(),
              SqlState.NUMERIC_VALUE_OUT_OF_RANGE,
              i);
        }
        batchQueryIDs.add(queryID);
      } catch (SQLException e) {
        exceptionReturned = exceptionReturned == null ? e : exceptionReturned;
        if (isLong) {
          updateCounts.longArr[i] = (long) EXECUTE_FAILED;
        } else {
          updateCounts.intArr[i] = EXECUTE_FAILED;
        }
      }
    }

    if (exceptionReturned != null && isLong) {
      throw new BatchUpdateException(
          exceptionReturned.getLocalizedMessage(),
          exceptionReturned.getSQLState(),
          exceptionReturned.getErrorCode(),
          updateCounts.longArr,
          exceptionReturned);
    } else if (exceptionReturned != null) {
      throw new BatchUpdateException(
          exceptionReturned.getLocalizedMessage(),
          exceptionReturned.getSQLState(),
          exceptionReturned.getErrorCode(),
          updateCounts.intArr,
          exceptionReturned);
    }

    return updateCounts;
  }

  @Override
  public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    logger.debug("executeUpdate(String sql, int autoGeneratedKeys)");

    return (int) this.executeLargeUpdate(sql, autoGeneratedKeys);
  }

  @Override
  public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    logger.debug("executeUpdate(String sql, int autoGeneratedKeys)");

    if (autoGeneratedKeys == Statement.NO_GENERATED_KEYS) {
      return executeLargeUpdate(sql);
    } else {
      throw new SnowflakeLoggedFeatureNotSupportedException(connection.getSFBaseSession());
    }
  }

  @Override
  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    logger.debug("executeUpdate(String sql, int[] columnIndexes)");

    throw new SnowflakeLoggedFeatureNotSupportedException(connection.getSFBaseSession());
  }

  @Override
  public long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException {
    logger.debug("executeLargeUpdate(String sql, int[] columnIndexes)");

    throw new SnowflakeLoggedFeatureNotSupportedException(connection.getSFBaseSession());
  }

  @Override
  public int executeUpdate(String sql, String[] columnNames) throws SQLException {
    logger.debug("executeUpdate(String sql, String[] columnNames)");

    throw new SnowflakeLoggedFeatureNotSupportedException(connection.getSFBaseSession());
  }

  @Override
  public long executeLargeUpdate(String sql, String[] columnNames) throws SQLException {
    logger.debug("executeUpdate(String sql, String[] columnNames)");

    throw new SnowflakeLoggedFeatureNotSupportedException(connection.getSFBaseSession());
  }

  @Override
  public Connection getConnection() throws SQLException {
    logger.debug("getConnection()");
    raiseSQLExceptionIfStatementIsClosed();
    return connection;
  }

  @Override
  public int getFetchDirection() throws SQLException {
    logger.debug("getFetchDirection()");
    raiseSQLExceptionIfStatementIsClosed();
    return ResultSet.FETCH_FORWARD;
  }

  @Override
  public int getFetchSize() throws SQLException {
    logger.debug("getFetchSize()");
    raiseSQLExceptionIfStatementIsClosed();
    return fetchSize;
  }

  @Override
  public ResultSet getGeneratedKeys() throws SQLException {
    logger.debug("getGeneratedKeys()");
    raiseSQLExceptionIfStatementIsClosed();
    return new SnowflakeResultSetV1.EmptyResultSet();
  }

  @Override
  public int getMaxFieldSize() throws SQLException {
    logger.debug("getMaxFieldSize()");
    raiseSQLExceptionIfStatementIsClosed();
    return maxFieldSize;
  }

  @Override
  public int getMaxRows() throws SQLException {
    logger.debug("getMaxRows()");
    raiseSQLExceptionIfStatementIsClosed();
    return maxRows;
  }

  @Override
  public boolean getMoreResults() throws SQLException {
    logger.debug("getMoreResults()");

    return getMoreResults(Statement.CLOSE_CURRENT_RESULT);
  }

  @Override
  public boolean getMoreResults(int current) throws SQLException {
    logger.debug("getMoreResults(int current)");
    raiseSQLExceptionIfStatementIsClosed();

    // clean up the current result set, if it exists
    if (resultSet != null
        && (current == Statement.CLOSE_CURRENT_RESULT || current == Statement.CLOSE_ALL_RESULTS)) {
      resultSet.close();
    }

    boolean hasResultSet = sfBaseStatement.getMoreResults(current);
    SFBaseResultSet sfResultSet = sfBaseStatement.getResultSet();

    if (hasResultSet) // result set returned
    {
      sfResultSet.setSession(this.connection.getSFBaseSession());
      if (resultSet != null) {
        openResultSets.add(resultSet);
      }
      resultSet = connection.getHandler().createResultSet(sfResultSet, this);
      updateCount = NO_UPDATES;
      return true;
    } else if (sfResultSet != null) // update count returned
    {
      if (resultSet != null) {
        openResultSets.add(resultSet);
      }
      resultSet = null;
      try {
        updateCount = ResultUtil.calculateUpdateCount(sfResultSet);
      } catch (SFException ex) {
        throw new SnowflakeSQLLoggedException(connection.getSFBaseSession(), ex);
      }
      return false;
    } else // no more results
    {
      updateCount = NO_UPDATES;
      return false;
    }
  }

  @Override
  public int getQueryTimeout() throws SQLException {
    logger.debug("getQueryTimeout()");
    raiseSQLExceptionIfStatementIsClosed();
    return this.queryTimeout;
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    logger.debug("getResultSet()");
    raiseSQLExceptionIfStatementIsClosed();
    return resultSet;
  }

  @Override
  public int getResultSetConcurrency() throws SQLException {
    logger.debug("getResultSetConcurrency()");
    raiseSQLExceptionIfStatementIsClosed();
    return resultSetConcurrency;
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    logger.debug("getResultSetHoldability()");
    raiseSQLExceptionIfStatementIsClosed();
    return resultSetHoldability;
  }

  @Override
  public int getResultSetType() throws SQLException {
    logger.debug("getResultSetType()");
    raiseSQLExceptionIfStatementIsClosed();
    return this.resultSetType;
  }

  @Override
  public int getUpdateCount() throws SQLException {
    logger.debug("getUpdateCount()");
    return (int) getUpdateCountIfDML();
  }

  @Override
  public long getLargeUpdateCount() throws SQLException {
    logger.debug("getLargeUpdateCount()");
    return getUpdateCountIfDML();
  }

  private long getUpdateCountIfDML() throws SQLException {
    raiseSQLExceptionIfStatementIsClosed();
    if (updateCount != -1 && sfBaseStatement.getResultSet().getStatementType().isDML()) {
      return updateCount;
    }
    return -1;
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    logger.debug("getWarnings()");
    raiseSQLExceptionIfStatementIsClosed();
    return sqlWarnings;
  }

  @Override
  public boolean isClosed() throws SQLException {
    logger.debug("isClosed()");
    return isClosed; // no exception
  }

  @Override
  public boolean isPoolable() throws SQLException {
    logger.debug("isPoolable()");
    raiseSQLExceptionIfStatementIsClosed();
    return poolable;
  }

  @Override
  public void setCursorName(String name) throws SQLException {
    logger.debug("setCursorName(String name)");

    throw new SnowflakeLoggedFeatureNotSupportedException(connection.getSFBaseSession());
  }

  @Override
  public void setEscapeProcessing(boolean enable) throws SQLException {
    logger.debug("setEscapeProcessing(boolean enable)");
    // NOTE: We could raise an exception here, because not implemented
    // but it may break the existing applications. For now returning nothnig.
    // we should revisit.
    raiseSQLExceptionIfStatementIsClosed();
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    logger.debug("setFetchDirection(int direction)");
    raiseSQLExceptionIfStatementIsClosed();
    if (direction != ResultSet.FETCH_FORWARD) {
      throw new SnowflakeLoggedFeatureNotSupportedException(connection.getSFBaseSession());
    }
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    logger.debug("setFetchSize(int rows), rows={}", rows);
    raiseSQLExceptionIfStatementIsClosed();
    fetchSize = rows;
  }

  @Override
  public void setMaxFieldSize(int max) throws SQLException {
    logger.debug("setMaxFieldSize(int max)");

    throw new SnowflakeLoggedFeatureNotSupportedException(connection.getSFBaseSession());
  }

  @Override
  public void setMaxRows(int max) throws SQLException {
    logger.debug("setMaxRows(int max)");

    raiseSQLExceptionIfStatementIsClosed();

    this.maxRows = max;
    try {
      if (this.sfBaseStatement != null) {
        this.sfBaseStatement.addProperty("rows_per_resultset", max);
      }
    } catch (SFException ex) {
      throw new SnowflakeSQLException(
          ex.getCause(), ex.getSqlState(), ex.getVendorCode(), ex.getParams());
    }
  }

  @Override
  public void setPoolable(boolean poolable) throws SQLException {
    logger.debug("setPoolable(boolean poolable)");
    raiseSQLExceptionIfStatementIsClosed();

    if (poolable) {
      throw new SnowflakeLoggedFeatureNotSupportedException(connection.getSFBaseSession());
    }
    this.poolable = poolable;
  }

  /**
   * Sets a parameter at the statement level.
   *
   * @param name parameter name.
   * @param value parameter value.
   * @throws SQLException if any SQL error occurs.
   */
  public void setParameter(String name, Object value) throws SQLException {
    logger.debug("setParameter");

    try {
      if (this.sfBaseStatement != null) {
        this.sfBaseStatement.addProperty(name, value);
      }
    } catch (SFException ex) {
      throw new SnowflakeSQLException(ex);
    }
  }

  @Override
  public void setQueryTimeout(int seconds) throws SQLException {
    logger.debug("setQueryTimeout(int seconds)");
    raiseSQLExceptionIfStatementIsClosed();

    this.queryTimeout = seconds;
    try {
      if (this.sfBaseStatement != null) {
        this.sfBaseStatement.addProperty("query_timeout", seconds);
      }
    } catch (SFException ex) {
      throw new SnowflakeSQLException(
          ex.getCause(), ex.getSqlState(), ex.getVendorCode(), ex.getParams());
    }
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    logger.debug("isWrapperFor(Class<?> iface)");

    return iface.isInstance(this);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    logger.debug("unwrap(Class<T> iface)");

    if (!iface.isInstance(this)) {
      throw new SQLException(
          this.getClass().getName() + " not unwrappable from " + iface.getName());
    }
    return (T) this;
  }

  @Override
  public void closeOnCompletion() throws SQLException {
    logger.debug("closeOnCompletion()");
    throw new SnowflakeLoggedFeatureNotSupportedException(connection.getSFBaseSession());
  }

  @Override
  public boolean isCloseOnCompletion() throws SQLException {
    logger.debug("isCloseOnCompletion()");
    throw new SnowflakeLoggedFeatureNotSupportedException(connection.getSFBaseSession());
  }

  @Override
  public void close() throws SQLException {
    close(true);
  }

  public void close(boolean removeClosedStatementFromConnection) throws SQLException {
    logger.debug("close()");

    // No exception is raised even if the statement is closed.
    if (resultSet != null) {
      resultSet.close();
      resultSet = null;
    }
    isClosed = true;
    batch.clear();

    // also make sure to close all created resultSets from this statement
    for (ResultSet rs : openResultSets) {
      if (rs != null && !rs.isClosed()) {
        if (rs.isWrapperFor(SnowflakeResultSetV1.class)) {
          rs.unwrap(SnowflakeResultSetV1.class).close(false);
        } else {
          rs.close();
        }
      }
    }
    openResultSets.clear();
    sfBaseStatement.close();
    if (removeClosedStatementFromConnection) {
      connection.removeClosedStatement(this);
    }
  }

  @Override
  public void cancel() throws SQLException {
    logger.debug("cancel()");
    raiseSQLExceptionIfStatementIsClosed();

    try {
      sfBaseStatement.cancel();
    } catch (SFException ex) {
      throw new SnowflakeSQLException(ex, ex.getSqlState(), ex.getVendorCode(), ex.getParams());
    }
  }

  @Override
  public void clearWarnings() throws SQLException {
    logger.debug("clearWarnings()");
    raiseSQLExceptionIfStatementIsClosed();
    sqlWarnings = null;
  }

  @Override
  public void addBatch(String sql) throws SQLException {
    logger.debug("addBatch(String sql)");

    raiseSQLExceptionIfStatementIsClosed();

    batch.add(new BatchEntry(sql, null));
  }

  @Override
  public void clearBatch() throws SQLException {
    logger.debug("clearBatch()");

    raiseSQLExceptionIfStatementIsClosed();

    batch.clear();
  }

  private void executeSetProperty(final String sql) {
    logger.debug("setting property");

    // tokenize the sql
    String[] tokens = sql.split("\\s+");

    if (tokens.length < 2) {
      return;
    }

    if ("tracing".equalsIgnoreCase(tokens[1])) {
      if (tokens.length >= 3) {
        /*connection.tracingLevel = Level.parse(tokens[2].toUpperCase());
        if (connection.tracingLevel != null)
        {
          Logger snowflakeLogger = Logger.getLogger("net.snowflake");
          snowflakeLogger.setLevel(connection.tracingLevel);
        }*/
      }
    } else {
      this.sfBaseStatement.executeSetProperty(sql);
    }
  }

  public SFBaseStatement getSFBaseStatement() throws SQLException {
    return sfBaseStatement;
  }

  // Convenience method to return an SFStatement-typed SFStatementInterface object, but
  // performs the type-checking as necessary.
  public SFStatement getSfStatement() throws SnowflakeSQLException {
    if (sfBaseStatement instanceof SFStatement) {
      return (SFStatement) sfBaseStatement;
    }

    throw new SnowflakeSQLException(
        "getSfStatement() called with a different SFStatementInterface type.");
  }

  public void removeClosedResultSet(ResultSet rs) {
    openResultSets.remove(rs);
  }

  final class BatchEntry {
    private final String sql;

    private final Map<String, ParameterBindingDTO> parameterBindings;

    BatchEntry(String sql, Map<String, ParameterBindingDTO> parameterBindings) {
      this.sql = sql;
      this.parameterBindings = parameterBindings;
    }

    public String getSql() {
      return sql;
    }

    public Map<String, ParameterBindingDTO> getParameterBindings() {
      return parameterBindings;
    }
  }

  /**
   * This is a No Operation Statement to avoid null pointer exception for sessionless result set.
   */
  public static class NoOpSnowflakeStatementV1 extends SnowflakeStatementV1 {
    public NoOpSnowflakeStatementV1() throws SQLException {
      super(
          null,
          ResultSet.TYPE_FORWARD_ONLY,
          ResultSet.CONCUR_READ_ONLY,
          ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    private void throwExceptionAnyway() throws SQLException {
      throw new SQLException(
          "This is a dummy SnowflakeStatement, " + "no member function should be called for it.");
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
      throwExceptionAnyway();
      return null;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
      throwExceptionAnyway();
      return 0;
    }

    @Override
    public long executeLargeUpdate(String sql) throws SQLException {
      throwExceptionAnyway();
      return 0;
    }

    @Override
    public String getQueryID() {
      return "invalid_query_id";
    }

    @Override
    public List<String> getBatchQueryIDs() {
      return new ArrayList<>();
    }

    @Override
    public boolean execute(String sql) throws SQLException {
      throwExceptionAnyway();
      return false;
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
      throwExceptionAnyway();
      return false;
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
      throwExceptionAnyway();
      return false;
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
      throwExceptionAnyway();
      return false;
    }

    @Override
    public int[] executeBatch() throws SQLException {
      throwExceptionAnyway();
      return new int[1];
    }

    @Override
    public long[] executeLargeBatch() throws SQLException {
      throwExceptionAnyway();
      return new long[1];
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
      throwExceptionAnyway();
      return 0;
    }

    @Override
    public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
      throwExceptionAnyway();
      return 0;
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
      throwExceptionAnyway();
      return 0;
    }

    @Override
    public long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException {
      throwExceptionAnyway();
      return 0;
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
      throwExceptionAnyway();
      return 0;
    }

    @Override
    public long executeLargeUpdate(String sql, String[] columnNames) throws SQLException {
      throwExceptionAnyway();
      return 0;
    }

    @Override
    public Connection getConnection() throws SQLException {
      throwExceptionAnyway();
      return null;
    }

    @Override
    public int getFetchDirection() throws SQLException {
      throwExceptionAnyway();
      return 0;
    }

    @Override
    public int getFetchSize() throws SQLException {
      throwExceptionAnyway();
      return 0;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
      throwExceptionAnyway();
      return null;
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
      throwExceptionAnyway();
      return 0;
    }

    @Override
    public int getMaxRows() throws SQLException {
      throwExceptionAnyway();
      return 0;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
      throwExceptionAnyway();
      return false;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
      throwExceptionAnyway();
      return false;
    }

    @Override
    public int getQueryTimeout() throws SQLException {
      throwExceptionAnyway();
      return 0;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
      throwExceptionAnyway();
      return null;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
      throwExceptionAnyway();
      return 0;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
      throwExceptionAnyway();
      return 0;
    }

    @Override
    public int getResultSetType() throws SQLException {
      throwExceptionAnyway();
      return 0;
    }

    @Override
    public int getUpdateCount() throws SQLException {
      throwExceptionAnyway();
      return 0;
    }

    @Override
    public long getLargeUpdateCount() throws SQLException {
      throwExceptionAnyway();
      return 0;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
      throwExceptionAnyway();
      return null;
    }

    @Override
    public boolean isClosed() throws SQLException {
      throwExceptionAnyway();
      return false;
    }

    @Override
    public boolean isPoolable() throws SQLException {
      throwExceptionAnyway();
      return false;
    }

    @Override
    public void setCursorName(String name) throws SQLException {}

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {}

    @Override
    public void setFetchDirection(int direction) throws SQLException {}

    @Override
    public void setFetchSize(int rows) throws SQLException {}

    @Override
    public void setMaxFieldSize(int max) throws SQLException {}

    @Override
    public void setMaxRows(int max) throws SQLException {}

    @Override
    public void setPoolable(boolean poolable) throws SQLException {}

    @Override
    public void setParameter(String name, Object value) throws SQLException {}

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {}

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
      logger.debug("isWrapperFor(Class<?> iface)");

      return iface.isInstance(this);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
      logger.debug("unwrap(Class<T> iface)");

      if (!iface.isInstance(this)) {
        throw new SQLException(
            this.getClass().getName() + " not unwrappable from " + iface.getName());
      }
      return (T) this;
    }

    @Override
    public void closeOnCompletion() throws SQLException {}

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
      throwExceptionAnyway();
      return false;
    }

    @Override
    public void close() throws SQLException {}

    @Override
    public void close(boolean removeClosedStatementFromConnection) throws SQLException {}

    @Override
    public void cancel() throws SQLException {}

    @Override
    public void clearWarnings() throws SQLException {}

    @Override
    public void addBatch(String sql) throws SQLException {}

    @Override
    public void clearBatch() throws SQLException {}

    @Override
    public void removeClosedResultSet(ResultSet rs) {}
  }
}
