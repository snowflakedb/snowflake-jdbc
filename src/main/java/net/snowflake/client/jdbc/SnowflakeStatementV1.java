package net.snowflake.client.jdbc;

import static net.snowflake.client.jdbc.ErrorCode.FEATURE_UNSUPPORTED;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import net.snowflake.client.core.CancellationReason;
import net.snowflake.client.core.ExecTimeTelemetryData;
import net.snowflake.client.core.ParameterBindingDTO;
import net.snowflake.client.core.ResultUtil;
import net.snowflake.client.core.SFBaseResultSet;
import net.snowflake.client.core.SFBaseStatement;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SFStatement;
import net.snowflake.client.core.StmtUtil;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.client.util.VariableTypeArray;
import net.snowflake.common.core.SqlState;

/** Snowflake statement */
class SnowflakeStatementV1 implements Statement, SnowflakeStatement {
  private static final SFLogger logger = SFLoggerFactory.getLogger(SnowflakeStatementV1.class);

  private static final String NOOP_MESSAGE =
      "This is a dummy SnowflakeStatement, " + "no member function should be called for it.";
  private static final long NO_UPDATES = -1;

  protected final SnowflakeConnectionV1 connection;

  protected final int resultSetType;
  protected final int resultSetConcurrency;
  protected final int resultSetHoldability;
  protected String batchID = "";

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
    logger.trace("SnowflakeStatement(SnowflakeConnectionV1 conn)", false);

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
    ExecTimeTelemetryData execTimeData =
        new ExecTimeTelemetryData("ResultSet Statement.executeQuery(String)", this.batchID);

    raiseSQLExceptionIfStatementIsClosed();
    ResultSet rs = executeQueryInternal(sql, false, null, execTimeData);
    execTimeData.setQueryEnd();
    execTimeData.generateTelemetry();
    logger.debug("Query completed. {}", execTimeData.getLogString());
    return rs;
  }

  /**
   * Execute SQL query asynchronously
   *
   * @param sql sql statement
   * @return ResultSet
   * @throws SQLException if @link{#executeQueryInternal(String, Map)} throws an exception
   */
  public ResultSet executeAsyncQuery(String sql) throws SQLException {
    ExecTimeTelemetryData execTimeData =
        new ExecTimeTelemetryData("ResultSet Statement.executeAsyncQuery(String)", this.batchID);
    raiseSQLExceptionIfStatementIsClosed();
    ResultSet rs = executeQueryInternal(sql, true, null, execTimeData);
    execTimeData.setQueryEnd();
    execTimeData.generateTelemetry();
    logger.debug("Query completed. {}", queryID, execTimeData.getLogString());
    return rs;
  }

  @Override
  public void resultSetMetadataHandler(SFBaseResultSet resultSet) throws SQLException {
    // No-Op.
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
    ExecTimeTelemetryData execTimeData =
        new ExecTimeTelemetryData("ResultSet Statement.executeLargeUpdate(String)", this.batchID);
    long res = executeUpdateInternal(sql, null, true, execTimeData);
    execTimeData.setQueryEnd();
    execTimeData.generateTelemetry();
    logger.debug("Query completed. {}", queryID, execTimeData.getLogString());
    return res;
  }

  long executeUpdateInternal(
      String sql,
      Map<String, ParameterBindingDTO> parameterBindings,
      boolean updateQueryRequired,
      ExecTimeTelemetryData execTimeData)
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
              sql, parameterBindings, SFBaseStatement.CallingMethod.EXECUTE_UPDATE, execTimeData);
      sfResultSet.setSession(this.connection.getSFBaseSession());
      updateCount = ResultUtil.calculateUpdateCount(sfResultSet);
      queryID = sfResultSet.getQueryId();
      resultSetMetadataHandler(sfResultSet);
    } catch (SnowflakeSQLException ex) {
      setQueryIdWhenValidOrNull(ex.getQueryId());
      throw ex;
    } catch (SFException ex) {
      setQueryIdWhenValidOrNull(ex.getQueryId());
      throw new SnowflakeSQLException(
          ex.getCause(), ex.getSqlState(), ex.getVendorCode(), ex.getParams());
    } finally {
      if (resultSet != null && !resultSet.isClosed()) {
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

  private void setQueryIdWhenValidOrNull(String queryId) {
    if (QueryIdValidator.isValid(queryId)) {
      this.queryID = queryId;
    } else {
      this.queryID = null;
    }
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
      String sql,
      boolean asyncExec,
      Map<String, ParameterBindingDTO> parameterBindings,
      ExecTimeTelemetryData execTimeData)
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
                sql, parameterBindings, SFBaseStatement.CallingMethod.EXECUTE_QUERY, execTimeData);
      } else {
        sfResultSet =
            sfBaseStatement.execute(
                sql, parameterBindings, SFBaseStatement.CallingMethod.EXECUTE_QUERY, execTimeData);
        resultSetMetadataHandler(sfResultSet);
      }
      sfResultSet.setSession(this.connection.getSFBaseSession());
      queryID = sfResultSet.getQueryId();

    } catch (SnowflakeSQLException ex) {
      setQueryIdWhenValidOrNull(ex.getQueryId());
      throw ex;
    } catch (SFException ex) {
      setQueryIdWhenValidOrNull(ex.getQueryId());
      throw new SnowflakeSQLException(
          ex.getCause(), ex.getSqlState(), ex.getVendorCode(), ex.getParams());
    }

    if (resultSet != null && !resultSet.isClosed()) {
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
  boolean executeInternal(
      String sql,
      Map<String, ParameterBindingDTO> parameterBindings,
      ExecTimeTelemetryData execTimeData)
      throws SQLException {
    raiseSQLExceptionIfStatementIsClosed();
    connection.injectedDelay();

    logger.debug("Execute: {}", sql);

    String trimmedSql = sql.trim();

    if (trimmedSql.length() >= 20 && trimmedSql.toLowerCase().startsWith("set-sf-property")) {
      // deprecated: sfsql
      executeSetProperty(sql);
      return false;
    }

    SFBaseResultSet sfResultSet;
    try {
      sfResultSet =
          sfBaseStatement.execute(
              sql, parameterBindings, SFBaseStatement.CallingMethod.EXECUTE, execTimeData);
      sfResultSet.setSession(this.connection.getSFBaseSession());
      resultSetMetadataHandler(sfResultSet);
      if (resultSet != null && !resultSet.isClosed()) {
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
        if (resultSet != null && !resultSet.isClosed()) {
          openResultSets.add(resultSet);
        }
        resultSet = null;
        return false;
      }

      updateCount = NO_UPDATES;
      return true;
    } catch (SnowflakeSQLException ex) {
      setQueryIdWhenValidOrNull(ex.getQueryId());
      throw ex;
    } catch (SFException ex) {
      setQueryIdWhenValidOrNull(ex.getQueryId());
      throw new SnowflakeSQLException(
          ex.getCause(), ex.getSqlState(), ex.getVendorCode(), ex.getParams());
    }
  }

  /**
   * @return the query ID of the latest executed query
   */
  public String getQueryID() {
    return queryID;
  }

  /**
   * @return the query IDs of the latest executed batch queries
   */
  public List<String> getBatchQueryIDs() {
    return Collections.unmodifiableList(batchQueryIDs);
  }

  /**
   * @return the child query IDs for the multiple statements query.
   */
  public String[] getChildQueryIds(String queryID) throws SQLException {
    return sfBaseStatement.getChildQueryIds(queryID);
  }

  /**
   * @return the open resultSets from this statement
   */
  public Set<ResultSet> getOpenResultSets() {
    return openResultSets;
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
    ExecTimeTelemetryData execTimeData =
        new ExecTimeTelemetryData("ResultSet Statement.execute(String)", this.batchID);
    boolean res = executeInternal(sql, null, execTimeData);
    execTimeData.setQueryEnd();
    execTimeData.generateTelemetry();
    logger.debug("Query completed. {}", queryID, execTimeData.getLogString());
    return res;
  }

  @Override
  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    logger.trace("execute(String sql, int autoGeneratedKeys)", false);

    if (autoGeneratedKeys == Statement.NO_GENERATED_KEYS) {
      return execute(sql);
    } else {
      throw new SnowflakeLoggedFeatureNotSupportedException(connection.getSFBaseSession());
    }
  }

  @Override
  public boolean execute(String sql, int[] columnIndexes) throws SQLException {
    logger.trace("execute(String sql, int[] columnIndexes)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(connection.getSFBaseSession());
  }

  @Override
  public boolean execute(String sql, String[] columnNames) throws SQLException {
    logger.trace("execute(String sql, String[] columnNames)", false);

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
    logger.trace("int[] executeBatch()", false);
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
    logger.trace("executeBatch()", false);
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
        long cnt =
            this.executeUpdateInternal(
                b.getSql(), b.getParameterBindings(), false, new ExecTimeTelemetryData());
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
    if (this.getSFBaseStatement().getSFBaseSession().getClearBatchOnlyAfterSuccessfulExecution()) {
      clearBatch();
    }
    return updateCounts;
  }

  @Override
  public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    logger.trace("executeUpdate(String sql, int autoGeneratedKeys)", false);

    return (int) this.executeLargeUpdate(sql, autoGeneratedKeys);
  }

  @Override
  public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    logger.trace("executeUpdate(String sql, int autoGeneratedKeys)", false);

    if (autoGeneratedKeys == Statement.NO_GENERATED_KEYS) {
      return executeLargeUpdate(sql);
    } else {
      throw new SnowflakeLoggedFeatureNotSupportedException(connection.getSFBaseSession());
    }
  }

  @Override
  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    logger.trace("executeUpdate(String sql, int[] columnIndexes)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(connection.getSFBaseSession());
  }

  @Override
  public long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException {
    logger.trace("executeLargeUpdate(String sql, int[] columnIndexes)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(connection.getSFBaseSession());
  }

  @Override
  public int executeUpdate(String sql, String[] columnNames) throws SQLException {
    logger.trace("executeUpdate(String sql, String[] columnNames)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(connection.getSFBaseSession());
  }

  @Override
  public long executeLargeUpdate(String sql, String[] columnNames) throws SQLException {
    logger.trace("executeUpdate(String sql, String[] columnNames)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(connection.getSFBaseSession());
  }

  @Override
  public Connection getConnection() throws SQLException {
    logger.trace("getConnection()", false);
    raiseSQLExceptionIfStatementIsClosed();
    return connection;
  }

  @Override
  public int getFetchDirection() throws SQLException {
    logger.trace("getFetchDirection()", false);
    raiseSQLExceptionIfStatementIsClosed();
    return ResultSet.FETCH_FORWARD;
  }

  @Override
  public int getFetchSize() throws SQLException {
    logger.trace("getFetchSize()", false);
    raiseSQLExceptionIfStatementIsClosed();
    return fetchSize;
  }

  @Override
  public ResultSet getGeneratedKeys() throws SQLException {
    logger.trace("getGeneratedKeys()", false);
    raiseSQLExceptionIfStatementIsClosed();
    return new SnowflakeResultSetV1.EmptyResultSet();
  }

  @Override
  public int getMaxFieldSize() throws SQLException {
    logger.trace("getMaxFieldSize()", false);
    raiseSQLExceptionIfStatementIsClosed();
    return connection.getMetaData().getMaxCharLiteralLength();
  }

  @Override
  public int getMaxRows() throws SQLException {
    logger.trace("getMaxRows()", false);
    raiseSQLExceptionIfStatementIsClosed();
    return maxRows;
  }

  @Override
  public boolean getMoreResults() throws SQLException {
    logger.trace("getMoreResults()", false);

    return getMoreResults(Statement.CLOSE_CURRENT_RESULT);
  }

  @Override
  public boolean getMoreResults(int current) throws SQLException {
    logger.trace("getMoreResults(int current)", false);
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
      if (resultSet != null && !resultSet.isClosed()) {
        openResultSets.add(resultSet);
      }
      resultSet = connection.getHandler().createResultSet(sfResultSet, this);
      updateCount = NO_UPDATES;
      return true;
    } else if (sfResultSet != null) // update count returned
    {
      if (resultSet != null && !resultSet.isClosed()) {
        openResultSets.add(resultSet);
      }
      resultSet = null;
      try {
        updateCount = ResultUtil.calculateUpdateCount(sfResultSet);
      } catch (SFException ex) {
        throw new SnowflakeSQLLoggedException(connection.getSFBaseSession(), ex);
      }
      // Multi statement queries should return true while there are still statements to iterate
      // through.
      if (queryID != null
          && sfBaseStatement.hasChildren()
          && sfBaseStatement.getChildQueryIds(queryID).length > 0) {
        return true;
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
    logger.trace("getQueryTimeout()", false);
    raiseSQLExceptionIfStatementIsClosed();
    return this.queryTimeout;
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    logger.trace("getResultSet()", false);
    raiseSQLExceptionIfStatementIsClosed();
    return resultSet;
  }

  @Override
  public int getResultSetConcurrency() throws SQLException {
    logger.trace("getResultSetConcurrency()", false);
    raiseSQLExceptionIfStatementIsClosed();
    return resultSetConcurrency;
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    logger.trace("getResultSetHoldability()", false);
    raiseSQLExceptionIfStatementIsClosed();
    return resultSetHoldability;
  }

  @Override
  public int getResultSetType() throws SQLException {
    logger.trace("getResultSetType()", false);
    raiseSQLExceptionIfStatementIsClosed();
    return this.resultSetType;
  }

  @Override
  public int getUpdateCount() throws SQLException {
    logger.trace("getUpdateCount()", false);
    return (int) getUpdateCountIfDML();
  }

  @Override
  public long getLargeUpdateCount() throws SQLException {
    logger.trace("getLargeUpdateCount()", false);
    return getUpdateCountIfDML();
  }

  private long getUpdateCountIfDML() throws SQLException {
    raiseSQLExceptionIfStatementIsClosed();
    return updateCount;
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    logger.trace("getWarnings()", false);
    raiseSQLExceptionIfStatementIsClosed();
    return sqlWarnings;
  }

  @Override
  public boolean isClosed() throws SQLException {
    logger.trace("isClosed()", false);
    return isClosed; // no exception
  }

  @Override
  public boolean isPoolable() throws SQLException {
    logger.trace("isPoolable()", false);
    raiseSQLExceptionIfStatementIsClosed();
    return poolable;
  }

  @Override
  public void setCursorName(String name) throws SQLException {
    logger.trace("setCursorName(String name)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(connection.getSFBaseSession());
  }

  @Override
  public void setEscapeProcessing(boolean enable) throws SQLException {
    logger.trace("setEscapeProcessing(boolean enable)", false);
    // NOTE: We could raise an exception here, because not implemented
    // but it may break the existing applications. For now returning nothing.
    // we should revisit.
    raiseSQLExceptionIfStatementIsClosed();
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    logger.trace("setFetchDirection(int direction)", false);
    raiseSQLExceptionIfStatementIsClosed();
    if (direction != ResultSet.FETCH_FORWARD) {
      throw new SnowflakeLoggedFeatureNotSupportedException(connection.getSFBaseSession());
    }
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    logger.trace("setFetchSize(int rows), rows={}", rows);
    raiseSQLExceptionIfStatementIsClosed();
    fetchSize = rows;
  }

  @Override
  public void setMaxFieldSize(int max) throws SQLException {
    logger.trace("setMaxFieldSize(int max)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(connection.getSFBaseSession());
  }

  @Override
  public void setMaxRows(int max) throws SQLException {
    logger.trace("setMaxRows(int max)", false);

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
    logger.trace("setPoolable(boolean poolable)", false);
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
    logger.trace("setParameter", false);

    try {
      if (this.sfBaseStatement != null) {
        this.sfBaseStatement.addProperty(name, value);
      }
    } catch (SFException ex) {
      throw new SnowflakeSQLException(ex);
    }
  }

  @Override
  public void setBatchID(String batchID) {
    this.batchID = batchID;
  }

  @Override
  public void setQueryTimeout(int seconds) throws SQLException {
    logger.trace("setQueryTimeout(int seconds)", false);
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
  public void setAsyncQueryTimeout(int seconds) throws SQLException {
    logger.trace("setAsyncQueryTimeout(int seconds)", false);
    raiseSQLExceptionIfStatementIsClosed();

    try {
      if (this.sfBaseStatement != null) {
        this.sfBaseStatement.addProperty("STATEMENT_TIMEOUT_IN_SECONDS", seconds);
        // disable statement level query timeout to avoid override by connection parameter
        this.setQueryTimeout(0);
      }
    } catch (SFException ex) {
      throw new SnowflakeSQLException(
          null, ex.getCause(), ex.getSqlState(), ex.getVendorCode(), ex.getParams());
    }
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    logger.trace("isWrapperFor(Class<?> iface)", false);

    return iface.isInstance(this);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    logger.trace("unwrap(Class<T> iface)", false);

    if (!iface.isInstance(this)) {
      throw new SQLException(
          this.getClass().getName() + " not unwrappable from " + iface.getName());
    }
    return (T) this;
  }

  @Override
  public void closeOnCompletion() throws SQLException {
    logger.trace("closeOnCompletion()", false);
    throw new SnowflakeLoggedFeatureNotSupportedException(connection.getSFBaseSession());
  }

  @Override
  public boolean isCloseOnCompletion() throws SQLException {
    logger.trace("isCloseOnCompletion()", false);
    throw new SnowflakeLoggedFeatureNotSupportedException(connection.getSFBaseSession());
  }

  @Override
  public void close() throws SQLException {
    close(true);
  }

  public void close(boolean removeClosedStatementFromConnection) throws SQLException {
    logger.trace("close()", false);

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
    logger.trace("cancel()", false);
    raiseSQLExceptionIfStatementIsClosed();

    try {
      sfBaseStatement.cancel(CancellationReason.CLIENT_REQUESTED);
    } catch (SFException ex) {
      throw new SnowflakeSQLException(ex, ex.getSqlState(), ex.getVendorCode(), ex.getParams());
    }
  }

  @Override
  public void clearWarnings() throws SQLException {
    logger.trace("clearWarnings()", false);
    raiseSQLExceptionIfStatementIsClosed();
    sqlWarnings = null;
  }

  @Override
  public void addBatch(String sql) throws SQLException {
    logger.trace("addBatch(String sql)", false);

    raiseSQLExceptionIfStatementIsClosed();

    batch.add(new BatchEntry(sql, null));
  }

  @Override
  public void clearBatch() throws SQLException {
    logger.trace("clearBatch()", false);

    raiseSQLExceptionIfStatementIsClosed();

    batch.clear();
  }

  private void executeSetProperty(final String sql) {
    logger.trace("setting property", false);

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

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
      throw new SQLException(NOOP_MESSAGE);
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
      throw new SQLException(NOOP_MESSAGE);
    }

    @Override
    public long executeLargeUpdate(String sql) throws SQLException {
      throw new SQLException(NOOP_MESSAGE);
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
      throw new SQLException(NOOP_MESSAGE);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
      throw new SQLException(NOOP_MESSAGE);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
      throw new SQLException(NOOP_MESSAGE);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
      throw new SQLException(NOOP_MESSAGE);
    }

    @Override
    public int[] executeBatch() throws SQLException {
      throw new SQLException(NOOP_MESSAGE);
    }

    @Override
    public long[] executeLargeBatch() throws SQLException {
      throw new SQLException(NOOP_MESSAGE);
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
      throw new SQLException(NOOP_MESSAGE);
    }

    @Override
    public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
      throw new SQLException(NOOP_MESSAGE);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
      throw new SQLException(NOOP_MESSAGE);
    }

    @Override
    public long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException {
      throw new SQLException(NOOP_MESSAGE);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
      throw new SQLException(NOOP_MESSAGE);
    }

    @Override
    public long executeLargeUpdate(String sql, String[] columnNames) throws SQLException {
      throw new SQLException(NOOP_MESSAGE);
    }

    @Override
    public Connection getConnection() throws SQLException {
      throw new SQLException(NOOP_MESSAGE);
    }

    @Override
    public int getFetchDirection() throws SQLException {
      throw new SQLException(NOOP_MESSAGE);
    }

    @Override
    public int getFetchSize() throws SQLException {
      throw new SQLException(NOOP_MESSAGE);
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
      throw new SQLException(NOOP_MESSAGE);
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
      throw new SQLException(NOOP_MESSAGE);
    }

    @Override
    public int getMaxRows() throws SQLException {
      throw new SQLException(NOOP_MESSAGE);
    }

    @Override
    public boolean getMoreResults() throws SQLException {
      throw new SQLException(NOOP_MESSAGE);
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
      throw new SQLException(NOOP_MESSAGE);
    }

    @Override
    public int getQueryTimeout() throws SQLException {
      throw new SQLException(NOOP_MESSAGE);
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
      throw new SQLException(NOOP_MESSAGE);
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
      throw new SQLException(NOOP_MESSAGE);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
      throw new SQLException(NOOP_MESSAGE);
    }

    @Override
    public int getResultSetType() throws SQLException {
      throw new SQLException(NOOP_MESSAGE);
    }

    @Override
    public int getUpdateCount() throws SQLException {
      throw new SQLException(NOOP_MESSAGE);
    }

    @Override
    public long getLargeUpdateCount() throws SQLException {
      throw new SQLException(NOOP_MESSAGE);
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
      throw new SQLException(NOOP_MESSAGE);
    }

    @Override
    public boolean isClosed() throws SQLException {
      throw new SQLException(NOOP_MESSAGE);
    }

    @Override
    public boolean isPoolable() throws SQLException {
      throw new SQLException(NOOP_MESSAGE);
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
    public void setAsyncQueryTimeout(int seconds) throws SQLException {}

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
      logger.trace("isWrapperFor(Class<?> iface)", false);

      return iface.isInstance(this);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
      logger.trace("unwrap(Class<T> iface)", false);

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
      throw new SQLException(NOOP_MESSAGE);
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
