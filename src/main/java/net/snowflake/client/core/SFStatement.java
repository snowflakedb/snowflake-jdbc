/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import static net.snowflake.client.core.SessionUtil.DEFAULT_CLIENT_MEMORY_LIMIT;
import static net.snowflake.client.core.SessionUtil.DEFAULT_CLIENT_PREFETCH_THREADS;
import static net.snowflake.client.core.SessionUtil.MAX_CLIENT_CHUNK_SIZE;
import static net.snowflake.client.core.SessionUtil.MIN_CLIENT_CHUNK_SIZE;
import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;

import com.fasterxml.jackson.databind.JsonNode;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import net.snowflake.client.core.BasicEvent.QueryState;
import net.snowflake.client.core.bind.BindException;
import net.snowflake.client.core.bind.BindUploader;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.QueryStatusV2;
import net.snowflake.client.jdbc.SnowflakeDriver;
import net.snowflake.client.jdbc.SnowflakeFileTransferAgent;
import net.snowflake.client.jdbc.SnowflakeReauthenticationRequest;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.SnowflakeSQLLoggedException;
import net.snowflake.client.jdbc.telemetry.TelemetryData;
import net.snowflake.client.jdbc.telemetry.TelemetryField;
import net.snowflake.client.jdbc.telemetry.TelemetryUtil;
import net.snowflake.client.jdbc.telemetryOOB.TelemetryService;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.common.core.SqlState;
import org.apache.http.client.methods.HttpRequestBase;

/** Snowflake statement */
public class SFStatement extends SFBaseStatement {

  private static final SFLogger logger = SFLoggerFactory.getLogger(SFStatement.class);

  private SFSession session;

  private SFBaseResultSet resultSet = null;

  private HttpRequestBase httpRequest;

  private Boolean isClosed = false;

  private Integer sequenceId = -1;

  private String requestId = null;

  private String sqlText = null;

  private final AtomicBoolean canceling = new AtomicBoolean(false);

  private boolean isFileTransfer = false;

  private SnowflakeFileTransferAgent transferAgent = null;

  private static final int MAX_BINDING_PARAMS_FOR_LOGGING = 1000;

  /** id used in combine describe and execute */
  private String describeJobUUID;

  // list of child result objects for queries called by the current query, if any
  private List<SFChildResult> childResults = null;

  // Three parameters adjusted in conservative memory usage mode
  private int conservativePrefetchThreads;
  private int conservativeResultChunkSize;
  private long conservativeMemoryLimit; // in bytes

  public SFStatement(SFSession session) {
    logger.trace("SFStatement(SFSession session)", false);

    this.session = session;
    Integer queryTimeout = session == null ? null : session.getQueryTimeout();
    this.queryTimeout = queryTimeout != null ? queryTimeout : this.queryTimeout;
    verifyArrowSupport();
  }

  private void verifyArrowSupport() {
    if (SnowflakeDriver.isDisableArrowResultFormat()) {
      logger.debug(
          "Disable arrow support: {}", SnowflakeDriver.getDisableArrowResultFormatMessage());
      statementParametersMap.put("JDBC_QUERY_RESULT_FORMAT", "JSON");
    }
  }

  /**
   * Sanity check query text
   *
   * @param sql The SQL statement to check
   * @throws SQLException Will be thrown if sql is null or empty
   */
  private void sanityCheckQuery(String sql) throws SQLException {
    if (sql == null || sql.isEmpty()) {
      throw new SnowflakeSQLException(
          SqlState.SQL_STATEMENT_NOT_YET_COMPLETE, ErrorCode.INVALID_SQL.getMessageCode(), sql);
    }
  }

  /**
   * Execute SQL query with an option for describe only
   *
   * @param sql sql statement
   * @param describeOnly true if describe only
   * @return query result set
   * @throws SQLException if connection is already closed
   * @throws SFException if result set is null
   */
  private SFBaseResultSet executeQuery(
      String sql,
      Map<String, ParameterBindingDTO> parametersBinding,
      boolean describeOnly,
      boolean asyncExec,
      CallingMethod caller,
      ExecTimeTelemetryData execTimeData)
      throws SQLException, SFException {
    sanityCheckQuery(sql);

    String trimmedSql = sql.trim();

    // snowflake specific client side commands
    if (isFileTransfer(trimmedSql)) {
      // Server side value or Connection string value is false then disable the PUT/GET command
      if ((session != null && !(session.getJdbcEnablePutGet() && session.getEnablePutGet()))) {
        // PUT/GET command disabled either on server side or in the client connection string
        logger.debug("Executing file transfer locally is disabled: {}", sql);
        throw new SnowflakeSQLException("File transfers have been disabled.");
      }

      // PUT/GET command
      logger.debug("Executing file transfer locally: {}", sql);

      return executeFileTransfer(sql);
    }

    // NOTE: It is intentional two describeOnly parameters are specified.
    return executeQueryInternal(
        sql,
        parametersBinding,
        describeOnly,
        describeOnly, // internal query if describeOnly is true
        asyncExec,
        caller,
        execTimeData);
  }

  /**
   * Describe a statement
   *
   * @param sql statement
   * @return metadata of statement including result set metadata and binding information
   * @throws SQLException if connection is already closed
   * @throws SFException if result set is null
   */
  @Override
  public SFPreparedStatementMetaData describe(String sql) throws SFException, SQLException {
    SFBaseResultSet baseResultSet =
        executeQuery(sql, null, true, false, null, new ExecTimeTelemetryData());

    describeJobUUID = baseResultSet.getQueryId();

    return new SFPreparedStatementMetaData(
        baseResultSet.getMetaData(),
        baseResultSet.getStatementType(),
        baseResultSet.getNumberOfBinds(),
        baseResultSet.isArrayBindSupported(),
        baseResultSet.getMetaDataOfBinds(),
        true); // valid metadata
  }

  /**
   * Internal method for executing a query with bindings accepted.
   *
   * <p>
   *
   * @param sql sql statement
   * @param parameterBindings binding information
   * @param describeOnly true if just showing result set metadata
   * @param internal true if internal command not showing up in the history
   * @param caller the JDBC method that called this function, null if none
   * @return snowflake query result set
   * @throws SQLException if connection is already closed
   * @throws SFException if result set is null
   */
  SFBaseResultSet executeQueryInternal(
      String sql,
      Map<String, ParameterBindingDTO> parameterBindings,
      boolean describeOnly,
      boolean internal,
      boolean asyncExec,
      CallingMethod caller,
      ExecTimeTelemetryData execTimeData)
      throws SQLException, SFException {
    resetState();

    logger.debug("ExecuteQuery: {}", sql);

    if (session == null || session.isClosed()) {
      throw new SQLException("connection is closed");
    }

    Object result =
        executeHelper(
            sql,
            StmtUtil.SF_MEDIA_TYPE,
            parameterBindings,
            describeOnly,
            internal,
            asyncExec,
            execTimeData);

    if (result == null) {
      throw new SnowflakeSQLLoggedException(
          session,
          ErrorCode.INTERNAL_ERROR.getMessageCode(),
          SqlState.INTERNAL_ERROR,
          "got null result");
    }

    /*
     * we sort the result if the connection is in sorting mode
     */
    Object sortProperty = session.getSessionPropertyByKey("sort");

    boolean sortResult = sortProperty != null && (Boolean) sortProperty;

    logger.debug("Creating result set", false);

    try {
      JsonNode jsonResult = (JsonNode) result;
      resultSet = SFResultSetFactory.getResultSet(jsonResult, this, sortResult, execTimeData);
      childResults = ResultUtil.getChildResults(session, requestId, jsonResult);

      // if child results are available, skip over this result set and set the
      // current result to the first child's result.
      // we still construct the first result set for its side effects.
      if (!childResults.isEmpty()) {
        SFStatementType type = childResults.get(0).getType();

        // ensure first query type matches the calling JDBC method, if exists
        if (caller == CallingMethod.EXECUTE_QUERY && !type.isGenerateResultSet()) {
          throw new SnowflakeSQLLoggedException(
              session, ErrorCode.QUERY_FIRST_RESULT_NOT_RESULT_SET);
        } else if (caller == CallingMethod.EXECUTE_UPDATE && type.isGenerateResultSet()) {
          throw new SnowflakeSQLLoggedException(
              session, ErrorCode.UPDATE_FIRST_RESULT_NOT_UPDATE_COUNT);
        }

        // this will update resultSet to point to the first child result before we return it
        getMoreResults();
      }
    } catch (SnowflakeSQLException | OutOfMemoryError ex) {
      // snow-24428: no need to generate incident for exceptions we generate
      // snow-29403: or client OOM
      throw ex;
    } catch (Throwable ex) {
      // SNOW-22813 log exception
      logger.error("Exception creating result", ex);

      throw new SFException(
          ErrorCode.INTERNAL_ERROR, IncidentUtil.oneLiner("exception creating result", ex));
    }
    logger.debug("Done creating result set", false);

    if (asyncExec) {
      session.addQueryToActiveQueryList(resultSet.getQueryId());
    }
    execTimeData.setQueryId(resultSet.getQueryId());
    return resultSet;
  }

  /**
   * Set a time bomb to cancel the outstanding query when timeout is reached.
   *
   * @param executor object to execute statement cancel request
   */
  private void setTimeBomb(ScheduledExecutorService executor) {
    class TimeBombTask implements Callable<Void> {

      private final SFStatement statement;

      private TimeBombTask(SFStatement statement) {
        this.statement = statement;
      }

      @Override
      public Void call() throws SQLException {
        try {
          statement.cancel(CancellationReason.TIMEOUT);
        } catch (SFException ex) {
          throw new SnowflakeSQLLoggedException(
              session, ex.getSqlState(), ex.getVendorCode(), ex, ex.getParams());
        }
        return null;
      }
    }

    executor.schedule(new TimeBombTask(this), this.queryTimeout, TimeUnit.SECONDS);
  }

  /**
   * A helper method to build URL and submit the SQL to snowflake for exec
   *
   * @param sql sql statement
   * @param mediaType media type
   * @param bindValues map of binding values
   * @param describeOnly whether only show the result set metadata
   * @param internal run internal query not showing up in history
   * @param asyncExec is async execute
   * @param execTimeData ExecTimeTelemetryData
   * @return raw json response
   * @throws SFException if query is canceled
   * @throws SnowflakeSQLException if query is already running
   */
  public Object executeHelper(
      String sql,
      String mediaType,
      Map<String, ParameterBindingDTO> bindValues,
      boolean describeOnly,
      boolean internal,
      boolean asyncExec,
      ExecTimeTelemetryData execTimeData)
      throws SnowflakeSQLException, SFException {
    ScheduledExecutorService executor = null;

    try {
      synchronized (this) {
        if (isClosed) {
          throw new SFException(ErrorCode.STATEMENT_CLOSED);
        }

        // initialize a sequence id if not closed or not for aborting
        if (canceling.get()) {
          // nothing to do if canceled
          throw new SFException(ErrorCode.QUERY_CANCELED);
        }

        if (this.requestId != null) {
          throw new SnowflakeSQLLoggedException(
              session,
              ErrorCode.STATEMENT_ALREADY_RUNNING_QUERY.getMessageCode(),
              SqlState.FEATURE_NOT_SUPPORTED);
        }

        this.requestId = UUIDUtils.getUUID().toString();
        execTimeData.setRequestId(requestId);
        this.sequenceId = session.getAndIncrementSequenceId();

        this.sqlText = sql;
      }

      EventUtil.triggerStateTransition(
          BasicEvent.QueryState.QUERY_STARTED,
          String.format(QueryState.QUERY_STARTED.getArgString(), requestId));

      // if there are a large number of bind values, we should upload them to stage
      // instead of passing them in the payload (if enabled)
      execTimeData.setBindStart();
      int numBinds = BindUploader.arrayBindValueCount(bindValues);
      String bindStagePath = null;
      if (0 < session.getArrayBindStageThreshold()
          && session.getArrayBindStageThreshold() <= numBinds
          && !describeOnly
          && BindUploader.isArrayBind(bindValues)) {
        try (BindUploader uploader = BindUploader.newInstance(session, requestId)) {
          uploader.upload(bindValues);
          bindStagePath = uploader.getStagePath();
        } catch (BindException ex) {
          logger.debug(
              "Exception encountered trying to upload binds to stage with input stream. Attaching"
                  + " binds in payload instead. ",
              ex);
          TelemetryData errorLog = TelemetryUtil.buildJobData(this.requestId, ex.type.field, 1);
          this.session.getTelemetryClient().addLogToBatch(errorLog);
        } catch (SQLException ex) {
          logger.debug(
              "Exception encountered trying to upload binds to stage with input stream. Attaching"
                  + " binds in payload instead. ",
              ex);
          TelemetryData errorLog =
              TelemetryUtil.buildJobData(this.requestId, TelemetryField.FAILED_BIND_UPLOAD, 1);
          this.session.getTelemetryClient().addLogToBatch(errorLog);
        }
      }

      if (session.isConservativeMemoryUsageEnabled()) {
        logger.debug("JDBC conservative memory usage is enabled.", false);
        calculateConservativeMemoryUsage();
      }

      StmtUtil.StmtInput stmtInput = new StmtUtil.StmtInput();
      stmtInput
          .setSql(sql)
          .setMediaType(mediaType)
          .setInternal(internal)
          .setDescribeOnly(describeOnly)
          .setAsync(asyncExec)
          .setServerUrl(session.getServerUrl())
          .setRequestId(requestId)
          .setSequenceId(sequenceId)
          .setParametersMap(statementParametersMap)
          .setSessionToken(session.getSessionToken())
          .setNetworkTimeoutInMillis(session.getNetworkTimeoutInMilli())
          .setInjectSocketTimeout(session.getInjectSocketTimeout())
          .setInjectClientPause(session.getInjectClientPause())
          .setCanceling(canceling)
          .setRetry(false)
          .setDescribedJobId(describeJobUUID)
          .setCombineDescribe(session.getEnableCombineDescribe())
          .setQuerySubmissionTime(System.currentTimeMillis())
          .setServiceName(session.getServiceName())
          .setOCSPMode(session.getOCSPMode())
          .setHttpClientSettingsKey(session.getHttpClientKey())
          .setMaxRetries(session.getMaxHttpRetries())
          .setAdditionalHttpHeadersForSnowsight(session.getHttpHeaders())
          .setQueryContextDTO(session.isAsyncSession() ? null : session.getQueryContextDTO());
      if (bindStagePath != null) {
        stmtInput.setBindValues(null).setBindStage(bindStagePath);
        // use the new SQL format for this query so dates/timestamps are parsed correctly
        setUseNewSqlFormat(true);
        statementParametersMap.put("TIMESTAMP_INPUT_FORMAT", "AUTO");
      } else {
        stmtInput.setBindValues(bindValues).setBindStage(null);
      }
      if (numBinds > 0 && session.getPreparedStatementLogging()) {
        if (numBinds > MAX_BINDING_PARAMS_FOR_LOGGING) {
          logger.debug(
              "Number of binds exceeds logging limit. Printing off {} binding parameters.",
              MAX_BINDING_PARAMS_FOR_LOGGING);
        } else {
          logger.debug("Printing off {} binding parameters.", numBinds);
        }
        int counter = 0;
        // if it's an array bind, print off the first few rows from each column.
        if (BindUploader.isArrayBind(bindValues)) {
          int numRowsPrinted = MAX_BINDING_PARAMS_FOR_LOGGING / bindValues.size();
          if (numRowsPrinted <= 0) {
            numRowsPrinted = 1;
          }
          for (Map.Entry<String, ParameterBindingDTO> entry : bindValues.entrySet()) {
            List<String> bindRows = (List<String>) entry.getValue().getValue();
            if (numRowsPrinted >= bindRows.size()) {
              numRowsPrinted = bindRows.size();
            }
            String rows = "[";
            for (int i = 0; i < numRowsPrinted; i++) {
              rows += bindRows.get(i) + ", ";
            }
            rows += "]";
            logger.debug("Column {}: {}", entry.getKey(), rows);
            counter += numRowsPrinted;
            if (counter >= MAX_BINDING_PARAMS_FOR_LOGGING) {
              break;
            }
          }
        }
        // not an array, just a bunch of columns
        else {
          for (Map.Entry<String, ParameterBindingDTO> entry : bindValues.entrySet()) {
            if (counter >= MAX_BINDING_PARAMS_FOR_LOGGING) {
              break;
            }
            counter++;
            logger.debug("Column {}: {}", entry.getKey(), entry.getValue().getValue());
          }
        }
      }
      execTimeData.setBindEnd();

      if (canceling.get()) {
        logger.debug("Query cancelled", false);

        throw new SFException(ErrorCode.QUERY_CANCELED);
      }

      // if timeout is set, start a thread to cancel the request after timeout
      // reached.
      if (this.queryTimeout > 0) {
        executor = Executors.newScheduledThreadPool(1);
        setTimeBomb(executor);
      }

      StmtUtil.StmtOutput stmtOutput = null;
      boolean sessionRenewed;

      do {
        sessionRenewed = false;
        try {
          stmtOutput = StmtUtil.execute(stmtInput, execTimeData);
          break;
        } catch (SnowflakeSQLException ex) {
          if (ex.getErrorCode() == Constants.SESSION_EXPIRED_GS_CODE) {
            try {
              // renew the session
              session.renewSession(stmtInput.sessionToken);
            } catch (SnowflakeReauthenticationRequest ex0) {
              if (session.isExternalbrowserAuthenticator()) {
                reauthenticate();
              } else {
                throw ex0;
              }
            }
            // SNOW-18822: reset session token for the statement
            stmtInput.setSessionToken(session.getSessionToken());
            stmtInput.setRetry(true);
            sessionRenewed = true;
            execTimeData.incrementRetryCount();
            execTimeData.addRetryLocation("renewSession");
            logger.debug("Session got renewed, will retry", false);
          } else {
            throw ex;
          }
        }
      } while (sessionRenewed && !canceling.get());

      // Debugging/Testing for incidents
      if (Boolean.TRUE
          .toString()
          .equalsIgnoreCase(systemGetProperty("snowflake.enable_incident_test1"))) {
        throw new SFException(ErrorCode.STATEMENT_CLOSED);
      }

      synchronized (this) {
        /*
         * done with the remote execution of the query. set sequenceId to -1
         * and request id to null so that we don't try to abort it upon canceling.
         */
        this.sequenceId = -1;
        this.requestId = null;
      }

      if (canceling.get()) {
        // If we are here, this is the context for the initial query that
        // is being canceled. Raise an exception anyway here even if
        // the server fails to abort it.
        throw new SFException(ErrorCode.QUERY_CANCELED);
      }

      logger.debug("Returning from executeHelper", false);

      if (stmtOutput != null) {
        return stmtOutput.getResult();
      }
      throw new SFException(ErrorCode.INTERNAL_ERROR);
    } catch (SFException | SnowflakeSQLException ex) {
      isClosed = true;
      throw ex;
    } finally {
      if (executor != null) {
        executor.shutdownNow();
      }
      // if this query enabled the new SQL format, re-disable it now
      setUseNewSqlFormat(false);
    }
  }

  /**
   * calculate conservative memory limit and the number of prefetch threads before query execution
   */
  private void calculateConservativeMemoryUsage() {
    int clientMemoryLimit = session.getClientMemoryLimit();
    int clientPrefetchThread = session.getClientPrefetchThreads();
    int clientChunkSize = session.getClientResultChunkSize();

    long memoryLimitInBytes;
    if (clientMemoryLimit == DEFAULT_CLIENT_MEMORY_LIMIT) {
      // this is all default scenario
      // only allows JDBC use at most 80% of free memory
      long freeMemoryToUse = Runtime.getRuntime().freeMemory() * 8 / 10;
      memoryLimitInBytes =
          Math.min(
              (long) 2 * clientPrefetchThread * clientChunkSize * 1024 * 1024, freeMemoryToUse);
    } else {
      memoryLimitInBytes = (long) clientMemoryLimit * 1024 * 1024;
    }
    conservativeMemoryLimit = memoryLimitInBytes;
    reducePrefetchThreadsAndChunkSizeToFitMemoryLimit(
        conservativeMemoryLimit, clientPrefetchThread, clientChunkSize);
  }

  private void updateConservativeResultChunkSize(int clientChunkSize) {
    if (clientChunkSize != conservativeResultChunkSize) {
      logger.debug(
          "conservativeResultChunkSize changed from {} to {}",
          conservativeResultChunkSize,
          clientChunkSize);
      conservativeResultChunkSize = clientChunkSize;
      statementParametersMap.put("CLIENT_RESULT_CHUNK_SIZE", conservativeResultChunkSize);
    }
  }

  private void reducePrefetchThreadsAndChunkSizeToFitMemoryLimit(
      long clientMemoryLimit, int clientPrefetchThread, int clientChunkSize) {
    if (clientPrefetchThread != DEFAULT_CLIENT_PREFETCH_THREADS) {
      // prefetch threads are configured so only reduce chunk size
      conservativePrefetchThreads = clientPrefetchThread;
      for (;
          clientChunkSize >= MIN_CLIENT_CHUNK_SIZE;
          clientChunkSize -= session.getConservativeMemoryAdjustStep()) {
        if (clientMemoryLimit >= (long) 2 * clientPrefetchThread * clientChunkSize * 1024 * 1024) {
          updateConservativeResultChunkSize(clientChunkSize);
          return;
        }
      }
      updateConservativeResultChunkSize(MIN_CLIENT_CHUNK_SIZE);
    } else {
      // reduce both prefetch threads and chunk size
      while (clientPrefetchThread > 1) {
        for (clientChunkSize = MAX_CLIENT_CHUNK_SIZE;
            clientChunkSize >= MIN_CLIENT_CHUNK_SIZE;
            clientChunkSize -= session.getConservativeMemoryAdjustStep()) {
          if (clientMemoryLimit
              >= (long) 2 * clientPrefetchThread * clientChunkSize * 1024 * 1024) {
            conservativePrefetchThreads = clientPrefetchThread;
            updateConservativeResultChunkSize(clientChunkSize);
            return;
          }
        }
        clientPrefetchThread--;
      }
      conservativePrefetchThreads = clientPrefetchThread;
      updateConservativeResultChunkSize(MIN_CLIENT_CHUNK_SIZE);
    }
  }

  /**
   * @return conservative prefetch threads before fetching results
   */
  public int getConservativePrefetchThreads() {
    return conservativePrefetchThreads;
  }

  /**
   * @return conservative memory limit before fetching results
   */
  public long getConservativeMemoryLimit() {
    return conservativeMemoryLimit;
  }

  /**
   * Return an array of child query ID for the given query ID.
   *
   * <p>If the given query ID is for a multiple statements query, it returns an array of its child
   * statements, otherwise, it returns an array to include the given query ID.
   *
   * @param queryID The given query ID
   * @return An array of child query IDs
   * @throws SQLException If the query is running or the corresponding query is FAILED.
   */
  @Override
  public String[] getChildQueryIds(String queryID) throws SQLException {
    QueryStatusV2 qs = session.getQueryStatusV2(queryID);
    if (qs.isStillRunning()) {
      throw new SQLException(
          "Status of query associated with resultSet is "
              + qs.getDescription()
              + ". Results not generated.");
    }
    try {
      JsonNode jsonResult = StmtUtil.getQueryResultJSON(queryID, session);
      List<SFChildResult> childResults = ResultUtil.getChildResults(session, requestId, jsonResult);
      List<String> resultList = new ArrayList<>();
      for (int i = 0; i < childResults.size(); i++) {
        resultList.add(childResults.get(i).getId());
      }
      if (resultList.isEmpty()) {
        resultList.add(queryID);
      }
      String[] result = new String[resultList.size()];
      resultList.toArray(result);
      return result;
    } catch (SFException ex) {
      throw new SnowflakeSQLException(ex);
    }
  }

  @Override
  public SFBaseResultSet execute(
      String sql,
      Map<String, ParameterBindingDTO> parametersBinding,
      CallingMethod caller,
      ExecTimeTelemetryData execTimeData)
      throws SQLException, SFException {
    return execute(sql, false, parametersBinding, caller, execTimeData);
  }

  private void reauthenticate() throws SFException, SnowflakeSQLException {
    SFLoginInput input =
        new SFLoginInput()
            .setRole(session.getRole())
            .setWarehouse(session.getWarehouse())
            .setDatabaseName(session.getDatabase())
            .setSchemaName(session.getSchema())
            .setOCSPMode(session.getOCSPMode())
            .setHttpClientSettingsKey(session.getHttpClientKey());

    session.open();
  }

  /**
   * A helper method to build URL and cancel the SQL for exec
   *
   * @param sql sql statement
   * @param mediaType media type
   * @param cancellationReason reason for the cancellation
   * @throws SnowflakeSQLException if failed to cancel the statement
   * @throws SFException if statement is already closed
   */
  private void cancelHelper(String sql, String mediaType, CancellationReason cancellationReason)
      throws SnowflakeSQLException, SFException {
    synchronized (this) {
      if (isClosed) {
        throw new SFException(ErrorCode.INTERNAL_ERROR, "statement already closed");
      }
    }

    StmtUtil.StmtInput stmtInput = new StmtUtil.StmtInput();
    stmtInput
        .setServerUrl(session.getServerUrl())
        .setSql(sql)
        .setMediaType(mediaType)
        .setRequestId(requestId)
        .setSessionToken(session.getSessionToken())
        .setServiceName(session.getServiceName())
        .setOCSPMode(session.getOCSPMode())
        .setMaxRetries(session.getMaxHttpRetries())
        .setHttpClientSettingsKey(session.getHttpClientKey());

    StmtUtil.cancel(stmtInput, cancellationReason);

    synchronized (this) {
      /*
       * done with the remote execution of the query. set sequenceId to -1
       * and request id to null so that we don't try to abort it again upon
       * canceling.
       */
      this.sequenceId = -1;
      this.requestId = null;
    }
  }

  /**
   * Execute sql
   *
   * @param sql sql statement.
   * @param asyncExec is async exec
   * @param parametersBinding parameters to bind
   * @param caller the JDBC interface method that called this method, if any
   * @param execTimeData ExecTimeTelemetryData
   * @return whether there is result set or not
   * @throws SQLException if failed to execute sql
   * @throws SFException exception raised from Snowflake components
   * @throws SQLException if SQL error occurs
   */
  public SFBaseResultSet execute(
      String sql,
      boolean asyncExec,
      Map<String, ParameterBindingDTO> parametersBinding,
      CallingMethod caller,
      ExecTimeTelemetryData execTimeData)
      throws SQLException, SFException {
    TelemetryService.getInstance().updateContext(session.getSnowflakeConnectionString());
    sanityCheckQuery(sql);

    session.injectedDelay();

    if (session.getPreparedStatementLogging()) {
      logger.info("Execute: {}", sql);
    } else {
      logger.debug("Execute: {}", sql);
    }

    String trimmedSql = sql.trim();

    if (trimmedSql.length() >= 20 && trimmedSql.toLowerCase().startsWith("set-sf-property")) {
      executeSetProperty(sql);
      return null;
    }
    return executeQuery(sql, parametersBinding, false, asyncExec, caller, execTimeData);
  }

  private SFBaseResultSet executeFileTransfer(String sql) throws SQLException, SFException {
    session.injectedDelay();

    resetState();

    logger.debug("Entering executeFileTransfer", false);

    isFileTransfer = true;
    transferAgent = new SnowflakeFileTransferAgent(sql, session, this);

    try {
      transferAgent.execute();

      logger.debug("Setting result set", false);

      resultSet = (SFFixedViewResultSet) transferAgent.getResultSet();
      childResults = Collections.emptyList();

      logger.debug("Number of cols: {}", resultSet.getMetaData().getColumnCount());
      logger.debug("Completed transferring data", false);
      return resultSet;
    } catch (SQLException ex) {
      logger.debug("Exception: {}", ex.getMessage());
      throw ex;
    }
  }

  @Override
  public void close() {
    logger.trace("void close()", false);

    if (requestId != null) {
      EventUtil.triggerStateTransition(
          BasicEvent.QueryState.QUERY_ENDED,
          String.format(QueryState.QUERY_ENDED.getArgString(), requestId));
    }

    resultSet = null;
    childResults = null;
    isClosed = true;

    if (httpRequest != null) {
      logger.debug("Releasing connection for the http request", false);

      httpRequest.releaseConnection();
      httpRequest = null;
    }

    session.getTelemetryClient().sendBatchAsync();

    isFileTransfer = false;
    transferAgent = null;
  }

  @Override
  public void cancel() throws SFException, SQLException {
    logger.trace("void cancel()", false);
    cancel(CancellationReason.UNKNOWN);
  }

  @Override
  public void cancel(CancellationReason cancellationReason) throws SFException, SQLException {
    logger.trace("void cancel(CancellationReason)", false);

    if (canceling.get()) {
      logger.debug("Query is already cancelled", false);
      return;
    }

    canceling.set(true);

    if (isFileTransfer) {
      if (transferAgent != null) {
        logger.debug("Cancel file transferring ... ", false);
        transferAgent.cancel();
      }
    } else {
      synchronized (this) {
        // the query hasn't been sent to GS yet, just mark the stmt closed
        if (requestId == null) {
          logger.debug("No remote query outstanding", false);

          return;
        }
      }

      // cancel the query on the server side if it has been issued
      cancelHelper(this.sqlText, StmtUtil.SF_MEDIA_TYPE, cancellationReason);
    }
  }

  private void resetState() {
    resultSet = null;
    childResults = null;

    if (httpRequest != null) {
      httpRequest.releaseConnection();
      httpRequest = null;
    }

    isClosed = false;
    sequenceId = -1;
    requestId = null;
    sqlText = null;
    canceling.set(false);

    isFileTransfer = false;
    transferAgent = null;
  }

  @Override
  public SFBaseSession getSFBaseSession() {
    return session;
  }

  // *NOTE* this new SQL format is incomplete. It should only be used under certain circumstances.
  private void setUseNewSqlFormat(boolean useNewSqlFormat) throws SFException {
    this.addProperty("NEW_SQL_FORMAT", useNewSqlFormat);
  }

  public boolean getMoreResults() throws SQLException {
    return getMoreResults(Statement.CLOSE_CURRENT_RESULT);
  }

  @Override
  public boolean getMoreResults(int current) throws SQLException {
    // clean up current result, if exists
    if (resultSet != null
        && (current == Statement.CLOSE_CURRENT_RESULT || current == Statement.CLOSE_ALL_RESULTS)) {
      resultSet.close();
    }
    resultSet = null;

    // verify if more results exist
    if (childResults == null || childResults.isEmpty()) {
      return false;
    }

    // fetch next result using the query id
    SFChildResult nextResult = childResults.remove(0);
    try {
      JsonNode result = StmtUtil.getQueryResultJSON(nextResult.getId(), session);
      Object sortProperty = session.getSessionPropertyByKey("sort");
      boolean sortResult = sortProperty != null && (Boolean) sortProperty;
      resultSet =
          SFResultSetFactory.getResultSet(result, this, sortResult, new ExecTimeTelemetryData());
      // override statement type so we can treat the result set like a result of
      // the original statement called (and not the result scan)
      resultSet.setStatementType(nextResult.getType());

      return nextResult.getType().isGenerateResultSet();
    } catch (SFException ex) {
      throw new SnowflakeSQLException(ex);
    }
  }

  @Override
  public SFBaseResultSet getResultSet() {
    return resultSet;
  }

  @Override
  public boolean hasChildren() {
    return !childResults.isEmpty();
  }

  @Override
  public SFBaseResultSet asyncExecute(
      String sql,
      Map<String, ParameterBindingDTO> parametersBinding,
      CallingMethod caller,
      ExecTimeTelemetryData execTimeData)
      throws SQLException, SFException {
    return execute(sql, true, parametersBinding, caller, execTimeData);
  }
}
