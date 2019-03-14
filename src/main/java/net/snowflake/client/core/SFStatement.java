/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import com.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.core.BasicEvent.QueryState;
import net.snowflake.client.core.bind.BindException;
import net.snowflake.client.core.bind.BindUploader;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeFileTransferAgent;
import net.snowflake.client.jdbc.SnowflakeReauthenticationRequest;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.telemetry.TelemetryData;
import net.snowflake.client.jdbc.telemetry.TelemetryUtil;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.client.util.SecretDetector;
import net.snowflake.common.core.SqlState;
import org.apache.http.client.methods.HttpRequestBase;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Snowflake statement
 */
public class SFStatement
{
  public enum CallingMethod
  {
    EXECUTE,
    EXECUTE_UPDATE,
    EXECUTE_QUERY
  }

  static final SFLogger logger = SFLoggerFactory.getLogger(SFStatement.class);

  private SFSession session;

  private SFBaseResultSet resultSet = null;

  private HttpRequestBase httpRequest;

  private Boolean isClosed = false;

  private Integer sequenceId = -1;

  private String requestId = null;

  private String sqlText = null;

  private final AtomicBoolean canceling = new AtomicBoolean(false);

  // timeout in seconds
  private int queryTimeout = 0;

  private boolean isFileTransfer = false;

  private SnowflakeFileTransferAgent transferAgent = null;

  // statement level parameters
  private final Map<String, Object> statementParametersMap = new HashMap<String, Object>();

  final private static int MAX_STATEMENT_PARAMETERS = 1000;

  /**
   * id used in combine describe and execute
   */
  private String describeJobUUID;

  // when uploading binds to stage, we use a table scan which cannot parse times from ms
  // so, if the user binds time values, we don't upload to stage
  private boolean hasUnsupportedStageBind = false;

  // list of child result objects for queries called by the current query, if any
  private List<SFChildResult> childResults = null;

  /**
   * Add a statement parameter
   * <p>
   * Make sure a property is not added more than once and the number of
   * properties does not exceed limit.
   *
   * @param propertyName  property name
   * @param propertyValue property value
   * @throws SFException if too many parameters for a statement
   */
  public void addProperty(String propertyName, Object propertyValue)
  throws SFException
  {
    statementParametersMap.put(propertyName, propertyValue);

    // for query timeout, we implement it on client side for now
    if ("query_timeout".equalsIgnoreCase(propertyName))
    {
      queryTimeout = (Integer) propertyValue;
    }

    // check if the number of session properties exceed limit
    if (statementParametersMap.size() > MAX_STATEMENT_PARAMETERS)
    {
      throw new SFException(
          ErrorCode.TOO_MANY_STATEMENT_PARAMETERS, MAX_STATEMENT_PARAMETERS);
    }
  }

  public SFStatement(SFSession session)
  {
    logger.debug(" public SFStatement(SFSession session)");

    this.session = session;
    Integer queryTimeout = session == null ? null : session.getQueryTimeout();
    this.queryTimeout = queryTimeout != null ? queryTimeout : this.queryTimeout;
  }

  /**
   * Sanity check query text
   *
   * @param sql The SQL statement to check
   * @throws SQLException
   */
  private void sanityCheckQuery(String sql) throws SQLException
  {
    if (sql == null || sql.isEmpty())
    {
      throw new SnowflakeSQLException(SqlState.SQL_STATEMENT_NOT_YET_COMPLETE,
                                      ErrorCode.INVALID_SQL.getMessageCode(), sql);

    }
  }

  /**
   * Execute SQL query with an option for describe only
   *
   * @param sql          sql statement
   * @param describeOnly true if describe only
   * @return query result set
   * @throws SQLException if connection is already closed
   * @throws SFException  if result set is null
   */
  private SFBaseResultSet executeQuery(
      String sql,
      Map<String, ParameterBindingDTO> parametersBinding,
      boolean describeOnly,
      CallingMethod caller)
  throws SQLException, SFException
  {
    sanityCheckQuery(sql);

    String trimmedSql = sql.trim();

    // snowflake specific client side commands
    if (isFileTransfer(trimmedSql))
    {
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
        caller
    );
  }

  /**
   * Describe a statement
   *
   * @param sql statement
   * @return metadata of statement including result set metadata and binding information
   * @throws SQLException if connection is already closed
   * @throws SFException  if result set is null
   */
  public SFStatementMetaData describe(String sql) throws SFException, SQLException
  {
    SFBaseResultSet baseResultSet = executeQuery(sql, null, true, null);

    describeJobUUID = baseResultSet.getQueryId();

    return new SFStatementMetaData(baseResultSet.getMetaData(),
                                   baseResultSet.getStatementType(),
                                   baseResultSet.getNumberOfBinds(),
                                   baseResultSet.isArrayBindSupported());
  }

  /**
   * Internal method for executing a query with bindings accepted.
   * <p>
   *
   * @param sql               sql statement
   * @param parameterBindings binding information
   * @param describeOnly      true if just showing result set metadata
   * @param internal          true if internal command not showing up in the history
   * @param caller            the JDBC method that called this function, null if none
   * @return snowflake query result set
   * @throws SQLException if connection is already closed
   * @throws SFException  if result set is null
   */
  SFBaseResultSet executeQueryInternal(
      String sql,
      Map<String, ParameterBindingDTO> parameterBindings,
      boolean describeOnly,
      boolean internal,
      CallingMethod caller)
  throws SQLException, SFException
  {
    resetState();

    if (logger.isDebugEnabled())
    {
      logger.debug("executeQuery: {}", SecretDetector.maskAWSSecret(sql));
    }

    if (session == null || session.isClosed())
    {
      throw new SQLException("connection is closed");
    }

    Object result = executeHelper(sql,
                                  StmtUtil.SF_MEDIA_TYPE,
                                  parameterBindings,
                                  describeOnly,
                                  internal);

    if (result == null)
    {
      throw new SnowflakeSQLException(SqlState.INTERNAL_ERROR,
                                      ErrorCode.INTERNAL_ERROR.getMessageCode(),
                                      "got null result");
    }

    /*
     * we sort the result if the connection is in sorting mode
     */
    Object sortProperty = session.getSFSessionProperty("sort");

    boolean sortResult = sortProperty != null && (Boolean) sortProperty;

    logger.debug("Creating result set");

    try
    {
      JsonNode jsonResult = (JsonNode) result;
      resultSet = new SFResultSet(jsonResult, this, sortResult);
      childResults = ResultUtil.getChildResults(session, requestId, jsonResult);

      // if child results are available, skip over this result set and set the
      // current result to the first child's result.
      // we still construct the first result set for its side effects.
      if (!childResults.isEmpty())
      {
        SFStatementType type = childResults.get(0).type;

        // ensure first query type matches the calling JDBC method, if exists
        if (caller == CallingMethod.EXECUTE_QUERY && !type.isGenerateResultSet())
        {
          throw new SnowflakeSQLException(
              ErrorCode.QUERY_FIRST_RESULT_NOT_RESULT_SET);
        }
        else if (caller == CallingMethod.EXECUTE_UPDATE && type.isGenerateResultSet())
        {
          throw new SnowflakeSQLException(
              ErrorCode.UPDATE_FIRST_RESULT_NOT_UPDATE_COUNT);
        }

        // this will update resultSet to point to the first child result before we return it
        getMoreResults();
      }
    }
    catch (SnowflakeSQLException | OutOfMemoryError ex)
    {
      // snow-24428: no need to generate incident for exceptions we generate
      // snow-29403: or client OOM
      throw ex;
    }
    catch (Throwable ex)
    {
      // SNOW-22813 log exception
      logger.error("Exception creating result", ex);

      throw IncidentUtil.generateIncidentWithException(
          session,
          null,
          null, ex,
          ErrorCode.INTERNAL_ERROR,
          "exception creating result");
    }
    logger.debug("Done creating result set");

    return resultSet;
  }

  /**
   * Set a time bomb to cancel the outstanding query when timeout is reached.
   *
   * @param executor object to execute statement cancel request
   */
  private void setTimeBomb(ScheduledExecutorService executor)
  {
    class TimeBombTask implements Callable<Void>
    {

      private final SFStatement statement;

      private TimeBombTask(SFStatement statement)
      {
        this.statement = statement;
      }

      @Override
      public Void call() throws SQLException
      {
        try
        {
          statement.cancel();
        }
        catch (SFException ex)
        {
          throw new SnowflakeSQLException(ex, ex.getSqlState(),
                                          ex.getVendorCode(), ex.getParams());
        }
        return null;
      }
    }

    executor.schedule(new TimeBombTask(this), this.queryTimeout,
                      TimeUnit.SECONDS);
  }

  /**
   * A helper method to build URL and submit the SQL to snowflake for exec
   *
   * @param sql          sql statement
   * @param mediaType    media type
   * @param bindValues   map of binding values
   * @param describeOnly whether only show the result set metadata
   * @param internal     run internal query not showing up in history
   * @return raw json response
   * @throws SFException           if query is canceled
   * @throws SnowflakeSQLException if query is already running
   */
  public Object executeHelper(String sql,
                              String mediaType,
                              Map<String, ParameterBindingDTO> bindValues,
                              boolean describeOnly,
                              boolean internal)
  throws SnowflakeSQLException, SFException
  {
    ScheduledExecutorService executor = null;

    try
    {
      synchronized (this)
      {
        if (isClosed)
        {
          throw new SFException(ErrorCode.STATEMENT_CLOSED);
        }

        // initialize a sequence id if not closed or not for aborting
        if (canceling.get())
        {
          // nothing to do if canceled
          throw new SFException(ErrorCode.QUERY_CANCELED);
        }

        if (this.requestId != null)
        {
          throw new SnowflakeSQLException(SqlState.FEATURE_NOT_SUPPORTED,
                                          ErrorCode.STATEMENT_ALREADY_RUNNING_QUERY.getMessageCode());
        }

        this.requestId = UUID.randomUUID().toString();
        this.sequenceId = session.getAndIncrementSequenceId();

        this.sqlText = sql;
      }

      EventUtil.triggerStateTransition(BasicEvent.QueryState.QUERY_STARTED,
                                       String.format(QueryState.QUERY_STARTED.getArgString(), requestId));

      // if there are a large number of bind values, we should upload them to stage
      // instead of passing them in the payload (if enabled)
      int numBinds = BindUploader.arrayBindValueCount(bindValues);
      String bindStagePath = null;
      if (0 < session.getArrayBindStageThreshold()
          && session.getArrayBindStageThreshold() <= numBinds
          && !describeOnly
          && !hasUnsupportedStageBind
          && BindUploader.isArrayBind(bindValues))
      {
        try (BindUploader uploader = BindUploader.newInstance(session, requestId))
        {
          uploader.upload(bindValues);
          bindStagePath = uploader.getStagePath();
        }
        catch (BindException ex)
        {
          logger.debug("Exception encountered trying to upload binds to stage. Attaching binds in payload instead. ", ex);
          TelemetryData errorLog = TelemetryUtil.buildJobData(this.requestId, ex.type.field, 1);
          this.session.getTelemetryClient().tryAddLogToBatch(errorLog);
          IncidentUtil.generateIncident(session, "Failed to upload binds to stage",
                                        null, requestId, null, ex);
        }
      }

      StmtUtil.StmtInput stmtInput = new StmtUtil.StmtInput();
      stmtInput.setSql(sql)
          .setMediaType(mediaType)
          .setInternal(internal)
          .setDescribeOnly(describeOnly)
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
          .setServiceName(session.getServiceName());

      if (bindStagePath != null)
      {
        stmtInput.setBindValues(null)
            .setBindStage(bindStagePath);
        // use the new SQL format for this query so dates/timestamps are parsed correctly
        setUseNewSqlFormat(true);
      }
      else
      {
        stmtInput.setBindValues(bindValues)
            .setBindStage(null);
      }

      if (canceling.get())
      {
        logger.debug("Query cancelled");

        throw new SFException(ErrorCode.QUERY_CANCELED);
      }

      // if timeout is set, start a thread to cancel the request after timeout
      // reached.
      if (this.queryTimeout > 0)
      {
        executor = Executors.newScheduledThreadPool(1);
        setTimeBomb(executor);
      }

      StmtUtil.StmtOutput stmtOutput = null;
      boolean sessionRenewed;

      do
      {
        sessionRenewed = false;
        try
        {
          stmtOutput = StmtUtil.execute(stmtInput);
          break;
        }
        catch (SnowflakeSQLException ex)
        {
          if (ex.getErrorCode() == Constants.SESSION_EXPIRED_GS_CODE)
          {
            try
            {
              // renew the session
              session.renewSession(stmtInput.sessionToken);
            }
            catch (SnowflakeReauthenticationRequest ex0)
            {
              if (session.isExternalbrowserAuthenticator())
              {
                reauthenticate();
              }
              else
              {
                throw ex0;
              }
            }
            // SNOW-18822: reset session token for the statement
            stmtInput.setSessionToken(session.getSessionToken());
            stmtInput.setRetry(true);

            sessionRenewed = true;

            logger.debug("Session got renewed, will retry");
          }
          else
          {
            throw ex;
          }
        }
      }
      while (sessionRenewed && !canceling.get());

      // Debugging/Testing for incidents
      if (System.getProperty("snowflake.enable_incident_test1") != null &&
          System.getProperty("snowflake.enable_incident_test1").equals("true"))
      {
        SFException sfe =
            IncidentUtil.generateIncidentWithException(session, this.requestId,
                                                       null, ErrorCode.STATEMENT_CLOSED);

        throw sfe;
      }

      synchronized (this)
      {
        /*
         * done with the remote execution of the query. set sequenceId to -1
         * and request id to null so that we don't try to abort it upon canceling.
         */
        this.sequenceId = -1;
        this.requestId = null;
      }

      if (canceling.get())
      {
        // If we are here, this is the context for the initial query that
        // is being canceled. Raise an exception anyway here even if
        // the server fails to abort it.
        throw new SFException(ErrorCode.QUERY_CANCELED);
      }

      logger.debug("Returning from executeHelper");

      if (stmtOutput != null)
      {
        return stmtOutput.getResult();
      }
      throw new SFException(ErrorCode.INTERNAL_ERROR);
    }
    catch (SFException | SnowflakeSQLException ex)
    {
      isClosed = true;
      throw ex;
    }
    finally
    {
      if (executor != null)
      {
        executor.shutdownNow();
      }
      // if this query enabled the new SQL format, re-disable it now
      setUseNewSqlFormat(false);
    }
  }

  private void reauthenticate() throws SFException, SnowflakeSQLException
  {
    SessionUtil.LoginInput input = new SessionUtil.LoginInput();
    SessionUtil.LoginOutput output = new SessionUtil.LoginOutput();
    output.setSessionToken(session.getSessionToken());
    input.setRole(session.getRole());
    input.setWarehouse(session.getWarehouse());
    input.setDatabaseName(session.getDatabase());
    input.setSchemaName(session.getSchema());

    session.open();
    session.setCurrentObjects(input, output);
    // output is not used here.
  }

  /**
   * A helper method to build URL and cancel the SQL for exec
   *
   * @param sql       sql statement
   * @param mediaType media type
   * @throws SnowflakeSQLException if failed to cancel the statement
   * @throws SFException           if statement is already closed
   */
  private void cancelHelper(String sql, String mediaType)
  throws SnowflakeSQLException, SFException
  {
    synchronized (this)
    {
      if (isClosed)
      {
        throw new SFException(ErrorCode.INTERNAL_ERROR,
                              "statement already closed");
      }
    }

    StmtUtil.StmtInput stmtInput = new StmtUtil.StmtInput();
    stmtInput.setServerUrl(session.getServerUrl())
        .setSql(sql)
        .setMediaType(mediaType)
        .setRequestId(requestId)
        .setSessionToken(session.getSessionToken())
        .setServiceName(session.getServiceName());

    StmtUtil.cancel(stmtInput);

    synchronized (this)
    {
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
   * A method to check if a sql is file upload statement with consideration for
   * potential comments in front of put keyword.
   * <p>
   *
   * @param sql sql statement
   * @return true if the command is upload statement
   */
  private boolean isFileTransfer(String sql)
  {
    SFStatementType statementType = StmtUtil.checkStageManageCommand(sql);
    return statementType == SFStatementType.PUT ||
           statementType == SFStatementType.GET;
  }

  /**
   * Execute sql
   *
   * @param sql               sql statement.
   * @param parametersBinding parameters to bind
   * @param caller            the JDBC interface method that called this method, if any
   * @return whether there is result set or not
   * @throws SQLException if failed to execute sql
   * @throws SFException  exception raised from Snowflake components
   * @throws SQLException if SQL error occurs
   */
  public SFBaseResultSet execute(String sql,
                                 Map<String, ParameterBindingDTO>
                                     parametersBinding,
                                 CallingMethod caller)
  throws SQLException, SFException
  {
    sanityCheckQuery(sql);

    session.injectedDelay();

    if (logger.isDebugEnabled())
    {
      logger.debug("execute: {}", SecretDetector.maskAWSSecret(sql));
    }

    String trimmedSql = sql.trim();

    if (trimmedSql.length() >= 20
        && trimmedSql.toLowerCase().startsWith(
        "set-sf-property"))
    {
      executeSetProperty(sql);
      return null;
    }
    return executeQuery(sql, parametersBinding, false, caller);
  }

  private SFBaseResultSet executeFileTransfer(String sql) throws SQLException,
                                                                 SFException
  {
    session.injectedDelay();

    resetState();

    logger.debug("Entering executeFileTransfer");

    isFileTransfer = true;
    transferAgent = new SnowflakeFileTransferAgent(sql, session, this);

    try
    {
      transferAgent.execute();

      logger.debug("setting result set");

      resultSet = (SFFixedViewResultSet) transferAgent.getResultSet();
      childResults = Collections.emptyList();

      logger.debug("Number of cols: {}",
                   resultSet.getMetaData().getColumnCount());
      logger.debug("Completed transferring data");
      return resultSet;
    }
    catch (SQLException ex)
    {
      logger.debug("Exception: {}", ex.getMessage());
      throw ex;
    }
  }

  public void close() throws SQLException
  {
    logger.debug("public void close()");

    if (requestId != null)
    {
      EventUtil.triggerStateTransition(
          BasicEvent.QueryState.QUERY_ENDED,
          String.format(QueryState.QUERY_ENDED.getArgString(), requestId));
    }

    resultSet = null;
    childResults = null;
    isClosed = true;

    if (httpRequest != null)
    {
      logger.debug("releasing connection for the http request");

      httpRequest.releaseConnection();
      httpRequest = null;
    }

    try
    {
      session.getTelemetryClient().sendBatch();
    }
    catch (IOException ex)
    {
      logger.debug("Telemetry client failed to send batch metrics.");
    }

    isFileTransfer = false;
    transferAgent = null;
  }

  public void cancel() throws SFException, SQLException
  {
    logger.debug("public void cancel()");

    if (canceling.get())
    {
      logger.debug("Query is already cancelled");
      return;
    }

    canceling.set(true);

    if (isFileTransfer)
    {
      if (transferAgent != null)
      {
        logger.debug("Cancel file transferring ... ");
        transferAgent.cancel();
      }
    }
    else
    {
      synchronized (this)
      {
        // the query hasn't been sent to GS yet, just mark the stmt closed
        if (requestId == null)
        {
          logger.debug("No remote query outstanding");

          return;
        }
      }

      // cancel the query on the server side if it has been issued
      cancelHelper(this.sqlText, StmtUtil.SF_MEDIA_TYPE);
    }
  }

  private void resetState()
  {
    resultSet = null;
    childResults = null;

    if (httpRequest != null)
    {
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

  public void executeSetProperty(final String sql)
  {
    logger.debug("setting property");

    // tokenize the sql
    String[] tokens = sql.split("\\s+");

    if (tokens.length < 2)
    {
      return;
    }

    if ("sort".equalsIgnoreCase(tokens[1]))
    {
      if (tokens.length >= 3 && "on".equalsIgnoreCase(tokens[2]))
      {
        logger.debug("setting sort on");

        this.session.setSFSessionProperty("sort", true);
      }
      else
      {
        logger.debug("setting sort off");
        this.session.setSFSessionProperty("sort", false);
      }
    }
  }

  protected SFSession getSession()
  {
    return session;
  }

  public void setHasUnsupportedStageBind(boolean hasUnsupportedStageBind)
  {
    this.hasUnsupportedStageBind = hasUnsupportedStageBind;
  }

  // *NOTE* this new SQL format is incomplete. It should only be used under certain circumstances.
  private void setUseNewSqlFormat(boolean useNewSqlFormat) throws SFException
  {
    this.addProperty("NEW_SQL_FORMAT", useNewSqlFormat);
  }

  public boolean getMoreResults() throws SQLException
  {
    return getMoreResults(Statement.CLOSE_CURRENT_RESULT);
  }

  /**
   * Sets the result set to the next one, if available.
   *
   * @param current What to do with the current result.
   *                One of Statement.CLOSE_CURRENT_RESULT,
   *                Statement.CLOSE_ALL_RESULTS, or
   *                Statement.KEEP_CURRENT_RESULT
   * @return true if there is a next result and it's a result set
   * false if there are no more results, or there is a next result
   * and it's an update count
   * @throws SQLException if something fails while getting the next result
   */
  public boolean getMoreResults(int current) throws SQLException
  {
    // clean up current result, if exists
    if (resultSet != null &&
        (current == Statement.CLOSE_CURRENT_RESULT ||
         current == Statement.CLOSE_ALL_RESULTS))
    {
      resultSet.close();
    }
    resultSet = null;

    // verify if more results exist
    if (childResults == null || childResults.isEmpty())
    {
      return false;
    }

    // fetch next result using the query id
    SFChildResult nextResult = childResults.remove(0);
    try
    {
      JsonNode result = StmtUtil.getQueryResultJSON(
          nextResult.getId(), session);
      Object sortProperty = session.getSFSessionProperty("sort");
      boolean sortResult = sortProperty != null && (Boolean) sortProperty;
      resultSet = new SFResultSet(result, this, sortResult);
      // override statement type so we can treat the result set like a result of
      // the original statement called (and not the result scan)
      resultSet.setStatementType(nextResult.getType());

      return nextResult.getType().isGenerateResultSet();
    }
    catch (SFException ex)
    {
      throw new SnowflakeSQLException(ex);
    }
  }

  public SFBaseResultSet getResultSet()
  {
    return resultSet;
  }

  public boolean hasChildren()
  {
    return !childResults.isEmpty();
  }
}
