package net.snowflake.client.core;

import static net.snowflake.client.jdbc.SnowflakeUtil.isNullOrEmpty;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.GZIPOutputStream;
import net.snowflake.client.core.BasicEvent.QueryState;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.SnowflakeUtil;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.client.util.SecretDetector;
import net.snowflake.common.api.QueryInProgressResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.StringEntity;

/** Statement Util */
public class StmtUtil {
  static final EventHandler eventHandler = EventUtil.getEventHandlerInstance();

  static final ObjectMapper mapper = ObjectMapperFactory.getObjectMapper();

  static final String SF_PATH_QUERY_V1 = "/queries/v1/query-request";

  private static final String SF_PATH_ABORT_REQUEST_V1 = "/queries/v1/abort-request";

  private static final String SF_PATH_QUERY_RESULT = "/queries/%s/result";

  private static final String SF_QUERY_COMBINE_DESCRIBE_EXECUTE = "combinedDescribe";

  static final String SF_MEDIA_TYPE = "application/snowflake";

  // we don't want to retry canceling forever so put a limit which is
  // twice as much as our default socket timeout
  static final int SF_CANCELING_RETRY_TIMEOUT_IN_MILLIS = 600000; // 10 min

  private static final SFLogger logger = SFLoggerFactory.getLogger(StmtUtil.class);

  /** Input for executing a statement on server */
  static class StmtInput {
    String sql;

    // default to snowflake (a special json format for snowflake query result
    String mediaType = SF_MEDIA_TYPE;
    Map<String, ParameterBindingDTO> bindValues;
    String bindStage;
    boolean describeOnly;
    String serverUrl;
    String requestId;
    int sequenceId = -1;
    boolean internal = false;
    boolean asyncExec = false;

    Map<String, Object> parametersMap;
    String sessionToken;
    int networkTimeoutInMillis;
    int socketTimeout;
    int injectSocketTimeout; // seconds
    int injectClientPause; // seconds

    int maxRetries;

    AtomicBoolean canceling = null; // canceling flag
    boolean retry;
    String prevGetResultURL = null; // previous get result URL from ping pong

    boolean combineDescribe = false;

    String describedJobId;

    long querySubmissionTime; // millis since epoch

    String serviceName;

    OCSPMode ocspMode;

    HttpClientSettingsKey httpClientSettingsKey;

    QueryContextDTO queryContextDTO;

    Map<String, String> additionalHttpHeadersForSnowsight;

    StmtInput() {}

    public StmtInput setSql(String sql) {
      this.sql = sql;
      return this;
    }

    public StmtInput setMediaType(String mediaType) {
      this.mediaType = mediaType;
      return this;
    }

    public StmtInput setParametersMap(Map<String, Object> parametersMap) {
      this.parametersMap = parametersMap;
      return this;
    }

    public StmtInput setBindValues(Map<String, ParameterBindingDTO> bindValues) {
      this.bindValues = bindValues;
      return this;
    }

    public StmtInput setBindStage(String bindStage) {
      this.bindStage = bindStage;
      return this;
    }

    public StmtInput setDescribeOnly(boolean describeOnly) {
      this.describeOnly = describeOnly;
      return this;
    }

    public StmtInput setInternal(boolean internal) {
      this.internal = internal;
      return this;
    }

    public StmtInput setServerUrl(String serverUrl) {
      this.serverUrl = serverUrl;
      return this;
    }

    public StmtInput setRequestId(String requestId) {
      this.requestId = requestId;
      return this;
    }

    public StmtInput setSequenceId(int sequenceId) {
      this.sequenceId = sequenceId;
      return this;
    }

    public StmtInput setSessionToken(String sessionToken) {
      this.sessionToken = sessionToken;
      return this;
    }

    public StmtInput setNetworkTimeoutInMillis(int networkTimeoutInMillis) {
      this.networkTimeoutInMillis = networkTimeoutInMillis;
      return this;
    }

    public StmtInput setSocketTimeout(int socketTimeout) {
      this.socketTimeout = socketTimeout;
      return this;
    }

    public StmtInput setInjectSocketTimeout(int injectSocketTimeout) {
      this.injectSocketTimeout = injectSocketTimeout;
      return this;
    }

    public StmtInput setInjectClientPause(int injectClientPause) {
      this.injectClientPause = injectClientPause;
      return this;
    }

    public StmtInput setCanceling(AtomicBoolean canceling) {
      this.canceling = canceling;
      return this;
    }

    public StmtInput setRetry(boolean retry) {
      this.retry = retry;
      return this;
    }

    public StmtInput setCombineDescribe(boolean combineDescribe) {
      this.combineDescribe = combineDescribe;
      return this;
    }

    public StmtInput setDescribedJobId(String describedJobId) {
      this.describedJobId = describedJobId;
      return this;
    }

    public StmtInput setQuerySubmissionTime(long querySubmissionTime) {
      this.querySubmissionTime = querySubmissionTime;
      return this;
    }

    public StmtInput setServiceName(String serviceName) {
      this.serviceName = serviceName;
      return this;
    }

    public StmtInput setOCSPMode(OCSPMode ocspMode) {
      this.ocspMode = ocspMode;
      return this;
    }

    public StmtInput setHttpClientSettingsKey(HttpClientSettingsKey key) {
      this.httpClientSettingsKey = key;
      return this;
    }

    public StmtInput setAsync(boolean async) {
      this.asyncExec = async;
      return this;
    }

    public StmtInput setQueryContextDTO(QueryContextDTO queryContext) {
      this.queryContextDTO = queryContext;
      return this;
    }

    public StmtInput setMaxRetries(int maxRetries) {
      this.maxRetries = maxRetries;
      return this;
    }

    /**
     * Set additional http headers to apply to the outgoing request. The additional headers cannot
     * be used to replace or overwrite a header in use by the driver. These will be applied to the
     * outgoing request. Primarily used by Snowsight, as described in {@link
     * HttpUtil#applyAdditionalHeadersForSnowsight(org.apache.http.client.methods.HttpRequestBase,
     * Map)}
     *
     * @param additionalHttpHeaders The new headers to add
     * @return The input object, for chaining
     * @see
     *     HttpUtil#applyAdditionalHeadersForSnowsight(org.apache.http.client.methods.HttpRequestBase,
     *     Map)
     */
    @SuppressWarnings("unchecked")
    public StmtInput setAdditionalHttpHeadersForSnowsight(
        Map<String, String> additionalHttpHeaders) {
      this.additionalHttpHeadersForSnowsight = additionalHttpHeaders;
      return this;
    }
  }

  /** Output for running a statement on server */
  public static class StmtOutput {
    JsonNode result;

    public StmtOutput(JsonNode result) {
      this.result = result;
    }

    public JsonNode getResult() {
      return result;
    }
  }

  /**
   * Execute a statement
   *
   * <p>side effect: stmtInput.prevGetResultURL is set if we have started ping pong and receives an
   * exception from session token expiration so that during retry we don't retry the query
   * submission, but continue the ping pong process.
   *
   * @param stmtInput input statement
   * @param execTimeData ExecTimeTelemetryData
   * @return StmtOutput output statement
   * @throws SFException exception raised from Snowflake components
   * @throws SnowflakeSQLException exception raised from Snowflake components
   */
  public static StmtOutput execute(StmtInput stmtInput, ExecTimeTelemetryData execTimeData)
      throws SFException, SnowflakeSQLException {
    HttpPost httpRequest = null;

    AssertUtil.assertTrue(
        stmtInput.serverUrl != null, "Missing server url for statement execution");

    AssertUtil.assertTrue(stmtInput.sql != null, "Missing sql for statement execution");

    AssertUtil.assertTrue(
        stmtInput.requestId != null, "Missing request id for statement execution");

    AssertUtil.assertTrue(
        stmtInput.sequenceId >= 0, "Negative sequence id for statement execution");

    AssertUtil.assertTrue(
        stmtInput.mediaType != null, "Missing media type for statement execution");

    try {
      String resultAsString = null;

      // SNOW-20443: if we are retrying and there is get result URL, we
      // don't need to execute the query again
      if (stmtInput.retry && stmtInput.prevGetResultURL != null) {
        logger.debug(
            "Retrying statement execution with get result URL: {}", stmtInput.prevGetResultURL);
      } else {
        URIBuilder uriBuilder = new URIBuilder(stmtInput.serverUrl);

        uriBuilder.setPath(SF_PATH_QUERY_V1);
        uriBuilder.addParameter(SFSession.SF_QUERY_REQUEST_ID, stmtInput.requestId);

        if (stmtInput.combineDescribe) {
          uriBuilder.addParameter(SF_QUERY_COMBINE_DESCRIBE_EXECUTE, Boolean.TRUE.toString());
        }

        httpRequest = new HttpPost(uriBuilder.build());

        // Add custom headers before adding common headers
        HttpUtil.applyAdditionalHeadersForSnowsight(
            httpRequest, stmtInput.additionalHttpHeadersForSnowsight);

        /*
         * sequence id is only needed for old query API, when old query API
         * is deprecated, we can remove sequence id.
         */
        QueryExecDTO sqlJsonBody =
            new QueryExecDTO(
                stmtInput.sql,
                stmtInput.describeOnly,
                stmtInput.sequenceId,
                stmtInput.bindValues,
                stmtInput.bindStage,
                stmtInput.parametersMap,
                stmtInput.queryContextDTO,
                stmtInput.querySubmissionTime,
                stmtInput.describeOnly || stmtInput.internal,
                stmtInput.asyncExec);

        if (!stmtInput.describeOnly) {
          sqlJsonBody.setDescribedJobId(stmtInput.describedJobId);
        }

        String queryContextDTO = mapper.writeValueAsString(stmtInput.queryContextDTO);

        logger.debug("queryContextDTO: {}", queryContextDTO);

        String json = mapper.writeValueAsString(sqlJsonBody);

        logger.debug("JSON: {}", json);

        ByteArrayEntity input;
        if (!stmtInput.httpClientSettingsKey.getGzipDisabled()) {
          execTimeData.setGzipStart();
          // SNOW-18057: compress the post body in gzip
          ByteArrayOutputStream baos = new ByteArrayOutputStream();
          GZIPOutputStream gzos = new GZIPOutputStream(baos);
          byte[] bytes = json.getBytes("UTF-8");
          gzos.write(bytes);
          gzos.finish();
          input = new ByteArrayEntity(baos.toByteArray());
          httpRequest.addHeader("content-encoding", "gzip");
          execTimeData.setGzipEnd();
        } else {
          input = new ByteArrayEntity(json.getBytes("UTF-8"));
        }
        input.setContentType("application/json");
        httpRequest.setEntity(input);
        httpRequest.addHeader("accept", stmtInput.mediaType);

        httpRequest.setHeader(
            SFSession.SF_HEADER_AUTHORIZATION,
            SFSession.SF_HEADER_SNOWFLAKE_AUTHTYPE
                + " "
                + SFSession.SF_HEADER_TOKEN_TAG
                + "=\""
                + stmtInput.sessionToken
                + "\"");

        setServiceNameHeader(stmtInput, httpRequest);
        eventHandler.triggerStateTransition(
            BasicEvent.QueryState.SENDING_QUERY,
            String.format(QueryState.SENDING_QUERY.getArgString(), stmtInput.requestId));

        resultAsString =
            HttpUtil.executeRequest(
                httpRequest,
                stmtInput.networkTimeoutInMillis / 1000,
                stmtInput.socketTimeout,
                0,
                stmtInput.maxRetries,
                stmtInput.injectSocketTimeout,
                stmtInput.canceling,
                true, // include retry parameters
                false, // no retry on HTTP 403
                stmtInput.httpClientSettingsKey,
                execTimeData);
      }

      return pollForOutput(resultAsString, stmtInput, httpRequest, execTimeData);
    } catch (Exception ex) {
      if (!(ex instanceof SnowflakeSQLException)) {
        if (ex instanceof IOException) {
          logger.error("IOException encountered", ex);

          // network error
          throw new SFException(
              ex,
              ErrorCode.NETWORK_ERROR,
              "Exception encountered when executing statement: " + ex.getLocalizedMessage());
        } else {
          logger.error("Exception encountered", ex);

          // raise internal exception if this is not a snowflake exception
          throw new SFException(ex, ErrorCode.INTERNAL_ERROR, ex.getLocalizedMessage());
        }
      } else {
        throw (SnowflakeSQLException) ex;
      }
    } finally {
      // we can release the http connection now
      if (httpRequest != null) {
        httpRequest.releaseConnection();
      }
    }
  }

  private static void setServiceNameHeader(StmtInput stmtInput, HttpRequestBase httpRequest) {
    if (!isNullOrEmpty(stmtInput.serviceName)) {
      httpRequest.setHeader(SessionUtil.SF_HEADER_SERVICE_NAME, stmtInput.serviceName);
    }
  }

  private static StmtOutput pollForOutput(
      String resultAsString,
      StmtInput stmtInput,
      HttpPost httpRequest,
      ExecTimeTelemetryData execTimeData)
      throws SFException, SnowflakeSQLException {
    /*
     * Check response for error or for ping pong response
     *
     * For ping-pong: want to make sure our connection is not silently dropped
     * by middle players (e.g load balancer/VPN timeout) between client and GS
     */
    JsonNode pingPongResponseJson;
    boolean queryInProgress;
    boolean firstResponse = !stmtInput.retry;
    String previousGetResultPath = (stmtInput.retry ? stmtInput.prevGetResultURL : null);
    int retries = 0;
    final int MAX_RETRIES = 3;

    do {
      pingPongResponseJson = null;

      if (resultAsString != null) {
        try {
          pingPongResponseJson = mapper.readTree(resultAsString);
        } catch (Exception ex) {
          logger.error(
              "Bad result json: {}, " + "JSON parsing exception: {}, http request: {}",
              resultAsString,
              ex.getLocalizedMessage(),
              httpRequest);

          logger.error("Exception stack trace", ex);
        }
      }

      eventHandler.triggerStateTransition(
          BasicEvent.QueryState.WAITING_FOR_RESULT,
          "{requestId: " + stmtInput.requestId + "," + "pingNumber: " + retries + "}");

      if (pingPongResponseJson == null) {
        /*
         * Retry for bad response for server.
         * But we don't want to retry too many times
         */
        if (retries >= MAX_RETRIES) {
          throw new SFException(ErrorCode.BAD_RESPONSE, resultAsString);
        } else {
          logger.debug("Will retry get result. Retry count: {}", retries);
          execTimeData.incrementRetryCount();
          execTimeData.addRetryLocation("StmtUtil null response");
          retries++;
        }
      } else {
        retries = 0; // reset retry counter after a successful response
      }

      if (pingPongResponseJson != null)
      // raise server side error as an exception if any
      {
        SnowflakeUtil.checkErrorAndThrowException(pingPongResponseJson);
      }

      // check the response code to see if it is a progress report response
      if (pingPongResponseJson != null
          && !QueryInProgressResponse.QUERY_IN_PROGRESS_CODE.equals(
              pingPongResponseJson.path("code").asText())
          && !QueryInProgressResponse.QUERY_IN_PROGRESS_ASYNC_CODE.equals(
              pingPongResponseJson.path("code").asText())) {
        queryInProgress = false;
      }
      // for the purposes of this function, return false instead of true
      else if (stmtInput.asyncExec
          && QueryInProgressResponse.QUERY_IN_PROGRESS_ASYNC_CODE.equals(
              pingPongResponseJson.path("code").asText())) {
        queryInProgress = false;
      } else {
        queryInProgress = true;

        if (firstResponse) {
          // sleep some time to simulate client pause. The purpose is to
          // simulate client pause before trying to fetch result so that
          // we can test query behavior related to disconnected client
          if (stmtInput.injectClientPause != 0) {
            logger.debug("Inject client pause for {} seconds", stmtInput.injectClientPause);
            try {
              Thread.sleep(stmtInput.injectClientPause * 1000);
            } catch (InterruptedException ex) {
              logger.debug("Exception encountered while injecting pause", false);
            }
          }
        }
        execTimeData.incrementRetryCount();
        execTimeData.addRetryLocation("StmtUtil queryInProgress");
        resultAsString = getQueryResult(pingPongResponseJson, previousGetResultPath, stmtInput);

        // save the previous get result path in case we run into session
        // expiration
        if (pingPongResponseJson != null) {
          previousGetResultPath = pingPongResponseJson.path("data").path("getResultUrl").asText();
          stmtInput.prevGetResultURL = previousGetResultPath;
        }
      }

      // not first response any more
      if (firstResponse) {
        firstResponse = false;
      }
    } while (queryInProgress);

    logger.debug("Returning result", false);

    eventHandler.triggerStateTransition(
        BasicEvent.QueryState.PROCESSING_RESULT,
        String.format(QueryState.PROCESSING_RESULT.getArgString(), stmtInput.requestId));

    return new StmtOutput(pingPongResponseJson);
  }

  /**
   * Issue get-result call to get query result given an in-progress response.
   *
   * <p>
   *
   * @param inProgressResponse In progress response in JSON form
   * @param previousGetResultPath previous get results path
   * @param stmtInput input statement
   * @return results in string form
   * @throws SFException exception raised from Snowflake components
   * @throws SnowflakeSQLException exception raised from Snowflake components
   */
  protected static String getQueryResult(
      JsonNode inProgressResponse, String previousGetResultPath, StmtInput stmtInput)
      throws SFException, SnowflakeSQLException {
    String getResultPath = null;

    // get result url better not be empty
    if (inProgressResponse == null
        || inProgressResponse.path("data").path("getResultUrl").isMissingNode()) {
      if (previousGetResultPath == null) {
        throw new SFException(
            ErrorCode.INTERNAL_ERROR, "No query response or missing get result URL");
      } else {
        logger.debug(
            "No query response or missing get result URL, " + "use previous get result URL: {}",
            previousGetResultPath);
        getResultPath = previousGetResultPath;
      }
    } else {
      getResultPath = inProgressResponse.path("data").path("getResultUrl").asText();
    }
    return getQueryResult(getResultPath, stmtInput);
  }

  /**
   * Issue get-result call to get query result given an in-progress response.
   *
   * @param getResultPath path to results
   * @param stmtInput object with context information
   * @return results in string form
   * @throws SFException exception raised from Snowflake components
   * @throws SnowflakeSQLException exception raised from Snowflake components
   */
  protected static String getQueryResult(String getResultPath, StmtInput stmtInput)
      throws SFException, SnowflakeSQLException {
    HttpGet httpRequest = null;
    logger.debug("Get query result: {}", getResultPath);

    try {
      URIBuilder uriBuilder = new URIBuilder(stmtInput.serverUrl);

      uriBuilder.setPath(getResultPath);

      uriBuilder.addParameter(SFSession.SF_QUERY_REQUEST_ID, UUIDUtils.getUUID().toString());

      httpRequest = new HttpGet(uriBuilder.build());
      // Add custom headers before adding common headers
      HttpUtil.applyAdditionalHeadersForSnowsight(
          httpRequest, stmtInput.additionalHttpHeadersForSnowsight);

      httpRequest.addHeader("accept", stmtInput.mediaType);

      httpRequest.setHeader(
          SFSession.SF_HEADER_AUTHORIZATION,
          SFSession.SF_HEADER_SNOWFLAKE_AUTHTYPE
              + " "
              + SFSession.SF_HEADER_TOKEN_TAG
              + "=\""
              + stmtInput.sessionToken
              + "\"");

      setServiceNameHeader(stmtInput, httpRequest);

      return HttpUtil.executeRequest(
          httpRequest,
          stmtInput.networkTimeoutInMillis / 1000,
          stmtInput.socketTimeout,
          0,
          stmtInput.maxRetries,
          0,
          stmtInput.canceling,
          false, // no retry parameter
          false, // no retry on HTTP 403
          stmtInput.httpClientSettingsKey,
          new ExecTimeTelemetryData());
    } catch (URISyntaxException | IOException ex) {
      logger.error("Exception encountered when getting result for " + httpRequest, ex);

      // raise internal exception if this is not a snowflake exception
      throw new SFException(ex, ErrorCode.INTERNAL_ERROR, ex.getLocalizedMessage());
    }
  }

  /**
   * Issue get-result call to get query result given an in progress response.
   *
   * @param queryId id of query to get results for
   * @param session the current session
   * @return results in JSON
   * @throws SFException exception raised from Snowflake components
   * @throws SnowflakeSQLException exception raised from Snowflake components
   */
  protected static JsonNode getQueryResultJSON(String queryId, SFSession session)
      throws SFException, SnowflakeSQLException {
    String getResultPath = String.format(SF_PATH_QUERY_RESULT, queryId);
    StmtInput stmtInput =
        new StmtInput()
            .setServerUrl(session.getServerUrl())
            .setSessionToken(session.getSessionToken())
            .setNetworkTimeoutInMillis(session.getNetworkTimeoutInMilli())
            .setSocketTimeout(session.getHttpClientSocketTimeout())
            .setMediaType(SF_MEDIA_TYPE)
            .setServiceName(session.getServiceName())
            .setOCSPMode(session.getOCSPMode())
            .setHttpClientSettingsKey(session.getHttpClientKey())
            .setMaxRetries(session.getMaxHttpRetries());

    String resultAsString = getQueryResult(getResultPath, stmtInput);

    StmtOutput stmtOutput =
        pollForOutput(resultAsString, stmtInput, null, new ExecTimeTelemetryData());
    return stmtOutput.getResult();
  }

  /**
   * Cancel a statement identifiable by a request id
   *
   * @param stmtInput input statement
   * @throws SFException if there is an internal exception
   * @throws SnowflakeSQLException if failed to cancel the statement
   * @deprecated use {@link #cancel(StmtInput, CancellationReason)} instead
   */
  @Deprecated
  public static void cancel(StmtInput stmtInput) throws SFException, SnowflakeSQLException {
    cancel(stmtInput, CancellationReason.UNKNOWN);
  }

  /**
   * Cancel a statement identifiable by a request id
   *
   * @param stmtInput input statement
   * @param cancellationReason reason for the cancellation
   * @throws SFException if there is an internal exception
   * @throws SnowflakeSQLException if failed to cancel the statement
   */
  public static void cancel(StmtInput stmtInput, CancellationReason cancellationReason)
      throws SFException, SnowflakeSQLException {
    HttpPost httpRequest = null;

    AssertUtil.assertTrue(
        stmtInput.serverUrl != null, "Missing server url for statement execution");

    AssertUtil.assertTrue(stmtInput.sql != null, "Missing sql for statement execution");

    AssertUtil.assertTrue(
        stmtInput.mediaType != null, "Missing media type for statement execution");

    AssertUtil.assertTrue(
        stmtInput.requestId != null, "Missing request id for statement execution");

    AssertUtil.assertTrue(
        stmtInput.sessionToken != null, "Missing session token for statement execution");

    try {
      URIBuilder uriBuilder = new URIBuilder(stmtInput.serverUrl);
      logger.warn("Cancelling query {} with reason {}", stmtInput.requestId, cancellationReason);
      logger.debug("Aborting query: {}", stmtInput.sql);

      uriBuilder.setPath(SF_PATH_ABORT_REQUEST_V1);

      uriBuilder.addParameter(SFSession.SF_QUERY_REQUEST_ID, UUIDUtils.getUUID().toString());

      httpRequest = new HttpPost(uriBuilder.build());
      // Add custom headers before adding common headers
      HttpUtil.applyAdditionalHeadersForSnowsight(
          httpRequest, stmtInput.additionalHttpHeadersForSnowsight);

      /*
       * The JSON input has two fields: sqlText and requestId
       */
      Map<String, Object> sqlJsonBody = new HashMap<String, Object>();
      sqlJsonBody.put("sqlText", stmtInput.sql);
      sqlJsonBody.put("requestId", stmtInput.requestId);

      String json = mapper.writeValueAsString(sqlJsonBody);

      logger.debug("JSON for cancel request: {}", json);

      StringEntity input = new StringEntity(json, StandardCharsets.UTF_8);
      input.setContentType("application/json");
      httpRequest.setEntity(input);

      httpRequest.addHeader("accept", stmtInput.mediaType);

      httpRequest.setHeader(
          SFSession.SF_HEADER_AUTHORIZATION,
          SFSession.SF_HEADER_SNOWFLAKE_AUTHTYPE
              + " "
              + SFSession.SF_HEADER_TOKEN_TAG
              + "=\""
              + stmtInput.sessionToken
              + "\"");

      setServiceNameHeader(stmtInput, httpRequest);

      String jsonString =
          HttpUtil.executeRequest(
              httpRequest,
              SF_CANCELING_RETRY_TIMEOUT_IN_MILLIS,
              0,
              stmtInput.socketTimeout,
              0,
              0,
              null,
              false, // no retry parameter
              false, // no retry on HTTP 403
              stmtInput.httpClientSettingsKey,
              new ExecTimeTelemetryData());

      // trace the response if requested
      logger.debug("Json response: {}", jsonString);

      JsonNode rootNode = null;
      rootNode = mapper.readTree(jsonString);

      // raise server side error as an exception if any
      SnowflakeUtil.checkErrorAndThrowException(rootNode);
    } catch (URISyntaxException | IOException ex) {
      logger.error("Exception encountered when canceling " + httpRequest, ex);

      // raise internal exception if this is not a snowflake exception
      throw new SFException(ex, ErrorCode.INTERNAL_ERROR, ex.getLocalizedMessage());
    }
  }

  /**
   * A simple function to check if the statement is related to manipulate stage.
   *
   * @param sql a SQL statement/command
   * @return PUT/GET/LIST/RM if statement belongs to one of them, otherwise return NULL
   */
  public static SFStatementType checkStageManageCommand(String sql) {
    if (sql == null) {
      return null;
    }

    String trimmedSql = sql.trim();

    // skip commenting prefixed with //
    while (trimmedSql.startsWith("//")) {
      if (logger.isDebugEnabled()) {
        logger.debug("Skipping // comments in: \n{}", trimmedSql);
      }

      if (trimmedSql.indexOf('\n') > 0) {
        trimmedSql = trimmedSql.substring(trimmedSql.indexOf('\n'));
        trimmedSql = trimmedSql.trim();
      } else {
        break;
      }

      if (logger.isDebugEnabled()) {
        logger.debug("New sql after skipping // comments: \n{}", trimmedSql);
      }
    }

    // skip commenting enclosed with /* */
    while (trimmedSql.startsWith("/*")) {
      if (logger.isDebugEnabled()) {
        logger.debug("skipping /* */ comments in: \n{}", trimmedSql);
      }

      if (trimmedSql.indexOf("*/") > 0) {
        trimmedSql = trimmedSql.substring(trimmedSql.indexOf("*/") + 2);
        trimmedSql = trimmedSql.trim();
      } else {
        break;
      }

      if (logger.isDebugEnabled()) {
        logger.debug(
            "New sql after skipping /* */ comments: \n{}", SecretDetector.maskSecrets(trimmedSql));
      }
    }

    trimmedSql = trimmedSql.toLowerCase();

    if (trimmedSql.startsWith("put ")) {
      return SFStatementType.PUT;
    } else if (trimmedSql.startsWith("get ")) {
      return SFStatementType.GET;
    } else if (trimmedSql.startsWith("ls ") || trimmedSql.startsWith("list ")) {
      return SFStatementType.LIST;
    } else if (trimmedSql.startsWith("rm ") || trimmedSql.startsWith("remove ")) {
      return SFStatementType.REMOVE;
    } else {
      return null;
    }
  }

  /**
   * Truncate a SQL text for logging
   *
   * @param sql original SQL
   * @return truncated SQL command
   */
  public static String truncateSQL(String sql) {
    return sql.length() > 20 ? sql.substring(0, 20) + "..." : sql;
  }
}
