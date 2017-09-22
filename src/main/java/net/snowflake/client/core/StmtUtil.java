/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.snowflake.client.core.BasicEvent.QueryState;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.SnowflakeUtil;
import net.snowflake.common.api.QueryInProgressResponse;
import java.io.ByteArrayOutputStream;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.GZIPOutputStream;
import org.apache.http.entity.ByteArrayEntity;

import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

/**
 * Created by jhuang on 1/28/16.
 */
public class StmtUtil
{
  static final EventHandler eventHandler = EventUtil.getEventHandlerInstance();

  static final ObjectMapper mapper = new ObjectMapper();

  private static final String SF_PATH_QUERY_V1 = "/queries/v1/query-request";

  private static final String SF_PATH_ABORT_REQUEST_V1 = "/queries/v1/abort-request";

  private static final String SF_QUERY_REQUEST_ID = "requestId";

  private static final String SF_QUERY_COMBINE_DESCRIBE_EXECUTE = "combinedDescribe";

  private static final String SF_HEADER_AUTHORIZATION = HttpHeaders.AUTHORIZATION;

  private static final String SF_HEADER_SNOWFLAKE_AUTHTYPE = "Snowflake";

  private static final String SF_HEADER_TOKEN_TAG = "Token";

  // we don't want to retry canceling forever so put a limit which is
  // twice as much as our default socket timeout
  static final int SF_CANCELING_RETRY_TIMEOUT_IN_MILLIS = 600000; // 10 min

  static final SFLogger logger = SFLoggerFactory.getLogger(StmtUtil.class);

  /**
   * Input for executing a statement on server
   */
  static class StmtInput
  {
    String sql;

    // default to snowflake (a special json format for snowflake query result
    String mediaType = "application/snowflake";
    Map<String, ParameterBindingDTO> bindValues;
    boolean describeOnly;
    String serverUrl;
    String requestId;
    int sequenceId = -1;

    Map<String, Object> parametersMap;
    String sessionToken;
    HttpClient httpClient;
    int networkTimeoutInMillis;
    int injectSocketTimeout; // seconds
    int injectClientPause; // seconds

    AtomicBoolean canceling = null; // canceling flag
    boolean retry;
    String prevGetResultURL = null; // previous get result URL from ping pong

    boolean combineDescribe = false;

    String describedJobId;

    public StmtInput() {};

    public StmtInput setSql(String sql)
    {
      this.sql = sql;
      return this;
    }

    public StmtInput setMediaType(String mediaType)
    {
      this.mediaType = mediaType;
      return this;
    }

    public StmtInput setParametersMap(Map<String, Object> parametersMap)
    {
      this.parametersMap = parametersMap;
      return this;
    }


    public StmtInput setBindValues(Map<String, ParameterBindingDTO> bindValues)
    {
      this.bindValues = bindValues;
      return this;
    }

    public StmtInput setDescribeOnly(boolean describeOnly)
    {
      this.describeOnly = describeOnly;
      return this;
    }

    public StmtInput setServerUrl(String serverUrl)
    {
      this.serverUrl = serverUrl;
      return this;
    }

    public StmtInput setRequestId(String requestId)
    {
      this.requestId = requestId;
      return this;
    }

    public StmtInput setSequenceId(int sequenceId)
    {
      this.sequenceId = sequenceId;
      return this;
    }

    public StmtInput parametersMap(Map<String, Object> parametersMap)
    {
      this.parametersMap = parametersMap;
      return this;
    }

    public StmtInput setSessionToken(String sessionToken)
    {
      this.sessionToken = sessionToken;
      return this;
    }

    public StmtInput setHttpClient(HttpClient httpClient)
    {
      this.httpClient = httpClient;
      return this;
    }

    public StmtInput setNetworkTimeoutInMillis(int networkTimeoutInMillis)
    {
      this.networkTimeoutInMillis = networkTimeoutInMillis;
      return this;
    }

    public StmtInput setInjectSocketTimeout(int injectSocketTimeout)
    {
      this.injectSocketTimeout = injectSocketTimeout;
      return this;
    }

    public StmtInput setInjectClientPause(int injectClientPause)
    {
      this.injectClientPause = injectClientPause;
      return this;
    }

    public StmtInput setCanceling(AtomicBoolean canceling)
    {
      this.canceling = canceling;
      return this;
    }

    public StmtInput setRetry(boolean retry)
    {
      this.retry = retry;
      return this;
    }

    public StmtInput setCombineDescribe(boolean combineDescribe)
    {
      this.combineDescribe = combineDescribe;
      return this;
    }

    public StmtInput setDescribedJobId(String describedJobId)
    {
      this.describedJobId = describedJobId;
      return this;
    }
  }

  /**
   * Output for running a statement on server
   */
  static public class StmtOutput
  {
    JsonNode result;

    public StmtOutput(JsonNode result)
    {
      this.result = result;
    }

    public JsonNode getResult()
    {
      return result;
    }
  }

  /**
   * Execute a statement
   *
   * side effect: stmtInput.prevGetResultURL is set if we have started ping
   * pong and receives an exception from session token expiration so that
   * during retry we don't retry the query submission, but continue the
   * ping pong process.
   *
   * @param stmtInput input statement
   * @return StmtOutput output statement
   *
   * @throws SFException exception raised from Snowflake components
   * @throws SnowflakeSQLException exception raised from Snowflake components
   */
  public static StmtOutput execute(StmtInput stmtInput) throws SFException,
      SnowflakeSQLException
  {
    HttpPost httpRequest = null;

    AssertUtil.assertTrue(stmtInput.serverUrl != null,
        "Missing server url for statement execution");

    AssertUtil.assertTrue(stmtInput.sql != null,
        "Missing sql for statement execution");

    AssertUtil.assertTrue(stmtInput.requestId != null,
        "Missing request id for statement execution");

    AssertUtil.assertTrue(stmtInput.sequenceId >=0,
        "Negative sequence id for statement execution");

    AssertUtil.assertTrue(stmtInput.mediaType != null,
        "Missing media type for statement execution");

    AssertUtil.assertTrue(stmtInput.httpClient != null,
        "Missing http client for statement execution");

    try
    {
      String resultAsString = null;

      // SNOW-20443: if we are retrying and there is get result URL, we
      // don't need to execute the query again
      if (stmtInput.retry && stmtInput.prevGetResultURL != null)
      {
        logger.debug(
            "retrying statement execution with get result URL: {}",
            stmtInput.prevGetResultURL);
      }
      else
      {
        URIBuilder uriBuilder = new URIBuilder(stmtInput.serverUrl);

        uriBuilder.setPath(SF_PATH_QUERY_V1);
        uriBuilder.addParameter(SF_QUERY_REQUEST_ID, stmtInput.requestId);

        if (stmtInput.combineDescribe)
        {
          uriBuilder.addParameter(SF_QUERY_COMBINE_DESCRIBE_EXECUTE, "true");
        }

        httpRequest = new HttpPost(uriBuilder.build());

        /**
         * sequence id is only needed for old query API, when old query API
         * is deprecated, we can remove sequence id.
         */
        QueryExecDTO sqlJsonBody = new QueryExecDTO(
            stmtInput.sql,
            stmtInput.describeOnly,
            Integer.valueOf(stmtInput.sequenceId),
            stmtInput.bindValues,
            stmtInput.parametersMap);

        if (stmtInput.combineDescribe && !stmtInput.describeOnly)
        {
          sqlJsonBody.setDescribedJobId(stmtInput.describedJobId);
        }

        String json = mapper.writeValueAsString(sqlJsonBody);

        if (logger.isDebugEnabled())
        {
          logger.debug("JSON: {}", json);
        }

        // SNOW-18057: compress the post body in gzip
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        GZIPOutputStream gzos = new GZIPOutputStream(baos);
        byte[] bytes = json.getBytes("UTF-8");
        gzos.write(bytes);
        gzos.finish();
        ByteArrayEntity input = new ByteArrayEntity(baos.toByteArray());
        input.setContentType("application/json");
        httpRequest.setEntity(input);
        httpRequest.addHeader("content-encoding", "gzip");

        httpRequest.addHeader("accept", stmtInput.mediaType);

        httpRequest.setHeader(SF_HEADER_AUTHORIZATION,
            SF_HEADER_SNOWFLAKE_AUTHTYPE + " " + SF_HEADER_TOKEN_TAG
                + "=\"" + stmtInput.sessionToken + "\"");

        eventHandler.triggerStateTransition(BasicEvent.QueryState.SENDING_QUERY,
            String.format(QueryState.SENDING_QUERY.getArgString(), stmtInput.requestId));

        resultAsString =
            HttpUtil.executeRequest(httpRequest,
                                    stmtInput.httpClient,
                                    stmtInput.networkTimeoutInMillis / 1000,
                                    stmtInput.injectSocketTimeout,
                                    stmtInput.canceling);
      }

      /**
       * Check response for error or for ping pong response
       *
       * For ping-pong: want to make sure our connection is not silently dropped
       * by middle players (e.g load balancer/VPN timeout) between client and GS
       */
      JsonNode pingPongResponseJson = null;
      boolean queryInProgress = true;
      boolean firstResponse = !stmtInput.retry;
      String previousGetResultPath =
          (stmtInput.retry?stmtInput.prevGetResultURL:null);
      int retries = 0;
      final int MAX_RETRIES = 3;

      do
      {
        pingPongResponseJson = null;

        if (resultAsString != null)
        {
          try
          {
            pingPongResponseJson = mapper.readTree(resultAsString);
          }
          catch (Exception ex)
          {
            logger.error("Bad result json: {}, " +
                    "JSON parsing exception: {}, http request: {}",
                    resultAsString, ex.getLocalizedMessage(),
                    httpRequest);

            logger.error("Exception stack trace", ex);
          }
        }

        eventHandler.triggerStateTransition(BasicEvent.QueryState.WAITING_FOR_RESULT,
            "{requestId: " + stmtInput.requestId + "," +
             "pingNumber: " + retries + "}");

        if (pingPongResponseJson == null)
        {
          /**
           * Retry for bad response for server.
           * But we don't want to retry too many times
           */
          if (retries >= MAX_RETRIES)
          {
            SFException sfe =
                IncidentUtil.generateIncidentWithException(stmtInput.sessionToken,
                    stmtInput.serverUrl, stmtInput.requestId, null,
                        ErrorCode.BAD_RESPONSE, resultAsString);

            throw sfe;
          }
          else
          {
            logger.info("Will retry get result. Retry count: {}",
                retries);

            retries++;
          }
        }
        else
          retries = 0; // reset retry counter after a successful response

        // trace the response if requested
        logger.debug("Json response: {}", resultAsString);

        if (pingPongResponseJson != null)
        // raise server side error as an exception if any
        {
          SnowflakeUtil.checkErrorAndThrowException(pingPongResponseJson);
        }

        // check the response code to see if it is a progress report response
        if (pingPongResponseJson != null &&
            !QueryInProgressResponse.QUERY_IN_PROGRESS_CODE.equals(
                pingPongResponseJson.path("code").asText())
            && !QueryInProgressResponse.QUERY_IN_PROGRESS_ASYNC_CODE.equals(
            pingPongResponseJson.path("code").asText()))
        {
          queryInProgress = false;
        }
        else
        {
          queryInProgress = true;

          if (firstResponse)
          {
            // sleep some time to simulate client pause. The purpose is to
            // simulate client pause before trying to fetch result so that
            // we can test query behavior related to disconnected client
            if (stmtInput.injectClientPause != 0)
            {
              logger.debug(
                  "inject client pause for {} seconds",
                  stmtInput.injectClientPause);

              Thread.sleep(stmtInput.injectClientPause * 1000);
            }
          }

          resultAsString = getQueryResult(pingPongResponseJson,
                                          stmtInput.mediaType,
                                          previousGetResultPath,
                                          stmtInput);

          // save the previous get result path in case we run into session
          // expiration
          if (pingPongResponseJson != null)
          {
            previousGetResultPath = pingPongResponseJson.path("data").
                path("getResultUrl").asText();
            stmtInput.prevGetResultURL = previousGetResultPath;
          }
        }

        // not first response any more
        if (firstResponse)
          firstResponse = false;
      }
      while (queryInProgress);

      logger.debug("Returning result");

      eventHandler.triggerStateTransition(BasicEvent.QueryState.PROCESSING_RESULT,
          String.format(QueryState.PROCESSING_RESULT.getArgString(), stmtInput.requestId));

      return new StmtOutput(pingPongResponseJson);
    }
    catch (Exception ex)
    {
      if (!(ex instanceof SnowflakeSQLException))
      {
        if (ex instanceof IOException)
        {
          logger.error("IOException encountered", ex);

          // network error
          throw new SFException(ex, ErrorCode.NETWORK_ERROR,
              "Exception encountered when executing statement: " +
                  ex.getLocalizedMessage());
        }
        else
        {
          logger.error("Exception encountered", ex);

          // raise internal exception if this is not a snowflake exception
          throw new SFException(ex, ErrorCode.INTERNAL_ERROR,
              ex.getLocalizedMessage());
        }
      }
      else
      {
        throw (SnowflakeSQLException) ex;
      }
    }
    finally
    {
      // we can release the http connection now
      if (httpRequest != null)
      {
        httpRequest.releaseConnection();
        httpRequest = null;
      }
    }
  }

  /**
   * Issue get-result call to get query result given an in progress response.
   * <p>
   * @param inProgressResponse In pregress response in JSON form
   * @param mediaType media type name
   * @param previousGetResultPath previous get results path
   * @param stmtInput input statement
   * @return results in string form
   * @throws SFException exception raised from Snowflake components
   * @throws SnowflakeSQLException exception raised from Snowflake components
   */
  static protected String getQueryResult(JsonNode inProgressResponse,
                                        String mediaType,
                                        String previousGetResultPath,
                                        StmtInput stmtInput)
      throws SFException, SnowflakeSQLException
  {
    HttpGet httpRequest = null;
    String getResultPath = null;

    // get result url better not be empty
    if (inProgressResponse == null ||
        inProgressResponse.path("data").path("getResultUrl").isMissingNode())
    {
      if (previousGetResultPath == null)
      {
        throw new SFException(ErrorCode.INTERNAL_ERROR,
            "No query response or missing get result URL");
      }
      else
      {
        logger.debug("No query response or missing get result URL, " +
            "use previous get result URL: {}", previousGetResultPath);
        getResultPath = previousGetResultPath;
      }
    }
    else
    {
      getResultPath = inProgressResponse.path("data").path("getResultUrl").
          asText();
    }

    logger.debug("get query result: {}", getResultPath);

    try
    {
      URIBuilder uriBuilder = new URIBuilder(stmtInput.serverUrl);

      uriBuilder.setPath(getResultPath);

      uriBuilder.addParameter(SF_QUERY_REQUEST_ID,
          UUID.randomUUID().toString());

      httpRequest = new HttpGet(uriBuilder.build());

      httpRequest.addHeader("accept", mediaType);

      httpRequest.setHeader(SF_HEADER_AUTHORIZATION,
          SF_HEADER_SNOWFLAKE_AUTHTYPE + " " + SF_HEADER_TOKEN_TAG
              + "=\"" + stmtInput.sessionToken + "\"");

      String resultAsString =
          HttpUtil.executeRequest(httpRequest,
                                  stmtInput.httpClient,
                                  stmtInput.networkTimeoutInMillis/1000,
                                  0,
                                  stmtInput.canceling);

      return resultAsString;
    }
    catch (URISyntaxException | IOException ex)
    {
      logger.error("Exception encountered when getting result for "
          + httpRequest, ex);

      // raise internal exception if this is not a snowflake exception
      throw new SFException(ex, ErrorCode.INTERNAL_ERROR,
                            ex.getLocalizedMessage());
    }
  }

  /**
   * Cancel a statement identifiable by a request id
   *
   * @param stmtInput input statement
   * @throws SFException if there is an internal exception
   * @throws SnowflakeSQLException if failed to cancel the statement
   */
  public static void cancel(StmtInput stmtInput) throws SFException,
      SnowflakeSQLException
  {
    HttpPost httpRequest = null;

    AssertUtil.assertTrue(stmtInput.serverUrl != null,
        "Missing server url for statement execution");

    AssertUtil.assertTrue(stmtInput.sql != null,
        "Missing sql for statement execution");

    AssertUtil.assertTrue(stmtInput.mediaType != null,
        "Missing media type for statement execution");

    AssertUtil.assertTrue(stmtInput.requestId != null,
        "Missing request id for statement execution");

    AssertUtil.assertTrue(stmtInput.httpClient != null,
        "Missing http client for statement execution");

    AssertUtil.assertTrue(stmtInput.sessionToken != null,
        "Missing session token for statement execution");

    try
    {
      URIBuilder uriBuilder = new URIBuilder(stmtInput.serverUrl);

      logger.debug("Aborting query: {}", stmtInput.sql);

      uriBuilder.setPath(SF_PATH_ABORT_REQUEST_V1);

      uriBuilder.addParameter(SF_QUERY_REQUEST_ID, UUID.randomUUID().toString());

      httpRequest = new HttpPost(uriBuilder.build());

      /**
       * The JSON input has two fields: sqlText and requestId
       */
      Map sqlJsonBody = new HashMap<String, Object>();
      sqlJsonBody.put("sqlText", stmtInput.sql);
      sqlJsonBody.put("requestId", stmtInput.requestId);

      String json = mapper.writeValueAsString(sqlJsonBody);

      logger.debug("JSON for cancel request: {}", json);

      StringEntity input = new StringEntity(json, Charset.forName("UTF-8"));
      input.setContentType("application/json");
      httpRequest.setEntity(input);

      httpRequest.addHeader("accept", stmtInput.mediaType);

      httpRequest.setHeader(SF_HEADER_AUTHORIZATION,
          SF_HEADER_SNOWFLAKE_AUTHTYPE + " " + SF_HEADER_TOKEN_TAG
              + "=\"" + stmtInput.sessionToken + "\"");

      HttpResponse response;

      String jsonString =
          HttpUtil.executeRequest(httpRequest,
                                  stmtInput.httpClient,
                                  SF_CANCELING_RETRY_TIMEOUT_IN_MILLIS,
                                  0, null);

      // trace the response if requested
      logger.debug("Json response: {}", jsonString);

      JsonNode rootNode = null;
      rootNode = mapper.readTree(jsonString);

      // raise server side error as an exception if any
      SnowflakeUtil.checkErrorAndThrowException(rootNode);
    }
    catch (URISyntaxException | IOException ex)
    {
      logger.error(
                 "Exception encountered when canceling " + httpRequest,
                 ex);

      // raise internal exception if this is not a snowflake exception
      throw new SFException(ex,
                            ErrorCode.INTERNAL_ERROR,
                            ex.getLocalizedMessage());
    }
  }

  /**
   * A simple function to check if the statement is related to manipulate stage.
   *
   * @param sql a SQL statement/command
   * @return PUT/GET/LIST/RM if statment belongs to one of them, otherwise
   *         return NULL
   */
  static public SFStatementType checkStageManageCommand(String sql)
  {
    if (sql == null)
    {
      return null;
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

      logger.debug( "New sql after skipping // comments: \n{}",
          trimmedSql);

    }

    // skip commenting enclosed with /* */
    while (trimmedSql.startsWith("/*"))
    {
      logger.debug( "skipping /* */ comments in: \n{}", trimmedSql);

      if (trimmedSql.indexOf("*/") > 0)
      {
        trimmedSql = trimmedSql.substring(trimmedSql.indexOf("*/") + 2);
        trimmedSql = trimmedSql.trim();
      }
      else
      {
        break;

      }
      logger.debug( "New sql after skipping /* */ comments: \n{}", trimmedSql);

    }

    trimmedSql = trimmedSql.toLowerCase();

    if (trimmedSql.startsWith("put "))
    {
      return SFStatementType.PUT;
    }
    else if (trimmedSql.startsWith("get "))
    {
      return SFStatementType.GET;
    }
    else if (trimmedSql.startsWith("ls ") ||
             trimmedSql.startsWith("list "))
    {
      return SFStatementType.LIST;
    }
    else if (trimmedSql.startsWith("rm ") ||
            trimmedSql.startsWith("remove "))
    {
      return SFStatementType.REMOVE;
    }
    else
    {
      return null;
    }
  }
}
