package net.snowflake.client.jdbc;

import static net.snowflake.client.jdbc.SnowflakeUtil.isNullOrEmpty;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLKeyException;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLProtocolException;
import net.snowflake.client.core.Event;
import net.snowflake.client.core.EventUtil;
import net.snowflake.client.core.ExecTimeTelemetryData;
import net.snowflake.client.core.HttpExecutingContext;
import net.snowflake.client.core.HttpExecutingContextBuilder;
import net.snowflake.client.core.HttpResponseContextDto;
import net.snowflake.client.core.HttpUtil;
import net.snowflake.client.core.ObjectMapperFactory;
import net.snowflake.client.core.SFOCSPException;
import net.snowflake.client.core.SessionUtil;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.core.URLUtil;
import net.snowflake.client.core.UUIDUtils;
import net.snowflake.client.jdbc.telemetryOOB.TelemetryService;
import net.snowflake.client.log.ArgSupplier;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.client.util.DecorrelatedJitterBackoff;
import net.snowflake.client.util.SecretDetector;
import net.snowflake.client.util.Stopwatch;
import net.snowflake.common.core.SqlState;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;

/**
 * This is an abstraction on top of http client.
 *
 * <p>Currently it only has one method for retrying http request execution so that the same logic
 * doesn't have to be replicated at difference places where retry is needed.
 */
public class RestRequest {
  private static final SFLogger logger = SFLoggerFactory.getLogger(RestRequest.class);

  // Request guid per HTTP request
  private static final String SF_REQUEST_GUID = "request_guid";

  // min backoff in milli before we retry due to transient issues
  private static final long minBackoffInMilli = 1000;

  // max backoff in milli before we retry due to transient issues
  // we double the backoff after each retry till we reach the max backoff
  private static final long maxBackoffInMilli = 16000;

  // retry at least once even if timeout limit has been reached
  private static final int MIN_RETRY_COUNT = 1;

  static final String ERROR_FIELD_NAME = "error";
  static final String ERROR_USE_DPOP_NONCE = "use_dpop_nonce";
  static final String DPOP_NONCE_HEADER_NAME = "dpop-nonce";

  /**
   * Execute an HTTP request with retry logic.
   *
   * @param httpClient client object used to communicate with other machine
   * @param httpRequest request object contains all the request information
   * @param retryTimeout : retry timeout (in seconds)
   * @param authTimeout : authenticator specific timeout (in seconds)
   * @param socketTimeout : curl timeout (in ms)
   * @param maxRetries : max retry count for the request
   * @param injectSocketTimeout : simulate socket timeout
   * @param canceling canceling flag
   * @param withoutCookies whether the cookie spec should be set to IGNORE or not
   * @param includeRetryParameters whether to include retry parameters in retried requests. Only
   *     needs to be true for JDBC statement execution (query requests to Snowflake server).
   * @param includeRequestGuid whether to include request_guid parameter
   * @param retryHTTP403 whether to retry on HTTP 403 or not should be executed before and/or after
   *     the retry
   * @return HttpResponse Object get from server
   * @throws net.snowflake.client.jdbc.SnowflakeSQLException Request timeout Exception or Illegal
   *     State Exception i.e. connection is already shutdown etc
   */
  public static CloseableHttpResponse execute(
      CloseableHttpClient httpClient,
      HttpRequestBase httpRequest,
      long retryTimeout,
      long authTimeout,
      int socketTimeout,
      int maxRetries,
      int injectSocketTimeout,
      AtomicBoolean canceling,
      boolean withoutCookies,
      boolean includeRetryParameters,
      boolean includeRequestGuid,
      boolean retryHTTP403,
      ExecTimeTelemetryData execTimeTelemetryData)
      throws SnowflakeSQLException {
    return execute(
        httpClient,
        httpRequest,
        retryTimeout,
        authTimeout,
        socketTimeout,
        maxRetries,
        injectSocketTimeout,
        canceling,
        withoutCookies,
        includeRetryParameters,
        includeRequestGuid,
        retryHTTP403,
        false, // noRetry
        execTimeTelemetryData,
        null);
  }

  /**
   * Execute an HTTP request with retry logic.
   *
   * @param httpClient client object used to communicate with other machine
   * @param httpRequest request object contains all the request information
   * @param retryTimeout : retry timeout (in seconds)
   * @param authTimeout : authenticator specific timeout (in seconds)
   * @param socketTimeout : curl timeout (in ms)
   * @param maxRetries : max retry count for the request
   * @param injectSocketTimeout : simulate socket timeout
   * @param canceling canceling flag
   * @param withoutCookies whether the cookie spec should be set to IGNORE or not
   * @param includeRetryParameters whether to include retry parameters in retried requests. Only
   *     needs to be true for JDBC statement execution (query requests to Snowflake server).
   * @param includeRequestGuid whether to include request_guid parameter
   * @param retryHTTP403 whether to retry on HTTP 403 or not should be executed before and/or after
   *     the retry
   * @return HttpResponse Object get from server
   * @throws net.snowflake.client.jdbc.SnowflakeSQLException Request timeout Exception or Illegal
   *     State Exception i.e. connection is already shutdown etc
   */
  public static CloseableHttpResponse execute(
      CloseableHttpClient httpClient,
      HttpRequestBase httpRequest,
      long retryTimeout,
      long authTimeout,
      int socketTimeout,
      int maxRetries,
      int injectSocketTimeout,
      AtomicBoolean canceling,
      boolean withoutCookies,
      boolean includeRetryParameters,
      boolean includeRequestGuid,
      boolean retryHTTP403,
      boolean noRetry,
      ExecTimeTelemetryData execTimeData)
      throws SnowflakeSQLException {
    return execute(
        httpClient,
        httpRequest,
        retryTimeout,
        authTimeout,
        socketTimeout,
        maxRetries,
        injectSocketTimeout,
        canceling,
        withoutCookies,
        includeRetryParameters,
        includeRequestGuid,
        retryHTTP403,
        noRetry,
        execTimeData,
        null);
  }

  /**
   * Execute an HTTP request with retry logic.
   *
   * @param httpClient client object used to communicate with other machine
   * @param httpRequest request object contains all the request information
   * @param retryTimeout : retry timeout (in seconds)
   * @param authTimeout : authenticator specific timeout (in seconds)
   * @param socketTimeout : curl timeout (in ms)
   * @param maxRetries : max retry count for the request
   * @param injectSocketTimeout : simulate socket timeout
   * @param canceling canceling flag
   * @param withoutCookies whether the cookie spec should be set to IGNORE or not
   * @param includeRetryParameters whether to include retry parameters in retried requests. Only
   *     needs to be true for JDBC statement execution (query requests to Snowflake server).
   * @param includeRequestGuid whether to include request_guid parameter
   * @param retryHTTP403 whether to retry on HTTP 403 or not
   * @param execTimeData ExecTimeTelemetryData should be executed before and/or after the retry
   * @return HttpResponse Object get from server
   * @throws net.snowflake.client.jdbc.SnowflakeSQLException Request timeout Exception or Illegal
   *     State Exception i.e. connection is already shutdown etc
   */
  public static CloseableHttpResponse execute(
      CloseableHttpClient httpClient,
      HttpRequestBase httpRequest,
      long retryTimeout,
      long authTimeout,
      int socketTimeout,
      int maxRetries,
      int injectSocketTimeout,
      AtomicBoolean canceling,
      boolean withoutCookies,
      boolean includeRetryParameters,
      boolean includeRequestGuid,
      boolean retryHTTP403,
      ExecTimeTelemetryData execTimeData,
      RetryContextManager retryContextManager)
      throws SnowflakeSQLException {
    return execute(
        httpClient,
        httpRequest,
        retryTimeout,
        authTimeout,
        socketTimeout,
        maxRetries,
        injectSocketTimeout,
        canceling,
        withoutCookies,
        includeRetryParameters,
        includeRequestGuid,
        retryHTTP403,
        false, // noRetry
        execTimeData,
        retryContextManager);
  }

  /**
   * Execute an HTTP request with retry logic.
   *
   * @param httpClient client object used to communicate with other machine
   * @param httpRequest request object contains all the request information
   * @param retryTimeout : retry timeout (in seconds)
   * @param authTimeout : authenticator specific timeout (in seconds)
   * @param socketTimeout : curl timeout (in ms)
   * @param maxRetries : max retry count for the request
   * @param injectSocketTimeout : simulate socket timeout
   * @param canceling canceling flag
   * @param withoutCookies whether the cookie spec should be set to IGNORE or not
   * @param includeRetryParameters whether to include retry parameters in retried requests. Only
   *     needs to be true for JDBC statement execution (query requests to Snowflake server).
   * @param includeRequestGuid whether to include request_guid parameter
   * @param retryHTTP403 whether to retry on HTTP 403 or not
   * @param noRetry should we disable retry on non-successful http resp code
   * @param execTimeData ExecTimeTelemetryData
   * @param retryManager RetryContextManager - object allowing to optionally pass custom logic that
   *     should be executed before and/or after the retry
   * @return HttpResponse Object get from server
   * @throws net.snowflake.client.jdbc.SnowflakeSQLException Request timeout Exception or Illegal
   *     State Exception i.e. connection is already shutdown etc
   */
  public static CloseableHttpResponse execute(
      CloseableHttpClient httpClient,
      HttpRequestBase httpRequest,
      long retryTimeout,
      long authTimeout,
      int socketTimeout,
      int maxRetries,
      int injectSocketTimeout,
      AtomicBoolean canceling,
      boolean withoutCookies,
      boolean includeRetryParameters,
      boolean includeRequestGuid,
      boolean retryHTTP403,
      boolean noRetry,
      ExecTimeTelemetryData execTimeData,
      RetryContextManager retryManager)
      throws SnowflakeSQLException {
    return executeWitRetries(
            httpClient,
            httpRequest,
            retryTimeout,
            authTimeout,
            socketTimeout,
            maxRetries,
            injectSocketTimeout,
            canceling, // no canceling
            withoutCookies, // no cookie
            includeRetryParameters, // no retry
            includeRequestGuid, // no request_guid
            retryHTTP403, // retry on HTTP 403
            noRetry,
            new ExecTimeTelemetryData())
        .getHttpResponse();
  }

  static long getNewBackoffInMilli(
      long previousBackoffInMilli,
      boolean isLoginRequest,
      DecorrelatedJitterBackoff decorrelatedJitterBackoff,
      int retryCount,
      long retryTimeoutInMilliseconds,
      long elapsedMilliForTransientIssues) {
    long backoffInMilli;
    if (isLoginRequest) {
      long jitteredBackoffInMilli =
          decorrelatedJitterBackoff.getJitterForLogin(previousBackoffInMilli);
      backoffInMilli =
          (long)
              decorrelatedJitterBackoff.chooseRandom(
                  jitteredBackoffInMilli + previousBackoffInMilli,
                  Math.pow(2, retryCount) + jitteredBackoffInMilli);
    } else {

      backoffInMilli = decorrelatedJitterBackoff.nextSleepTime(previousBackoffInMilli);
    }

    backoffInMilli = Math.min(maxBackoffInMilli, Math.max(previousBackoffInMilli, backoffInMilli));

    if (retryTimeoutInMilliseconds > 0
        && (elapsedMilliForTransientIssues + backoffInMilli) > retryTimeoutInMilliseconds) {
      // If the timeout will be reached before the next backoff, just use the remaining
      // time (but cannot be negative) - this is the only place when backoff is not in range
      // min-max.
      backoffInMilli =
          Math.max(
              0,
              Math.min(
                  backoffInMilli, retryTimeoutInMilliseconds - elapsedMilliForTransientIssues));
      logger.debug(
          "We are approaching retry timeout {}ms, setting backoff to {}ms",
          retryTimeoutInMilliseconds,
          backoffInMilli);
    }
    return backoffInMilli;
  }

  static boolean isNonRetryableHTTPCode(CloseableHttpResponse response, boolean retryHTTP403) {
    return (response != null)
        && (response.getStatusLine().getStatusCode() < 500
            || // service unavailable
            response.getStatusLine().getStatusCode() >= 600)
        && // gateway timeout
        response.getStatusLine().getStatusCode() != 408
        && // retry
        response.getStatusLine().getStatusCode() != 429
        && // request timeout
        (!retryHTTP403 || response.getStatusLine().getStatusCode() != 403);
  }

  private static boolean isCertificateRevoked(Exception ex) {
    if (ex == null) {
      return false;
    }
    Throwable ex0 = getRootCause(ex);
    if (!(ex0 instanceof SFOCSPException)) {
      return false;
    }
    SFOCSPException cause = (SFOCSPException) ex0;
    return cause.getErrorCode() == OCSPErrorCode.CERTIFICATE_STATUS_REVOKED;
  }

  private static Throwable getRootCause(Throwable ex) {
    Throwable ex0 = ex;
    while (ex0.getCause() != null) {
      ex0 = ex0.getCause();
    }
    return ex0;
  }

  private static void setRequestConfig(
      HttpRequestBase httpRequest,
      boolean withoutCookies,
      int injectSocketTimeout,
      String requestIdStr,
      long authTimeoutInMilli) {
    if (withoutCookies) {
      httpRequest.setConfig(HttpUtil.getRequestConfigWithoutCookies());
    }

    // For first call, simulate a socket timeout by setting socket timeout
    // to the injected socket timeout value
    if (injectSocketTimeout != 0) {
      // test code path
      logger.debug(
          "{}Injecting socket timeout by setting socket timeout to {} ms",
          requestIdStr,
          injectSocketTimeout);
      httpRequest.setConfig(
          HttpUtil.getDefaultRequestConfigWithSocketTimeout(injectSocketTimeout, withoutCookies));
    }

    // When the auth timeout is set, set the socket timeout as the authTimeout
    // so that it can be renewed in time and pass it to the http request configuration.
    if (authTimeoutInMilli > 0) {
      int requestSocketAndConnectTimeout = (int) authTimeoutInMilli;
      logger.debug(
          "{}Setting auth timeout as the socket timeout: {} ms", requestIdStr, authTimeoutInMilli);
      httpRequest.setConfig(
          HttpUtil.getDefaultRequestConfigWithSocketAndConnectTimeout(
              requestSocketAndConnectTimeout, withoutCookies));
    }
  }

  private static void setRequestURI(
      HttpRequestBase httpRequest,
      String requestIdStr,
      boolean includeRetryParameters,
      boolean includeRequestGuid,
      int retryCount,
      String lastStatusCodeForRetry,
      long startTime,
      String requestInfoScrubbed)
      throws URISyntaxException {
    /*
     * Add retryCount if the first request failed
     * GS can use the parameter for optimization. Specifically GS
     * will only check metadata database to see if a query has been running
     * for a retry request. This way for the majority of query requests
     * which are not part of retry we don't have to pay the performance
     * overhead of looking up in metadata database.
     */
    URIBuilder builder = new URIBuilder(httpRequest.getURI());
    // If HTAP
    if ("true".equalsIgnoreCase(System.getenv("HTAP_SIMULATION"))
        && builder.getPathSegments().contains("query-request")) {
      logger.debug("{}Setting htap simulation", requestIdStr);
      builder.setParameter("target", "htap_simulation");
    }
    if (includeRetryParameters && retryCount > 0) {
      updateRetryParameters(builder, retryCount, lastStatusCodeForRetry, startTime);
    }

    if (includeRequestGuid) {
      UUID guid = UUIDUtils.getUUID();
      logger.debug("{}Request {} guid: {}", requestIdStr, requestInfoScrubbed, guid.toString());
      // Add request_guid for better tracing
      builder.setParameter(SF_REQUEST_GUID, guid.toString());
    }

    httpRequest.setURI(builder.build());
  }

  /**
   * Execute an HTTP request with retry logic.
   *
   * @param httpClient client object used to communicate with other machine
   * @param httpRequest request object contains all the request information
   * @param retryTimeout : retry timeout (in seconds)
   * @param authTimeout : authenticator specific timeout (in seconds)
   * @param socketTimeout : curl timeout (in ms)
   * @param maxRetries : max retry count for the request
   * @param injectSocketTimeout : simulate socket timeout
   * @param canceling canceling flag
   * @param withoutCookies whether the cookie spec should be set to IGNORE or not
   * @param includeRetryParameters whether to include retry parameters in retried requests. Only
   *     needs to be true for JDBC statement execution (query requests to Snowflake server).
   * @param includeRequestGuid whether to include request_guid parameter
   * @param retryHTTP403 whether to retry on HTTP 403 or not
   * @return HttpResponseContextDto Object get from server or exception
   * @throws net.snowflake.client.jdbc.SnowflakeSQLException Request timeout Exception or Illegal
   *     State Exception i.e. connection is already shutdown etc
   */
  @SnowflakeJdbcInternalApi
  public static HttpResponseContextDto executeWitRetries(
      CloseableHttpClient httpClient,
      HttpRequestBase httpRequest,
      long retryTimeout,
      long authTimeout,
      int socketTimeout,
      int maxRetries,
      int injectSocketTimeout,
      AtomicBoolean canceling,
      boolean withoutCookies,
      boolean includeRetryParameters,
      boolean includeRequestGuid,
      boolean retryHTTP403,
      boolean unpackResponse,
      ExecTimeTelemetryData execTimeTelemetryData)
      throws SnowflakeSQLException {
    return executeWitRetries(
        httpClient,
        httpRequest,
        retryTimeout,
        authTimeout,
        socketTimeout,
        maxRetries,
        injectSocketTimeout,
        canceling,
        withoutCookies,
        includeRetryParameters,
        includeRequestGuid,
        retryHTTP403,
        false,
        unpackResponse,
        execTimeTelemetryData);
  }

  /**
   * Execute an HTTP request with retry logic.
   *
   * @param httpClient client object used to communicate with other machine
   * @param httpRequest request object contains all the request information
   * @param retryTimeout : retry timeout (in seconds)
   * @param authTimeout : authenticator specific timeout (in seconds)
   * @param socketTimeout : curl timeout (in ms)
   * @param maxRetries : max retry count for the request
   * @param injectSocketTimeout : simulate socket timeout
   * @param canceling canceling flag
   * @param withoutCookies whether the cookie spec should be set to IGNORE or not
   * @param includeRetryParameters whether to include retry parameters in retried requests. Only
   *     needs to be true for JDBC statement execution (query requests to Snowflake server).
   * @param includeRequestGuid whether to include request_guid parameter
   * @param retryHTTP403 whether to retry on HTTP 403 or not
   * @param execTimeTelemetryData ExecTimeTelemetryData should be executed before and/or after the
   *     retry
   * @return HttpResponseContextDto Object get from server or exception
   * @throws net.snowflake.client.jdbc.SnowflakeSQLException Request timeout Exception or Illegal
   *     State Exception i.e. connection is already shutdown etc
   */
  @SnowflakeJdbcInternalApi
  public static HttpResponseContextDto executeWitRetries(
      CloseableHttpClient httpClient,
      HttpRequestBase httpRequest,
      long retryTimeout,
      long authTimeout,
      int socketTimeout,
      int maxRetries,
      int injectSocketTimeout,
      AtomicBoolean canceling,
      boolean withoutCookies,
      boolean includeRetryParameters,
      boolean includeRequestGuid,
      boolean retryHTTP403,
      boolean noRetry,
      boolean unpackResponse,
      ExecTimeTelemetryData execTimeTelemetryData)
      throws SnowflakeSQLException {
    String requestIdStr = URLUtil.getRequestIdLogStr(httpRequest.getURI());
    String requestInfoScrubbed = SecretDetector.maskSASToken(httpRequest.toString());
    HttpExecutingContext context =
        HttpExecutingContextBuilder.withRequest(requestIdStr, requestInfoScrubbed)
            .retryTimeout(retryTimeout)
            .authTimeout(authTimeout)
            .origSocketTimeout(socketTimeout)
            .maxRetries(maxRetries)
            .injectSocketTimeout(injectSocketTimeout)
            .canceling(canceling)
            .withoutCookies(withoutCookies)
            .includeRetryParameters(includeRetryParameters)
            .includeRequestGuid(includeRequestGuid)
            .retryHTTP403(retryHTTP403)
            .noRetry(noRetry)
            .unpackResponse(unpackResponse)
            .loginRequest(SessionUtil.isNewRetryStrategyRequest(httpRequest))
            .build();
    return executeWitRetries(httpClient, httpRequest, context, execTimeTelemetryData, null);
  }

  /**
   * Execute an HTTP request with retry logic.
   *
   * @param httpClient client object used to communicate with other machine
   * @param httpRequest request object contains all the request information
   * @param execTimeData ExecTimeTelemetryData should be executed before and/or after the retry
   * @param retryManager RetryManager containing extra actions used during retries
   * @return HttpResponseContextDto Object get from server or exception
   * @throws net.snowflake.client.jdbc.SnowflakeSQLException Request timeout Exception or Illegal
   *     State Exception i.e. connection is already shutdown etc
   */
  @SnowflakeJdbcInternalApi
  public static HttpResponseContextDto executeWitRetries(
      CloseableHttpClient httpClient,
      HttpRequestBase httpRequest,
      HttpExecutingContext httpExecutingContext,
      ExecTimeTelemetryData execTimeData,
      RetryContextManager retryManager)
      throws SnowflakeSQLException {
    Stopwatch networkComunnicationStapwatch = null;
    Stopwatch requestReponseStopWatch = null;
    HttpResponseContextDto responseDto = new HttpResponseContextDto();

    if (logger.isDebugEnabled()) {
      networkComunnicationStapwatch = new Stopwatch();
      networkComunnicationStapwatch.start();
      logger.debug(
          "{}Executing rest request: {}, retry timeout: {}, socket timeout: {}, max retries: {},"
              + " inject socket timeout: {}, canceling: {}, without cookies: {}, include retry parameters: {},"
              + " include request guid: {}, retry http 403: {}, no retry: {}",
          httpExecutingContext.getRequestId(),
          httpExecutingContext.getRequestInfoScrubbed(),
          httpExecutingContext.getRetryTimeoutInMilliseconds(),
          httpExecutingContext.getOrigSocketTimeout(),
          httpExecutingContext.getMaxRetries(),
          httpExecutingContext.isInjectSocketTimeout(),
          httpExecutingContext.getCanceling(),
          httpExecutingContext.isWithoutCookies(),
          httpExecutingContext.isIncludeRetryParameters(),
          httpExecutingContext.isIncludeRequestGuid(),
          httpExecutingContext.isRetryHTTP403(),
          httpExecutingContext.isNoRetry());
    }
    if (httpExecutingContext.isLoginRequest()) {
      logger.debug(
          "{}Request is a login/auth request. Using new retry strategy",
          httpExecutingContext.getRequestId());
    }

    RestRequest.setRequestConfig(
        httpRequest,
        httpExecutingContext.isWithoutCookies(),
        httpExecutingContext.getInjectSocketTimeout(),
        httpExecutingContext.getRequestId(),
        httpExecutingContext.getAuthTimeoutInMilliseconds());

    // try request till we get a good response or retry timeout
    while (true) {
      logger.debug(
          "{}Retry count: {}, max retries: {}, retry timeout: {} s, backoff: {} ms. Attempting request: {}",
          httpExecutingContext.getRequestId(),
          httpExecutingContext.getRetryCount(),
          httpExecutingContext.getMaxRetries(),
          httpExecutingContext.getRetryTimeout(),
          httpExecutingContext.getMinBackoffInMillis(),
          httpExecutingContext.getRequestInfoScrubbed());
      try {
        // update start time
        httpExecutingContext.setStartTimePerRequest(System.currentTimeMillis());

        RestRequest.setRequestURI(
            httpRequest,
            httpExecutingContext.getRequestId(),
            httpExecutingContext.isIncludeRetryParameters(),
            httpExecutingContext.isIncludeRequestGuid(),
            httpExecutingContext.getRetryCount(),
            httpExecutingContext.getLastStatusCodeForRetry(),
            httpExecutingContext.getStartTime(),
            httpExecutingContext.getRequestInfoScrubbed());

        execTimeData.setHttpClientStart();
        CloseableHttpResponse response = httpClient.execute(httpRequest);
        responseDto.setHttpResponse(response);
        execTimeData.setHttpClientEnd();
      } catch (Exception ex) {
        responseDto.setSavedEx(handlingNotRetryableException(ex, httpExecutingContext));
      } finally {
        // Reset the socket timeout to its original value if it is not the
        // very first iteration.
        if (httpExecutingContext.getInjectSocketTimeout() != 0
            && httpExecutingContext.getRetryCount() == 0) {
          // test code path
          httpRequest.setConfig(
              HttpUtil.getDefaultRequestConfigWithSocketTimeout(
                  httpExecutingContext.getOrigSocketTimeout(),
                  httpExecutingContext.isWithoutCookies()));
        }
      }
      boolean shouldSkipRetry =
          shouldSkipRetryWithLoggedReason(httpRequest, responseDto, httpExecutingContext);
      httpExecutingContext.setShouldRetry(!shouldSkipRetry);

      if (httpExecutingContext.isUnpackResponse()
          && responseDto.getHttpResponse() != null
          && responseDto.getHttpResponse().getStatusLine().getStatusCode()
              == 200) { // todo extract getter for statusCode
        processHttpResponse(httpExecutingContext, execTimeData, responseDto);
      }

      if (!httpExecutingContext.isShouldRetry()) {
        if (responseDto.getHttpResponse() == null) {
          if (responseDto.getSavedEx() != null) {
            logger.error(
                "{}Returning null response. Cause: {}, request: {}",
                httpExecutingContext.getRequestId(),
                getRootCause(responseDto.getSavedEx()),
                httpExecutingContext.getRequestInfoScrubbed());
          } else {
            logger.error(
                "{}Returning null response for request: {}",
                httpExecutingContext.getRequestId(),
                httpExecutingContext.getRequestInfoScrubbed());
          }
        } else if (responseDto.getHttpResponse().getStatusLine().getStatusCode() != 200) {
          logger.error(
              "{}Error response: HTTP Response code: {}, request: {}",
              httpExecutingContext.getRequestId(),
              responseDto.getHttpResponse().getStatusLine().getStatusCode(),
              httpExecutingContext.getRequestInfoScrubbed());
          responseDto.setSavedEx(
              new SnowflakeSQLException(
                  SqlState.IO_ERROR,
                  ErrorCode.NETWORK_ERROR.getMessageCode(),
                  "HTTP status="
                      + ((responseDto.getHttpResponse() != null)
                          ? responseDto.getHttpResponse().getStatusLine().getStatusCode()
                          : "null response")));
        } else if ((responseDto.getHttpResponse() == null
            || responseDto.getHttpResponse().getStatusLine().getStatusCode() != 200)) {
          sendTelemetryEvent(
              httpRequest,
              httpExecutingContext,
              responseDto.getHttpResponse(),
              responseDto.getSavedEx());
        }
        break;
      } else {
        prepareRetry(httpRequest, httpExecutingContext, retryManager, responseDto);
      }
    }

    logger.debug(
        "{}Execution of request {} took {} ms with total of {} retries",
        httpExecutingContext.getRequestId(),
        httpExecutingContext.getRequestInfoScrubbed(),
        networkComunnicationStapwatch == null
            ? "n/a"
            : networkComunnicationStapwatch.elapsedMillis(),
        httpExecutingContext.getRetryCount());

    httpExecutingContext.resetRetryCount();
    if (logger.isDebugEnabled() && networkComunnicationStapwatch != null) {
      networkComunnicationStapwatch.stop();
    }
    if (responseDto.getSavedEx() != null) {
      throw new SnowflakeSQLException(
          responseDto.getSavedEx(),
          ErrorCode.NETWORK_ERROR,
          "Exception encountered for HTTP request: " + responseDto.getSavedEx().getMessage());
    }
    return responseDto;
  }

  private static void processHttpResponse(
      HttpExecutingContext httpExecutingContext,
      ExecTimeTelemetryData execTimeData,
      HttpResponseContextDto responseDto) {
    CloseableHttpResponse response = responseDto.getHttpResponse();
    try {
      String responseText;
      responseText = verifyAndUnpackResponse(response, execTimeData);
      httpExecutingContext.setShouldRetry(false);
      responseDto.setUnpackedCloseableHttpResponse(responseText);
    } catch (IOException ex) {
      boolean skipRetriesBecauseOf200 = httpExecutingContext.isSkipRetriesBecauseOf200();
      boolean retryReasonDifferentThan200 =
          !httpExecutingContext.isShouldRetry() && skipRetriesBecauseOf200;
      httpExecutingContext.setShouldRetry(retryReasonDifferentThan200);
      responseDto.setSavedEx(ex);
    }
  }

  private static void updateRetryParameters(
      URIBuilder builder, int retryCount, String lastStatusCodeForRetry, long startTime) {
    builder.setParameter("retryCount", String.valueOf(retryCount));
    builder.setParameter("retryReason", lastStatusCodeForRetry);
    builder.setParameter("clientStartTime", String.valueOf(startTime));
  }

  private static void prepareRetry(
      HttpRequestBase httpRequest,
      HttpExecutingContext httpExecutingContext,
      RetryContextManager retryManager,
      HttpResponseContextDto dto)
      throws SnowflakeSQLException {
    //        Potentially retryable error
    logRequestResult(
        dto.getHttpResponse(),
        httpExecutingContext.getRequestId(),
        httpExecutingContext.getRequestInfoScrubbed(),
        dto.getSavedEx());

    // get the elapsed time for the last request
    // elapsed in millisecond for last call, used for calculating the
    // remaining amount of time to sleep:
    // (backoffInMilli - elapsedMilliForLastCall)
    long elapsedMilliForLastCall =
        System.currentTimeMillis() - httpExecutingContext.getStartTimePerRequest();

    if (httpExecutingContext.socketOrConnectTimeoutReached())
    /* socket timeout not reached */ {
      /* connect timeout not reached */
      // check if this is a login-request
      if (String.valueOf(httpRequest.getURI()).contains("login-request")) {
        throw new SnowflakeSQLException(
            ErrorCode.AUTHENTICATOR_REQUEST_TIMEOUT,
            httpExecutingContext.getRetryCount(),
            true,
            httpExecutingContext.getElapsedMilliForTransientIssues() / 1000);
      }
    }

    // sleep for backoff - elapsed amount of time
    sleepForBackoffAndPrepareNext(elapsedMilliForLastCall, httpExecutingContext);

    httpExecutingContext.incrementRetryCount();
    httpExecutingContext.setLastStatusCodeForRetry(
        dto.getHttpResponse() == null
            ? "0"
            : String.valueOf(dto.getHttpResponse().getStatusLine().getStatusCode()));
    // If the request failed with any other retry-able error and auth timeout is reached
    // increase the retry count and throw special exception to renew the token before retrying.

    RetryContextManager.RetryHook retryManagerHook = null;
    if (retryManager != null) {
      retryManagerHook = retryManager.getRetryHook();
      retryManager
          .getRetryContext()
          .setElapsedTimeInMillis(httpExecutingContext.getElapsedMilliForTransientIssues())
          .setRetryTimeoutInMillis(httpExecutingContext.getRetryTimeoutInMilliseconds());
    }

    // Make sure that any authenticator specific info that needs to be
    // updated gets updated before the next retry. Ex - OKTA OTT, JWT token
    // Aim is to achieve this using RetryContextManager, but raising
    // AUTHENTICATOR_REQUEST_TIMEOUT Exception is still supported as well. In both cases the
    // retried request must be aware of the elapsed time not to exceed the timeout limit.
    if (retryManagerHook == RetryContextManager.RetryHook.ALWAYS_BEFORE_RETRY) {
      retryManager.executeRetryCallbacks(httpRequest);
    }

    if (httpExecutingContext.getAuthTimeout() > 0
        && httpExecutingContext.getElapsedMilliForTransientIssues()
            >= httpExecutingContext.getAuthTimeout()) {
      throw new SnowflakeSQLException(
          ErrorCode.AUTHENTICATOR_REQUEST_TIMEOUT,
          httpExecutingContext.getRetryCount(),
          false,
          httpExecutingContext.getElapsedMilliForTransientIssues() / 1000);
    }

    int numOfRetryToTriggerTelemetry =
        TelemetryService.getInstance().getNumOfRetryToTriggerTelemetry();
    if (httpExecutingContext.getRetryCount() == numOfRetryToTriggerTelemetry) {
      TelemetryService.getInstance()
          .logHttpRequestTelemetryEvent(
              String.format("HttpRequestRetry%dTimes", numOfRetryToTriggerTelemetry),
              httpRequest,
              httpExecutingContext.getInjectSocketTimeout(),
              httpExecutingContext.getCanceling(),
              httpExecutingContext.isWithoutCookies(),
              httpExecutingContext.isIncludeRetryParameters(),
              httpExecutingContext.isIncludeRequestGuid(),
              dto.getHttpResponse(),
              dto.getSavedEx(),
              httpExecutingContext.getBreakRetryReason(),
              httpExecutingContext.getRetryTimeout(),
              httpExecutingContext.getRetryCount(),
              SqlState.IO_ERROR,
              ErrorCode.NETWORK_ERROR.getMessageCode());
    }
    dto.setSavedEx(null);
    httpExecutingContext.setSkipRetriesBecauseOf200(false);

    // release connection before retry
    httpRequest.releaseConnection();
  }

  private static void sendTelemetryEvent(
      HttpRequestBase httpRequest,
      HttpExecutingContext httpExecutingContext,
      CloseableHttpResponse response,
      Exception savedEx) {
    String eventName;
    if (response == null) {
      eventName = "NullResponseHttpError";
    } else {
      if (response.getStatusLine() == null) {
        eventName = "NullResponseStatusLine";
      } else {
        eventName = String.format("HttpError%d", response.getStatusLine().getStatusCode());
      }
    }
    TelemetryService.getInstance()
        .logHttpRequestTelemetryEvent(
            eventName,
            httpRequest,
            httpExecutingContext.getInjectSocketTimeout(),
            httpExecutingContext.getCanceling(),
            httpExecutingContext.isWithoutCookies(),
            httpExecutingContext.isIncludeRetryParameters(),
            httpExecutingContext.isIncludeRequestGuid(),
            response,
            savedEx,
            httpExecutingContext.getBreakRetryReason(),
            httpExecutingContext.getRetryTimeout(),
            httpExecutingContext.getRetryCount(),
            null,
            0);
  }

  private static void sleepForBackoffAndPrepareNext(
      long elapsedMilliForLastCall, HttpExecutingContext context) {
    if (context.getMinBackoffInMillis() > elapsedMilliForLastCall) {
      try {
        logger.debug(
            "{}Retry request {}: sleeping for {} ms",
            context.getRequestId(),
            context.getRequestInfoScrubbed(),
            context.getBackoffInMillis());
        Thread.sleep(context.getBackoffInMillis());
      } catch (InterruptedException ex1) {
        logger.debug(
            "{}Backoff sleep before retrying login got interrupted", context.getRequestId());
      }
      context.increaseElapsedMilliForTransientIssues(context.getBackoffInMillis());
      context.setBackoffInMillis(
          getNewBackoffInMilli(
              context.getBackoffInMillis(),
              context.isLoginRequest(),
              context.getBackoff(),
              context.getRetryCount(),
              context.getRetryTimeoutInMilliseconds(),
              context.getElapsedMilliForTransientIssues()));
    }
  }

  private static void logRequestResult(
      CloseableHttpResponse response,
      String requestIdStr,
      String requestInfoScrubbed,
      Exception savedEx) {
    if (response != null) {
      logger.debug(
          "{}HTTP response not ok: status code: {}, request: {}",
          requestIdStr,
          response.getStatusLine().getStatusCode(),
          requestInfoScrubbed);
    } else if (savedEx != null) {
      logger.debug(
          "{}Null response for cause: {}, request: {}",
          requestIdStr,
          getRootCause(savedEx).getMessage(),
          requestInfoScrubbed);
    } else {
      logger.debug("{}Null response for request: {}", requestIdStr, requestInfoScrubbed);
    }
  }

  private static void checkForDPoPNonceError(CloseableHttpResponse response) throws IOException {
    String errorResponse = EntityUtils.toString(response.getEntity());
    if (!isNullOrEmpty(errorResponse)) {
      ObjectMapper objectMapper = ObjectMapperFactory.getObjectMapper();
      JsonNode rootNode = objectMapper.readTree(errorResponse);
      JsonNode errorNode = rootNode.get(ERROR_FIELD_NAME);
      if (errorNode != null
          && errorNode.isValueNode()
          && errorNode.isTextual()
          && errorNode.textValue().equals(ERROR_USE_DPOP_NONCE)) {
        throw new SnowflakeUseDPoPNonceException(
            response.getFirstHeader(DPOP_NONCE_HEADER_NAME).getValue());
      }
    }
  }

  private static Exception handlingNotRetryableException(
      Exception ex, HttpExecutingContext httpExecutingContext) throws SnowflakeSQLLoggedException {
    Set<Class<?>> sslExceptions = new HashSet<>();
    sslExceptions.add(SSLHandshakeException.class);
    sslExceptions.add(SSLKeyException.class);
    sslExceptions.add(SSLPeerUnverifiedException.class);
    sslExceptions.add(SSLProtocolException.class);
    Exception savedEx = null;
    if (ex instanceof IllegalStateException) {
      throw new SnowflakeSQLLoggedException(
          null, ErrorCode.INVALID_STATE, ex, /* session= */ ex.getMessage());
    } else if (isExceptionInGroup(ex, sslExceptions)) {
      String formattedMsg =
          ex.getMessage()
              + "\n"
              + "Verify that the hostnames and portnumbers in SYSTEM$ALLOWLIST are added to your firewall's allowed list.\n"
              + "To troubleshoot your connection further, you can refer to this article:\n"
              + "https://docs.snowflake.com/en/user-guide/client-connectivity-troubleshooting/overview";

      throw new SnowflakeSQLLoggedException(null, ErrorCode.NETWORK_ERROR, ex, formattedMsg);
    } else if (ex instanceof Exception) {
      savedEx = ex;
      // if the request took more than socket timeout log a warning
      long currentMillis = System.currentTimeMillis();
      if ((currentMillis - httpExecutingContext.getStartTimePerRequest())
          > HttpUtil.getSocketTimeout().toMillis()) {
        logger.warn(
            "{}HTTP request took longer than socket timeout {} ms: {} ms",
            httpExecutingContext.getRequestId(),
            HttpUtil.getSocketTimeout().toMillis(),
            (currentMillis - httpExecutingContext.getStartTimePerRequest()));
      }
      StringWriter sw = new StringWriter();
      savedEx.printStackTrace(new PrintWriter(sw));
      logger.debug(
          "{}Exception encountered for: {}, {}, {}",
          httpExecutingContext.getRequestId(),
          httpExecutingContext.getRequestInfoScrubbed(),
          ex.getLocalizedMessage(),
          (ArgSupplier) sw::toString);
    }
    return ex;
  }

  private static boolean isExceptionInGroup(Exception e, Set<Class<?>> group) {
    for (Class<?> clazz : group) {
      if (clazz.isInstance(e)) {
        return true;
      }
    }
    return false;
  }

  private static boolean handleCertificateRevoked(
      Exception savedEx, HttpExecutingContext httpExecutingContext, boolean skipRetrying) {
    if (!skipRetrying && RestRequest.isCertificateRevoked(savedEx)) {
      String msg = "Unknown reason";
      Throwable rootCause = RestRequest.getRootCause(savedEx);
      msg =
          rootCause.getMessage() != null && !rootCause.getMessage().isEmpty()
              ? rootCause.getMessage()
              : msg;
      logger.debug(
          "{}Error response not retryable, " + msg + ", request: {}",
          httpExecutingContext.getRequestId(),
          httpExecutingContext.getRequestInfoScrubbed());
      EventUtil.triggerBasicEvent(
          Event.EventType.NETWORK_ERROR,
          msg + ", Request: " + httpExecutingContext.getRequestInfoScrubbed(),
          false);

      httpExecutingContext.setBreakRetryReason("certificate revoked error");
      httpExecutingContext.setBreakRetryEventNam("HttpRequestRetryVertificateRevoked");
      httpExecutingContext.setShouldRetry(false);
      return true;
    }
    return skipRetrying;
  }

  private static boolean handleNoRetryiableHttpCode(
      HttpResponseContextDto dto, HttpExecutingContext httpExecutingContext, boolean skipRetrying) {
    CloseableHttpResponse response = dto.getHttpResponse();
    if (!skipRetrying && isNonRetryableHTTPCode(response, httpExecutingContext.isRetryHTTP403())) {
      String msg = "Unknown reason";
      if (response != null) {
        logger.debug(
            "{}HTTP response code for request {}: {}",
            httpExecutingContext.getRequestId(),
            httpExecutingContext.getRequestInfoScrubbed(),
            response.getStatusLine().getStatusCode());
        msg =
            "StatusCode: "
                + response.getStatusLine().getStatusCode()
                + ", Reason: "
                + response.getStatusLine().getReasonPhrase();
      } else if (dto.getSavedEx() != null) // may be null.
      {
        Throwable rootCause = RestRequest.getRootCause(dto.getSavedEx());
        msg = rootCause.getMessage();
      }

      if (response == null || response.getStatusLine().getStatusCode() != 200) {
        logger.debug(
            "{}Error response not retryable, " + msg + ", request: {}",
            httpExecutingContext.getRequestId(),
            httpExecutingContext.getRequestInfoScrubbed());
        EventUtil.triggerBasicEvent(
            Event.EventType.NETWORK_ERROR,
            msg + ", Request: " + httpExecutingContext.getRequestInfoScrubbed(),
            false);
      }
      httpExecutingContext.setBreakRetryReason("status code does not need retry");
      //            httpExecutingContext.resetRetryCount();
      httpExecutingContext.setShouldRetry(false);
      skipRetrying = true;
      httpExecutingContext.setSkipRetriesBecauseOf200(
          response.getStatusLine().getStatusCode() == 200);

      logger.error("Error executing request: {}", httpExecutingContext.getRequestInfoScrubbed());

      try {
        if (response == null || response.getStatusLine().getStatusCode() != 200) {
          logger.error(
              "Error executing request: {}", httpExecutingContext.getRequestInfoScrubbed());

          if (response != null
              && response.getStatusLine().getStatusCode() == 400
              && response.getEntity() != null) {
            checkForDPoPNonceError(response);
          }

          SnowflakeUtil.logResponseDetails(response, logger);

          if (response != null) {
            EntityUtils.consume(response.getEntity());
          }

          //           We throw here exception if timeout was reached for login
          dto.setSavedEx(
              new SnowflakeSQLException(
                  SqlState.IO_ERROR,
                  ErrorCode.NETWORK_ERROR.getMessageCode(),
                  "HTTP status="
                      + ((response != null)
                          ? response.getStatusLine().getStatusCode()
                          : "null response")));
        }
      } catch (IOException e) {
        dto.setSavedEx(
            new SnowflakeSQLException(
                SqlState.IO_ERROR,
                ErrorCode.NETWORK_ERROR.getMessageCode(),
                "Exception details: " + e.getMessage()));
      }
    }
    return skipRetrying;
  }

  private static void logTelemetryEvent(
      HttpRequestBase request,
      CloseableHttpResponse response,
      Exception savedEx,
      HttpExecutingContext httpExecutingContext) {
    TelemetryService.getInstance()
        .logHttpRequestTelemetryEvent(
            httpExecutingContext.getBreakRetryEventNam(),
            request,
            httpExecutingContext.getInjectSocketTimeout(),
            httpExecutingContext.getCanceling(),
            httpExecutingContext.isWithoutCookies(),
            httpExecutingContext.isIncludeRetryParameters(),
            httpExecutingContext.isIncludeRequestGuid(),
            response,
            savedEx,
            httpExecutingContext.getBreakRetryReason(),
            httpExecutingContext.getRetryTimeout(),
            httpExecutingContext.getRetryCount(),
            SqlState.IO_ERROR,
            ErrorCode.NETWORK_ERROR.getMessageCode());
  }

  private static boolean handleMaxRetriesExceeded(
      HttpExecutingContext httpExecutingContext, boolean skipRetrying) {
    if (!skipRetrying && httpExecutingContext.maxRetriesExceeded()) {
      logger.error(
          "{}Stop retrying as max retries have been reached for request: {}! Max retry count: {}",
          httpExecutingContext.getRequestId(),
          httpExecutingContext.getRequestInfoScrubbed(),
          httpExecutingContext.getMaxRetries());

      httpExecutingContext.setBreakRetryReason("max retries reached");
      httpExecutingContext.setBreakRetryEventNam("HttpRequestRetryLimitExceeded");
      httpExecutingContext.setShouldRetry(false);
      return true;
    }
    return skipRetrying;
  }

  private static boolean handleElapsedTimeoutExceeded(
      HttpExecutingContext httpExecutingContext, boolean skipRetrying) {
    if (!skipRetrying && httpExecutingContext.getRetryTimeoutInMilliseconds() > 0) {
      // Check for retry time-out.
      // increment total elapsed due to transient issues
      long elapsedMilliForLastCall =
          System.currentTimeMillis() - httpExecutingContext.getStartTimePerRequest();
      httpExecutingContext.increaseElapsedMilliForTransientIssues(elapsedMilliForLastCall);

      // check if the total elapsed time for transient issues has exceeded
      // the retry timeout and we retry at least the min, if so, we will not
      // retry
      if (httpExecutingContext.elapsedTimeExceeded() && httpExecutingContext.moreThanMinRetries()) {
        logger.error(
            "{}Stop retrying since elapsed time due to network "
                + "issues has reached timeout. "
                + "Elapsed: {} ms, timeout: {} ms",
            httpExecutingContext.getRequestId(),
            httpExecutingContext.getElapsedMilliForTransientIssues(),
            httpExecutingContext.getRetryTimeoutInMilliseconds());

        httpExecutingContext.setBreakRetryReason("retry timeout");
        httpExecutingContext.setBreakRetryEventNam("HttpRequestRetryTimeout");
        httpExecutingContext.setShouldRetry(false);
        return true;
      }
    }
    return skipRetrying;
  }

  private static boolean handleCancelingSignal(
      HttpExecutingContext httpExecutingContext, boolean skipRetrying) {
    if (!skipRetrying
        && httpExecutingContext.getCanceling() != null
        && httpExecutingContext.getCanceling().get()) {
      logger.debug(
          "{}Stop retrying since canceling is requested", httpExecutingContext.getRequestId());
      httpExecutingContext.setBreakRetryReason("canceling is requested");
      httpExecutingContext.setShouldRetry(false);
      return true;
    }
    return skipRetrying;
  }

  private static boolean handleNoRetryFlag(
      HttpExecutingContext httpExecutingContext, boolean skipRetrying) {
    if (!skipRetrying && httpExecutingContext.isNoRetry()) {
      logger.debug(
          "{}HTTP retry disabled for this request. noRetry: {}",
          httpExecutingContext.getRequestId(),
          httpExecutingContext.isNoRetry());
      httpExecutingContext.setBreakRetryReason("retry is disabled");
      httpExecutingContext.resetRetryCount();
      httpExecutingContext.setShouldRetry(false);
      return true;
    }
    return skipRetrying;
  }

  private static boolean shouldSkipRetryWithLoggedReason(
      HttpRequestBase request,
      HttpResponseContextDto responseDto,
      HttpExecutingContext httpExecutingContext) {
    CloseableHttpResponse response = responseDto.getHttpResponse();
    Exception savedEx = responseDto.getSavedEx();
    List<Function<Boolean, Boolean>> conditions =
        Arrays.asList(
            skipRetrying -> handleNoRetryFlag(httpExecutingContext, skipRetrying),
            skipRetrying -> handleCancelingSignal(httpExecutingContext, skipRetrying),
            skipRetrying -> handleElapsedTimeoutExceeded(httpExecutingContext, skipRetrying),
            skipRetrying -> handleMaxRetriesExceeded(httpExecutingContext, skipRetrying),
            skipRetrying -> handleCertificateRevoked(savedEx, httpExecutingContext, skipRetrying),
            skipRetrying ->
                handleNoRetryiableHttpCode(responseDto, httpExecutingContext, skipRetrying));

    // Process each condition using Stream
    boolean skipRetrying =
        conditions.stream().reduce(Function::andThen).orElse(Function.identity()).apply(false);

    // Log telemetry
    logTelemetryEvent(request, response, savedEx, httpExecutingContext);

    return skipRetrying;
  }

  private static String verifyAndUnpackResponse(
      CloseableHttpResponse response, ExecTimeTelemetryData execTimeData) throws IOException {
    try (StringWriter writer = new StringWriter()) {
      execTimeData.setResponseIOStreamStart();
      try (InputStream ins = response.getEntity().getContent()) {
        IOUtils.copy(ins, writer, "UTF-8");
      }

      execTimeData.setResponseIOStreamEnd();
      return writer.toString();
    } finally {
      IOUtils.closeQuietly(response);
    }
  }
}
