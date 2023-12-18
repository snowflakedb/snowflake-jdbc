/*
 * Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLKeyException;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLProtocolException;
import net.snowflake.client.core.*;
import net.snowflake.client.jdbc.telemetryOOB.TelemetryService;
import net.snowflake.client.log.ArgSupplier;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.client.util.DecorrelatedJitterBackoff;
import net.snowflake.client.util.SecretDetector;
import net.snowflake.common.core.SqlState;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;

/**
 * This is an abstraction on top of http client.
 *
 * <p>Currently it only has one method for retrying http request execution so that the same logic
 * doesn't have to be replicated at difference places where retry is needed.
 *
 * @author jhuang
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
        execTimeTelemetryData);
  }

  /**
   * Execute an http request with retry logic.
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
    CloseableHttpResponse response = null;

    String requestInfoScrubbed = SecretDetector.maskSASToken(httpRequest.toString());

    // time the client started attempting to submit request
    final long startTime = System.currentTimeMillis();

    // start time for each request,
    // used for keeping track how much time we have spent
    // due to network issues so that we can compare against the user
    // specified network timeout to make sure we do not retry infinitely
    // when there are transient network/GS issues.
    long startTimePerRequest = startTime;

    // Used to indicate that this is a login/auth request and will be using the new retry strategy.
    boolean isLoginRequest = SessionUtil.isNewRetryStrategyRequest(httpRequest);

    // total elapsed time due to transient issues.
    long elapsedMilliForTransientIssues = 0;

    // retry timeout (ms)
    long retryTimeoutInMilliseconds = retryTimeout * 1000;

    // amount of time to wait for backing off before retry
    long backoffInMilli = minBackoffInMilli;

    // auth timeout (ms)
    long authTimeoutInMilli = authTimeout * 1000;

    DecorrelatedJitterBackoff backoff =
        new DecorrelatedJitterBackoff(backoffInMilli, maxBackoffInMilli);

    int origSocketTimeout = 0;

    Exception savedEx = null;

    // label the reason to break retry
    String breakRetryReason = "";

    String lastStatusCodeForRetry = "";

    int retryCount = 0;

    // try request till we get a good response or retry timeout
    while (true) {
      logger.debug("Retry count: {}", retryCount);
      logger.debug("Attempting request: {}", requestInfoScrubbed);

      try {
        // update start time
        startTimePerRequest = System.currentTimeMillis();

        if (withoutCookies) {
          httpRequest.setConfig(HttpUtil.getRequestConfigWithoutCookies());
        }

        // for first call, simulate a socket timeout by setting socket timeout
        // to the injected socket timeout value
        if (injectSocketTimeout != 0 && retryCount == 0) {
          // test code path
          logger.debug(
              "Injecting socket timeout by setting " + "socket timeout to {} millisecond ",
              injectSocketTimeout);
          httpRequest.setConfig(
              HttpUtil.getDefaultRequestConfigWithSocketTimeout(
                  injectSocketTimeout, withoutCookies));
        }

        /*
         * Add retryCount if the first request failed
         * GS can uses the parameter for optimization. Specifically GS
         * will only check metadata database to see if a query has been running
         * for a retry request. This way for the majority of query requests
         * which are not part of retry we don't have to pay the performance
         * overhead of looking up in metadata database.
         */
        URIBuilder builder = new URIBuilder(httpRequest.getURI());
        // If HTAP
        if ("true".equalsIgnoreCase(System.getenv("HTAP_SIMULATION"))
            && builder.getPathSegments().contains("query-request")) {
          builder.setParameter("target", "htap_simulation");
        }
        if (includeRetryParameters && retryCount > 0) {
          builder.setParameter("retryCount", String.valueOf(retryCount));
          builder.setParameter("retryReason", lastStatusCodeForRetry);
          builder.setParameter("clientStartTime", String.valueOf(startTime));
        }

        // When the auth timeout is set, set the socket timeout as the authTimeout
        // so that it can be renewed in time and pass it to the http request configuration.
        if (authTimeout > 0) {
          int requestSocketAndConnectTimeout = (int) authTimeout * 1000;
          httpRequest.setConfig(
              HttpUtil.getDefaultRequestConfigWithSocketAndConnectTimeout(
                  requestSocketAndConnectTimeout, withoutCookies));
        }

        if (includeRequestGuid) {
          // Add request_guid for better tracing
          builder.setParameter(SF_REQUEST_GUID, UUIDUtils.getUUID().toString());
        }

        httpRequest.setURI(builder.build());
        execTimeData.setHttpClientStart();
        response = httpClient.execute(httpRequest);
        execTimeData.setHttpClientEnd();
      } catch (IllegalStateException ex) {
        // if exception is caused by illegal state, e.g shutdown of http client
        // because of closing of connection, then fail immediately and stop retrying.
        throw new SnowflakeSQLLoggedException(
            null, ErrorCode.INVALID_STATE, ex, /* session = */ ex.getMessage());

      } catch (SSLHandshakeException
          | SSLKeyException
          | SSLPeerUnverifiedException
          | SSLProtocolException ex) {
        // if an SSL issue occurs like an SSLHandshakeException then fail
        // immediately and stop retrying the requests

        throw new SnowflakeSQLLoggedException(null, ErrorCode.NETWORK_ERROR, ex, ex.getMessage());

      } catch (Exception ex) {

        savedEx = ex;
        // if the request took more than 5 min (socket timeout) log an error
        if ((System.currentTimeMillis() - startTimePerRequest) > 300000) {
          logger.warn(
              "HTTP request took longer than 5 min: {} sec",
              (System.currentTimeMillis() - startTimePerRequest) / 1000);
        }
        StringWriter sw = new StringWriter();
        savedEx.printStackTrace(new PrintWriter(sw));
        logger.debug(
            "Exception encountered for: {}, {}, {}",
            requestInfoScrubbed,
            ex.getLocalizedMessage(),
            (ArgSupplier) sw::toString);

      } finally {
        // Reset the socket timeout to its original value if it is not the
        // very first iteration.
        if (injectSocketTimeout != 0 && retryCount == 0) {
          // test code path
          httpRequest.setConfig(
              HttpUtil.getDefaultRequestConfigWithSocketTimeout(origSocketTimeout, withoutCookies));
        }
      }

      /*
       * If we got a response and the status code is not one of those
       * transient failures, no more retry
       */
      if (noRetry
          || isCertificateRevoked(savedEx)
          || isNonRetryableHTTPCode(response, retryHTTP403)) {
        String msg = "Unknown cause";
        if (response != null) {
          logger.debug("HTTP response code: {}", response.getStatusLine().getStatusCode());
          msg =
              "StatusCode: "
                  + response.getStatusLine().getStatusCode()
                  + ", Reason: "
                  + response.getStatusLine().getReasonPhrase();
        } else if (savedEx != null) // may be null.
        {
          Throwable rootCause = getRootCause(savedEx);
          msg = rootCause.getMessage();
        }

        if (response == null || response.getStatusLine().getStatusCode() != 200) {
          logger.debug(
              "Error response not retryable, " + msg + ", request: {}", requestInfoScrubbed);
          EventUtil.triggerBasicEvent(
              Event.EventType.NETWORK_ERROR, msg + ", Request: " + httpRequest.toString(), false);
        }
        breakRetryReason = "status code does not need retry";
        if (noRetry) {
          logger.debug("HTTP retry disabled for this request. noRetry: {}", noRetry);
          breakRetryReason = "retry is disabled";
        }

        // reset retryCount
        retryCount = 0;
        break;
      } else {
        if (response != null) {
          logger.debug(
              "HTTP response not ok: status code: {}, request: {}",
              response.getStatusLine().getStatusCode(),
              requestInfoScrubbed);
        } else if (savedEx != null) {
          logger.debug(
              "Null response for cause: {}, request: {}",
              getRootCause(savedEx).getMessage(),
              requestInfoScrubbed);
        } else {
          logger.debug("Null response for request: {}", requestInfoScrubbed);
        }

        // get the elapsed time for the last request
        // elapsed in millisecond for last call, used for calculating the
        // remaining amount of time to sleep:
        // (backoffInMilli - elapsedMilliForLastCall)
        long elapsedMilliForLastCall = System.currentTimeMillis() - startTimePerRequest;

        // check canceling flag
        if (canceling != null && canceling.get()) {
          logger.debug("Stop retrying since canceling is requested", false);
          breakRetryReason = "canceling is requested";
          break;
        }

        String breakRetryEventName = "";

        if (retryTimeoutInMilliseconds > 0) {
          // Check for retry time-out.
          // increment total elapsed due to transient issues
          elapsedMilliForTransientIssues += elapsedMilliForLastCall;

          // check if the total elapsed time for transient issues has exceeded
          // the retry timeout and we retry at least the min, if so, we will not
          // retry
          if (elapsedMilliForTransientIssues > retryTimeoutInMilliseconds
              && retryCount >= MIN_RETRY_COUNT) {
            logger.error(
                "Stop retrying since elapsed time due to network "
                    + "issues has reached timeout. "
                    + "Elapsed: {}(ms), timeout: {}(ms)",
                elapsedMilliForTransientIssues,
                retryTimeoutInMilliseconds);

            breakRetryReason = "retry timeout";
            breakRetryEventName = "HttpRequestRetryTimeout";
          }
        }
        if (maxRetries > 0 && retryCount > maxRetries) {
          // check for max retries.
          logger.error(
              "Stop retrying as max retries have been reached! max retry count: {}", maxRetries);
          breakRetryReason = "max retries reached";
          breakRetryEventName = "HttpRequestRetryLimitExceeded";
        }

        if (breakRetryEventName != "" && !breakRetryEventName.isEmpty()) {
          // If either of network timeout is exhausted or max retries have been reached, stop
          // retrying!
          TelemetryService.getInstance()
              .logHttpRequestTelemetryEvent(
                  breakRetryEventName,
                  httpRequest,
                  injectSocketTimeout,
                  canceling,
                  withoutCookies,
                  includeRetryParameters,
                  includeRequestGuid,
                  response,
                  savedEx,
                  breakRetryReason,
                  retryTimeout,
                  retryCount,
                  SqlState.IO_ERROR,
                  ErrorCode.NETWORK_ERROR.getMessageCode());
          // rethrow the timeout exception
          if (response == null && savedEx != null) {
            throw new SnowflakeSQLException(
                savedEx,
                ErrorCode.NETWORK_ERROR,
                "Exception encountered for HTTP request: " + savedEx.getMessage());
          }
          // no more retry
          // reset state
          retryCount = 0;
          break;
        }

        // Make sure that any authenticator specific info that needs to be
        // updated get's updated before the next retry. Ex - JWT token
        // Check to see if customer set socket/connect timeout has been reached,
        // if not we don't increase the retry count since JWT renew doesn't count as a retry
        // attempt.
        if (authTimeout > 0
            && elapsedMilliForTransientIssues > authTimeoutInMilli
            && (socketTimeout == 0
                || elapsedMilliForTransientIssues
                    < socketTimeout)) /* socket timeout not reached */ {
          /* connect timeout not reached */
          // check if this is a login-request
          if (String.valueOf(httpRequest.getURI()).contains("login-request")) {
            throw new SnowflakeSQLException(
                ErrorCode.AUTHENTICATOR_REQUEST_TIMEOUT,
                retryCount,
                true,
                elapsedMilliForTransientIssues / 1000);
          }
        }

        // sleep for backoff - elapsed amount of time
        if (backoffInMilli > elapsedMilliForLastCall) {
          try {
            logger.debug("sleeping in {}(ms)", backoffInMilli);
            Thread.sleep(backoffInMilli);
            elapsedMilliForTransientIssues += backoffInMilli;
            if (isLoginRequest) {
              long jitteredBackoffInMilli = backoff.getJitterForLogin(backoffInMilli);
              backoffInMilli =
                  (long)
                      backoff.chooseRandom(
                          jitteredBackoffInMilli + backoffInMilli,
                          Math.pow(2, retryCount) + jitteredBackoffInMilli);
            } else {
              backoffInMilli = backoff.nextSleepTime(backoffInMilli);
            }
            if (retryTimeoutInMilliseconds > 0
                && (elapsedMilliForTransientIssues + backoffInMilli) > retryTimeoutInMilliseconds) {
              // If the timeout will be reached before the next backoff, just use the remaining
              // time.
              backoffInMilli =
                  Math.min(
                      backoffInMilli, retryTimeoutInMilliseconds - elapsedMilliForTransientIssues);
            }
          } catch (InterruptedException ex1) {
            logger.debug("Backoff sleep before retrying login got interrupted", false);
          }
        }

        retryCount++;
        lastStatusCodeForRetry =
            response == null ? "0" : String.valueOf(response.getStatusLine().getStatusCode());
        // If the request failed with any other retry-able error and auth timeout is reached
        // increase the retry count and throw special exception to renew the token before retrying.
        if (authTimeout > 0) {
          if (elapsedMilliForTransientIssues >= authTimeoutInMilli) {
            throw new SnowflakeSQLException(
                ErrorCode.AUTHENTICATOR_REQUEST_TIMEOUT,
                retryCount,
                false,
                elapsedMilliForTransientIssues / 1000);
          }
        }

        int numOfRetryToTriggerTelemetry =
            TelemetryService.getInstance().getNumOfRetryToTriggerTelemetry();
        if (retryCount == numOfRetryToTriggerTelemetry) {
          TelemetryService.getInstance()
              .logHttpRequestTelemetryEvent(
                  String.format("HttpRequestRetry%dTimes", numOfRetryToTriggerTelemetry),
                  httpRequest,
                  injectSocketTimeout,
                  canceling,
                  withoutCookies,
                  includeRetryParameters,
                  includeRequestGuid,
                  response,
                  savedEx,
                  breakRetryReason,
                  retryTimeout,
                  retryCount,
                  SqlState.IO_ERROR,
                  ErrorCode.NETWORK_ERROR.getMessageCode());
        }
        savedEx = null;

        // release connection before retry
        httpRequest.releaseConnection();
      }
    }

    if (response == null) {
      if (savedEx != null) {
        logger.error(
            "Returning null response: cause: {}, request: {}",
            getRootCause(savedEx),
            requestInfoScrubbed);
      } else {
        logger.error("Returning null response for request: {}", requestInfoScrubbed);
      }
    } else if (response.getStatusLine().getStatusCode() != 200) {
      logger.error(
          "Error response: HTTP Response code: {}, request: {}",
          response.getStatusLine().getStatusCode(),
          requestInfoScrubbed);
    }
    if ((response == null || response.getStatusLine().getStatusCode() != 200)) {

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
              injectSocketTimeout,
              canceling,
              withoutCookies,
              includeRetryParameters,
              includeRequestGuid,
              response,
              savedEx,
              breakRetryReason,
              retryTimeout,
              retryCount,
              null,
              0);

      // rethrow the timeout exception
      if (response == null && savedEx != null) {
        throw new SnowflakeSQLException(
            savedEx,
            ErrorCode.NETWORK_ERROR,
            "Exception encountered for HTTP request: " + savedEx.getMessage());
      }
    }

    return response;
  }

  static boolean isNonRetryableHTTPCode(CloseableHttpResponse response, boolean retryHTTP403) {
    return response != null
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
}
