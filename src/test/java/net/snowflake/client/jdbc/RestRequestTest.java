package net.snowflake.client.jdbc;

import static net.snowflake.client.AssumptionUtils.assumeRunningOnLinuxMac;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import net.snowflake.client.core.ExecTimeTelemetryData;
import net.snowflake.client.core.HttpUtil;
import net.snowflake.client.jdbc.telemetryOOB.TelemetryService;
import net.snowflake.client.util.DecorrelatedJitterBackoff;
import org.apache.http.StatusLine;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/** RestRequest unit tests. */
public class RestRequestTest {

  static final int DEFAULT_CONNECTION_TIMEOUT = 300000;
  static final int DEFAULT_HTTP_CLIENT_SOCKET_TIMEOUT = 300000; // ms

  private CloseableHttpResponse retryResponse() {
    StatusLine retryStatusLine = mock(StatusLine.class);
    when(retryStatusLine.getStatusCode()).thenReturn(503);

    CloseableHttpResponse retryResponse = mock(CloseableHttpResponse.class);
    when(retryResponse.getStatusLine()).thenReturn(retryStatusLine);

    return retryResponse;
  }

  private CloseableHttpResponse retryLoginResponse() {
    StatusLine retryStatusLine = mock(StatusLine.class);
    when(retryStatusLine.getStatusCode()).thenReturn(429);

    CloseableHttpResponse retryResponse = mock(CloseableHttpResponse.class);
    when(retryResponse.getStatusLine()).thenReturn(retryStatusLine);

    return retryResponse;
  }

  private CloseableHttpResponse successResponse() {
    StatusLine successStatusLine = mock(StatusLine.class);
    when(successStatusLine.getStatusCode()).thenReturn(200);

    CloseableHttpResponse successResponse = mock(CloseableHttpResponse.class);
    when(successResponse.getStatusLine()).thenReturn(successStatusLine);

    return successResponse;
  }

  private CloseableHttpResponse socketTimeoutResponse() throws SocketTimeoutException {
    StatusLine successStatusLine = mock(StatusLine.class);
    when(successStatusLine.getStatusCode()).thenThrow(new SocketTimeoutException("Read timed out"));

    CloseableHttpResponse successResponse = mock(CloseableHttpResponse.class);
    when(successStatusLine.getStatusCode()).thenThrow(new SocketTimeoutException("Read timed out"));

    return successResponse;
  }

  private void execute(
      CloseableHttpClient client,
      String uri,
      int retryTimeout,
      int authTimeout,
      int socketTimeout,
      boolean includeRetryParameters,
      boolean noRetry)
      throws IOException, SnowflakeSQLException {

    this.execute(
        client, uri, retryTimeout, authTimeout, socketTimeout, includeRetryParameters, noRetry, 0);
  }

  private void execute(
      CloseableHttpClient client,
      String uri,
      int retryTimeout,
      int authTimeout,
      int socketTimeout,
      boolean includeRetryParameters,
      boolean noRetry,
      int maxRetries)
      throws IOException, SnowflakeSQLException {

    RequestConfig.Builder builder =
        RequestConfig.custom()
            .setConnectTimeout(DEFAULT_CONNECTION_TIMEOUT)
            .setConnectionRequestTimeout(DEFAULT_CONNECTION_TIMEOUT)
            .setSocketTimeout(DEFAULT_HTTP_CLIENT_SOCKET_TIMEOUT);
    RequestConfig defaultRequestConfig = builder.build();
    HttpUtil util = new HttpUtil();
    util.setRequestConfig(defaultRequestConfig);

    RestRequest.execute(
        client,
        new HttpGet(uri),
        retryTimeout, // retry timeout
        authTimeout,
        socketTimeout,
        maxRetries,
        0, // inject socket timeout
        new AtomicBoolean(false), // canceling
        false, // without cookie
        includeRetryParameters,
        true,
        true,
        noRetry,
        new ExecTimeTelemetryData());
  }

  @Test
  public void testRetryParamsInRequest() throws IOException, SnowflakeSQLException {
    CloseableHttpClient client = mock(CloseableHttpClient.class);
    when(client.execute(any(HttpUriRequest.class)))
        .thenAnswer(
            new Answer<CloseableHttpResponse>() {
              int callCount = 0;

              @Override
              public CloseableHttpResponse answer(InvocationOnMock invocation) throws Throwable {
                HttpUriRequest arg = (HttpUriRequest) invocation.getArguments()[0];
                String params = arg.getURI().getQuery();

                if (callCount == 0) {
                  assertFalse(params.contains("retryCount="));
                  assertFalse(params.contains("retryReason="));
                  assertFalse(params.contains("clientStartTime="));
                  assertTrue(params.contains("request_guid="));
                } else {
                  assertTrue(params.contains("retryCount=" + callCount));
                  assertTrue(params.contains("retryReason=503"));
                  assertTrue(params.contains("clientStartTime="));
                  assertTrue(params.contains("request_guid="));
                }

                callCount += 1;
                if (callCount >= 3) {
                  return successResponse();
                } else {
                  return retryResponse();
                }
              }
            });

    execute(client, "fakeurl.com/?requestId=abcd-1234", 0, 0, 0, true, false);
  }

  @Test
  public void testRetryNoParamsInRequest() throws IOException, SnowflakeSQLException {
    CloseableHttpClient client = mock(CloseableHttpClient.class);
    when(client.execute(any(HttpUriRequest.class)))
        .thenAnswer(
            new Answer<CloseableHttpResponse>() {
              int callCount = 0;

              @Override
              public CloseableHttpResponse answer(InvocationOnMock invocation) throws Throwable {
                HttpUriRequest arg = (HttpUriRequest) invocation.getArguments()[0];
                String params = arg.getURI().getQuery();

                assertFalse(params.contains("retryCount="));
                assertFalse(params.contains("retryReason="));
                assertFalse(params.contains("clientStartTime="));
                assertTrue(params.contains("request_guid="));

                callCount += 1;
                if (callCount >= 3) {
                  return successResponse();
                } else {
                  return retryResponse();
                }
              }
            });

    execute(client, "fakeurl.com/?requestId=abcd-1234", 0, 0, 0, false, false);
  }

  private CloseableHttpResponse anyStatusCodeResponse(int statusCode) {
    StatusLine successStatusLine = mock(StatusLine.class);
    when(successStatusLine.getStatusCode()).thenReturn(statusCode);

    CloseableHttpResponse response = mock(CloseableHttpResponse.class);
    when(response.getStatusLine()).thenReturn(successStatusLine);

    return response;
  }

  @Test
  public void testIsNonRetryableHTTPCode() throws Exception {
    class TestCase {
      TestCase(int statusCode, boolean retryHTTP403, boolean result) {
        this.statusCode = statusCode;
        this.retryHTTP403 = retryHTTP403;
        this.result = result; // expected result of calling isNonRetryableHTTPCode()
      }

      public int statusCode;
      public boolean retryHTTP403;
      public boolean result;
    }
    List<TestCase> testCases = new ArrayList<>();
    // no retry on HTTP 403 option

    testCases.add(new TestCase(100, false, true));
    testCases.add(new TestCase(101, false, true));
    testCases.add(new TestCase(103, false, true));
    testCases.add(new TestCase(200, false, true));
    testCases.add(new TestCase(201, false, true));
    testCases.add(new TestCase(202, false, true));
    testCases.add(new TestCase(203, false, true));
    testCases.add(new TestCase(204, false, true));
    testCases.add(new TestCase(205, false, true));
    testCases.add(new TestCase(206, false, true));
    testCases.add(new TestCase(300, false, true));
    testCases.add(new TestCase(301, false, true));
    testCases.add(new TestCase(302, false, true));
    testCases.add(new TestCase(303, false, true));
    testCases.add(new TestCase(304, false, true));
    testCases.add(new TestCase(307, false, true));
    testCases.add(new TestCase(308, false, true));
    testCases.add(new TestCase(400, false, true));
    testCases.add(new TestCase(401, false, true));
    testCases.add(new TestCase(403, false, true)); // no retry on HTTP 403
    testCases.add(new TestCase(404, false, true));
    testCases.add(new TestCase(405, false, true));
    testCases.add(new TestCase(406, false, true));
    testCases.add(new TestCase(407, false, true));
    testCases.add(new TestCase(408, false, false)); // do retry on HTTP 408
    testCases.add(new TestCase(410, false, true));
    testCases.add(new TestCase(411, false, true));
    testCases.add(new TestCase(412, false, true));
    testCases.add(new TestCase(413, false, true));
    testCases.add(new TestCase(414, false, true));
    testCases.add(new TestCase(415, false, true));
    testCases.add(new TestCase(416, false, true));
    testCases.add(new TestCase(417, false, true));
    testCases.add(new TestCase(418, false, true));
    testCases.add(new TestCase(425, false, true));
    testCases.add(new TestCase(426, false, true));
    testCases.add(new TestCase(428, false, true));
    testCases.add(new TestCase(429, false, false)); // do retry on HTTP 429
    testCases.add(new TestCase(431, false, true));
    testCases.add(new TestCase(451, false, true));
    testCases.add(new TestCase(500, false, false));
    testCases.add(new TestCase(501, false, false));
    testCases.add(new TestCase(502, false, false));
    testCases.add(new TestCase(503, false, false));
    testCases.add(new TestCase(504, false, false));
    testCases.add(new TestCase(505, false, false));
    testCases.add(new TestCase(506, false, false));
    testCases.add(new TestCase(507, false, false));
    testCases.add(new TestCase(508, false, false));
    testCases.add(new TestCase(509, false, false));
    testCases.add(new TestCase(510, false, false));
    testCases.add(new TestCase(511, false, false));
    testCases.add(new TestCase(513, false, false));
    // do retry on HTTP 403 option
    testCases.add(new TestCase(100, true, true));
    testCases.add(new TestCase(101, true, true));
    testCases.add(new TestCase(103, true, true));
    testCases.add(new TestCase(200, true, true));
    testCases.add(new TestCase(201, true, true));
    testCases.add(new TestCase(202, true, true));
    testCases.add(new TestCase(203, true, true));
    testCases.add(new TestCase(204, true, true));
    testCases.add(new TestCase(205, true, true));
    testCases.add(new TestCase(206, true, true));
    testCases.add(new TestCase(300, true, true));
    testCases.add(new TestCase(301, true, true));
    testCases.add(new TestCase(302, true, true));
    testCases.add(new TestCase(303, true, true));
    testCases.add(new TestCase(304, true, true));
    testCases.add(new TestCase(307, true, true));
    testCases.add(new TestCase(308, true, true));
    testCases.add(new TestCase(400, true, true));
    testCases.add(new TestCase(401, true, true));
    testCases.add(new TestCase(403, true, false)); // do retry on HTTP 403
    testCases.add(new TestCase(404, true, true));
    testCases.add(new TestCase(405, true, true));
    testCases.add(new TestCase(406, true, true));
    testCases.add(new TestCase(407, true, true));
    testCases.add(new TestCase(408, true, false)); // do retry on HTTP 408
    testCases.add(new TestCase(410, true, true));
    testCases.add(new TestCase(411, true, true));
    testCases.add(new TestCase(412, true, true));
    testCases.add(new TestCase(413, true, true));
    testCases.add(new TestCase(414, true, true));
    testCases.add(new TestCase(415, true, true));
    testCases.add(new TestCase(416, true, true));
    testCases.add(new TestCase(417, true, true));
    testCases.add(new TestCase(418, true, true));
    testCases.add(new TestCase(425, true, true));
    testCases.add(new TestCase(426, true, true));
    testCases.add(new TestCase(428, true, true));
    testCases.add(new TestCase(429, true, false)); // do retry on HTTP 429
    testCases.add(new TestCase(431, true, true));
    testCases.add(new TestCase(451, true, true));
    testCases.add(new TestCase(500, true, false));
    testCases.add(new TestCase(501, true, false));
    testCases.add(new TestCase(502, true, false));
    testCases.add(new TestCase(503, true, false));
    testCases.add(new TestCase(504, true, false));
    testCases.add(new TestCase(505, true, false));
    testCases.add(new TestCase(506, true, false));
    testCases.add(new TestCase(507, true, false));
    testCases.add(new TestCase(508, true, false));
    testCases.add(new TestCase(509, true, false));
    testCases.add(new TestCase(510, true, false));
    testCases.add(new TestCase(511, true, false));
    testCases.add(new TestCase(513, true, false));

    for (TestCase t : testCases) {
      if (t.result) {
        assertTrue(
            RestRequest.isNonRetryableHTTPCode(anyStatusCodeResponse(t.statusCode), t.retryHTTP403),
            String.format(
                "Result must be true but false: HTTP Code: %d, RetryHTTP403: %s",
                t.statusCode, t.retryHTTP403));
      } else {
        assertFalse(
            RestRequest.isNonRetryableHTTPCode(anyStatusCodeResponse(t.statusCode), t.retryHTTP403),
            String.format(
                "Result must be false but true: HTTP Code: %d, RetryHTTP403: %s",
                t.statusCode, t.retryHTTP403));
      }
    }
  }

  @Test
  public void testExceptionAuthBasedTimeout() throws IOException {
    CloseableHttpClient client = mock(CloseableHttpClient.class);
    when(client.execute(any(HttpUriRequest.class)))
        .thenAnswer((Answer<CloseableHttpResponse>) invocation -> retryResponse());

    SnowflakeSQLException ex =
        assertThrows(
            SnowflakeSQLException.class,
            () ->
                execute(
                    client, "login-request.com/?requestId=abcd-1234", 2, 1, 30000, true, false));
    assertThat(
        ex.getErrorCode(), equalTo(ErrorCode.AUTHENTICATOR_REQUEST_TIMEOUT.getMessageCode()));
  }

  @Test
  public void testExceptionAuthBasedTimeoutFor429ErrorCode() throws IOException {
    CloseableHttpClient client = mock(CloseableHttpClient.class);
    when(client.execute(any(HttpUriRequest.class)))
        .thenAnswer((Answer<CloseableHttpResponse>) invocation -> retryLoginResponse());

    SnowflakeSQLException ex =
        assertThrows(
            SnowflakeSQLException.class,
            () ->
                execute(
                    client, "login-request.com/?requestId=abcd-1234", 2, 1, 30000, true, false));
    assertThat(
        ex.getErrorCode(), equalTo(ErrorCode.AUTHENTICATOR_REQUEST_TIMEOUT.getMessageCode()));
  }

  @Test
  public void testNoRetry() throws IOException, SnowflakeSQLException {
    boolean telemetryEnabled = TelemetryService.getInstance().isEnabled();
    try {
      TelemetryService.disable(); // disable telemetry for the test
      CloseableHttpClient client = mock(CloseableHttpClient.class);
      when(client.execute(any(HttpUriRequest.class)))
          .thenAnswer(
              new Answer<CloseableHttpResponse>() {
                int callCount = 0;

                @Override
                public CloseableHttpResponse answer(InvocationOnMock invocation) throws Throwable {
                  callCount += 1;
                  assertTrue(callCount <= 1);
                  return retryResponse(); // return a retryable resp on the first attempt
                }
              });

      execute(client, "fakeurl.com/?requestId=abcd-1234", 0, 0, 0, true, true);
    } finally {
      if (telemetryEnabled) {
        TelemetryService.enable();
      } else {
        TelemetryService.disable();
      }
    }
  }

  /**
   * Test that after socket timeout, retryReason parameter is set for queries and is set to 0 for
   * null response.
   *
   * @throws SnowflakeSQLException
   * @throws IOException
   */
  @Test
  public void testRetryParametersWithSocketTimeout() throws IOException, SnowflakeSQLException {

    CloseableHttpClient client = mock(CloseableHttpClient.class);
    when(client.execute(any(HttpUriRequest.class)))
        .thenAnswer(
            new Answer<CloseableHttpResponse>() {
              int callCount = 0;

              @Override
              public CloseableHttpResponse answer(InvocationOnMock invocation) throws Throwable {
                HttpUriRequest arg = (HttpUriRequest) invocation.getArguments()[0];
                String params = arg.getURI().getQuery();

                if (callCount == 0) {
                  assertFalse(params.contains("retryCount="));
                  assertFalse(params.contains("retryReason="));
                  assertFalse(params.contains("clientStartTime="));
                  assertTrue(params.contains("request_guid="));
                } else {
                  assertTrue(params.contains("retryCount=" + callCount));
                  assertTrue(params.contains("retryReason=0"));
                  assertTrue(params.contains("clientStartTime="));
                  assertTrue(params.contains("request_guid="));
                }

                callCount += 1;
                if (callCount >= 2) {
                  return successResponse();
                } else {
                  return socketTimeoutResponse();
                }
              }
            });

    execute(client, "fakeurl.com/?requestId=abcd-1234", 0, 0, 0, true, false);
  }

  @Test
  public void testMaxRetriesExceeded() throws IOException {
    boolean telemetryEnabled = TelemetryService.getInstance().isEnabled();

    CloseableHttpClient client = mock(CloseableHttpClient.class);
    when(client.execute(any(HttpUriRequest.class)))
        .thenAnswer(
            new Answer<CloseableHttpResponse>() {
              int callCount = 0;

              @Override
              public CloseableHttpResponse answer(InvocationOnMock invocation) throws Throwable {
                callCount += 1;
                if (callCount >= 4) {
                  return successResponse();
                } else {
                  return socketTimeoutResponse();
                }
              }
            });

    try {
      TelemetryService.disable();
      assertThrows(
          SnowflakeSQLException.class,
          () -> execute(client, "fakeurl.com/?requestId=abcd-1234", 0, 0, 0, true, false, 1));
    } finally {
      if (telemetryEnabled) {
        TelemetryService.enable();
      } else {
        TelemetryService.disable();
      }
    }
  }

  @Test
  public void testConnectionClosedRetriesSuccessful() throws IOException, SnowflakeSQLException {
    CloseableHttpClient client = mock(CloseableHttpClient.class);
    when(client.execute(any(HttpUriRequest.class)))
        .then(
            new Answer<CloseableHttpResponse>() {
              int callCount = 0;

              @Override
              public CloseableHttpResponse answer(InvocationOnMock invocationOnMock)
                  throws Throwable {
                callCount += 1;
                if (callCount >= 1) {
                  return successResponse();
                } else {
                  throw new SocketException("Connection reset");
                }
              }
            });

    execute(client, "fakeurl.com/?requestId=abcd-1234", 0, 0, 0, true, false, 1);
  }

  @Test
  public void testLoginMaxRetries() throws IOException {
    boolean telemetryEnabled = TelemetryService.getInstance().isEnabled();

    CloseableHttpClient client = mock(CloseableHttpClient.class);
    when(client.execute(any(HttpUriRequest.class)))
        .thenAnswer(
            new Answer<CloseableHttpResponse>() {
              int callCount = 0;

              @Override
              public CloseableHttpResponse answer(InvocationOnMock invocation) throws Throwable {
                callCount += 1;
                if (callCount >= 4) {
                  return retryLoginResponse();
                } else {
                  return socketTimeoutResponse();
                }
              }
            });

    try {
      TelemetryService.disable();
      assertThrows(
          SnowflakeSQLException.class,
          () -> execute(client, "/session/v1/login-request", 0, 0, 0, true, false, 1));
    } finally {
      if (telemetryEnabled) {
        TelemetryService.enable();
      } else {
        TelemetryService.disable();
      }
    }
  }

  @Test
  public void testLoginTimeout() throws IOException {
    assumeRunningOnLinuxMac();
    boolean telemetryEnabled = TelemetryService.getInstance().isEnabled();

    CloseableHttpClient client = mock(CloseableHttpClient.class);
    when(client.execute(any(HttpUriRequest.class)))
        .thenAnswer(
            new Answer<CloseableHttpResponse>() {
              int callCount = 0;

              @Override
              public CloseableHttpResponse answer(InvocationOnMock invocation) throws Throwable {
                callCount += 1;
                if (callCount >= 4) {
                  return retryLoginResponse();
                } else {
                  return socketTimeoutResponse();
                }
              }
            });

    try {
      TelemetryService.disable();
      assertThrows(
          SnowflakeSQLException.class,
          () -> {
            execute(client, "/session/v1/login-request", 1, 0, 0, true, false, 10);
          });
    } finally {
      if (telemetryEnabled) {
        TelemetryService.enable();
      } else {
        TelemetryService.disable();
      }
    }
  }

  @Test
  public void testMaxRetriesWithSuccessfulResponse() throws IOException, SnowflakeSQLException {
    boolean telemetryEnabled = TelemetryService.getInstance().isEnabled();

    CloseableHttpClient client = mock(CloseableHttpClient.class);
    when(client.execute(any(HttpUriRequest.class)))
        .thenAnswer(
            new Answer<CloseableHttpResponse>() {
              int callCount = 0;

              @Override
              public CloseableHttpResponse answer(InvocationOnMock invocation) throws Throwable {
                callCount += 1;
                if (callCount >= 3) {
                  return successResponse();
                } else {
                  return socketTimeoutResponse();
                }
              }
            });

    try {
      TelemetryService.disable();
      execute(client, "fakeurl.com/?requestId=abcd-1234", 0, 0, 0, true, false, 4);
    } finally {
      if (telemetryEnabled) {
        TelemetryService.enable();
      } else {
        TelemetryService.disable();
      }
    }
  }

  @Test
  public void shouldGenerateBackoffInRangeExceptTheLastBackoff() {
    int minBackoffInMilli = 1000;
    int maxBackoffInMilli = 16000;
    long backoffInMilli = minBackoffInMilli;
    long elapsedMilliForTransientIssues = 0;
    DecorrelatedJitterBackoff decorrelatedJitterBackoff =
        new DecorrelatedJitterBackoff(minBackoffInMilli, maxBackoffInMilli);
    int retryTimeoutInMilli = 5 * 60 * 1000;
    while (true) {
      backoffInMilli =
          RestRequest.getNewBackoffInMilli(
              backoffInMilli,
              true,
              decorrelatedJitterBackoff,
              10,
              retryTimeoutInMilli,
              elapsedMilliForTransientIssues);

      assertTrue(
          backoffInMilli <= maxBackoffInMilli,
          "Backoff should be lower or equal to max backoff limit");
      if (elapsedMilliForTransientIssues + backoffInMilli >= retryTimeoutInMilli) {
        assertEquals(
            retryTimeoutInMilli - elapsedMilliForTransientIssues,
            backoffInMilli,
            "Backoff should fill time till retry timeout");
        break;
      } else {
        assertTrue(
            backoffInMilli >= minBackoffInMilli,
            "Backoff should be higher or equal to min backoff limit");
      }
      elapsedMilliForTransientIssues += backoffInMilli;
    }
  }
}
