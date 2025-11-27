package net.snowflake.client.jdbc;

import static net.snowflake.client.AssumptionUtils.assumeRunningOnLinuxMac;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLKeyException;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLProtocolException;
import net.snowflake.client.core.ExecTimeTelemetryData;
import net.snowflake.client.core.HttpClientSettingsKey;
import net.snowflake.client.core.HttpExecutingContext;
import net.snowflake.client.core.HttpExecutingContextBuilder;
import net.snowflake.client.core.HttpResponseContextDto;
import net.snowflake.client.core.HttpUtil;
import net.snowflake.client.core.OCSPMode;
import net.snowflake.client.core.SFBaseSession;
import net.snowflake.client.core.SFTrustManager;
import net.snowflake.client.jdbc.telemetry.Telemetry;
import net.snowflake.client.jdbc.telemetry.TelemetryClient;
import net.snowflake.client.jdbc.telemetry.TelemetryData;
import net.snowflake.client.jdbc.telemetry.TelemetryField;
import net.snowflake.client.jdbc.telemetry.TelemetryUtil;
import net.snowflake.client.jdbc.telemetryOOB.TelemetryService;
import net.snowflake.client.util.DecorrelatedJitterBackoff;
import net.snowflake.common.core.SqlState;
import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicStatusLine;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
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
    InputStream inputStream = new ByteArrayInputStream("response body".getBytes());

    // Create a mock entity and assign the stream to it
    BasicHttpEntity mockEntity = new BasicHttpEntity();
    mockEntity.setContent(inputStream);
    when(successResponse.getEntity()).thenReturn(mockEntity);

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

    RestRequest.executeWithRetries(
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
        false,
        new ExecTimeTelemetryData(),
        null,
        null,
        null,
        false);
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
  public void testRetryOnTransientSslHandshakeEof() throws Exception {
    CloseableHttpClient client = mock(CloseableHttpClient.class);
    // First attempt throws SSLHandshakeException with EOF root cause, second succeeds
    SSLHandshakeException handshake = new SSLHandshakeException("handshake failed");
    handshake.initCause(new EOFException("remote host closed connection during handshake"));
    when(client.execute(any(HttpUriRequest.class)))
        .thenThrow(handshake)
        .thenReturn(successResponse());

    // Use helper to execute with maxRetries >= 1 so the loop can retry
    execute(
        client,
        "fakeurl.com/?requestId=abcd-1234",
        /* retryTimeout */ 0,
        /* authTimeout */ 0,
        /* socketTimeout */ 0,
        /* includeRetryParameters */ true,
        /* noRetry */ false,
        /* maxRetries */ 2);

    // Verify two executions (first throws, second succeeds)
    verify(client, times(2)).execute(any(HttpUriRequest.class));
  }

  @Test
  public void testNoRetryOnSslHandshakeWithoutEof() throws Exception {
    CloseableHttpClient client = mock(CloseableHttpClient.class);
    // SSLHandshakeException without EOF root cause should be treated as non-retryable
    SSLHandshakeException handshake = new SSLHandshakeException("handshake failed (non-EOF)");
    when(client.execute(any(HttpUriRequest.class))).thenThrow(handshake);

    assertThrows(
        SnowflakeSQLException.class,
        () ->
            execute(
                client,
                "fakeurl.com/?requestId=abcd-1234",
                /* retryTimeout */ 0,
                /* authTimeout */ 0,
                /* socketTimeout */ 0,
                /* includeRetryParameters */ true,
                /* noRetry */ false,
                /* maxRetries */ 2));
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

      SnowflakeSQLException ex =
          assertThrows(
              SnowflakeSQLException.class,
              () -> execute(client, "fakeurl.com/?requestId=abcd-1234", 0, 0, 0, true, true));
      assertThat(ex.getErrorCode(), equalTo(ErrorCode.NETWORK_ERROR.getMessageCode()));

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

  /**
   * Test that IllegalStateException during HTTP request execution triggers a rebuild of the HTTP
   * client.
   *
   * @param statusCode the status code to return from the mock response
   * @param useDecompression whether to use decompression in the HTTP client
   * @throws Exception if an error occurs during the test
   */
  @ParameterizedTest
  @MethodSource("httpClientRebuildParams")
  public void testIllegalStateExceptionTriggersHttpClientRebuildParameterized(
      int statusCode, boolean useDecompression) throws Exception {

    CloseableHttpClient mockHttpClient = mock(CloseableHttpClient.class);
    HttpRequestBase mockRequest = mock(HttpRequestBase.class);
    when(mockRequest.getURI()).thenReturn(new URI("https://example.com"));
    CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
    StatusLine mockStatusLine = mock(StatusLine.class);
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockStatusLine.getStatusCode()).thenReturn(statusCode);
    HttpClientSettingsKey mockKey = mock(HttpClientSettingsKey.class);

    when(mockHttpClient.execute(any(HttpRequestBase.class)))
        .thenThrow(new IllegalStateException("HttpClient shutdown"))
        .thenReturn(mockResponse);

    try (MockedStatic<HttpUtil> httpUtilMockedStatic = mockStatic(HttpUtil.class)) {
      httpUtilMockedStatic
          .when(() -> HttpUtil.getHttpClient(any(), any()))
          .thenReturn(mockHttpClient);
      httpUtilMockedStatic
          .when(() -> HttpUtil.getHttpClientWithoutDecompression(any(), any()))
          .thenReturn(mockHttpClient);

      HttpResponseContextDto result =
          RestRequest.executeWithRetries(
              mockHttpClient,
              mockRequest,
              10,
              0,
              1000,
              2,
              0,
              new AtomicBoolean(false),
              false,
              false,
              false,
              false,
              false,
              new ExecTimeTelemetryData(),
              null,
              mockKey,
              Collections.emptyList(),
              useDecompression);

      verify(mockHttpClient, times(2)).execute(any(HttpRequestBase.class));
      assertNotNull(result);
    }
  }

  /**
   * Provides parameters for the parameterized test.
   *
   * @return a stream of arguments for the test
   */
  private static Stream<Arguments> httpClientRebuildParams() {
    return Stream.of(Arguments.of(200, false), Arguments.of(200, true));
  }

  @Test
  public void testHandlingNotRetryableException_isProtocolVersionError() throws Exception {
    // Create a mock HttpExecutingContext
    HttpExecutingContext mockContext =
        HttpExecutingContextBuilder.withRequest("test-request-id", "test-request-info").build();

    // Test case 1: SSL exception with protocol version error - should NOT throw
    // SnowflakeSQLLoggedException
    // This should fall through to the general exception handling
    SSLHandshakeException protocolVersionException =
        new SSLHandshakeException("Received fatal alert: protocol_version");

    Exception result =
        RestRequest.handlingNotRetryableException(protocolVersionException, mockContext);

    // Should return the original exception, not throw SnowflakeSQLLoggedException
    assertEquals(
        protocolVersionException,
        result,
        "SSL exception with protocol version error should return original exception");

    // Test case 2: SSL exception without protocol version error - should throw
    // SnowflakeSQLLoggedException
    SSLHandshakeException nonProtocolVersionException =
        new SSLHandshakeException("SSL handshake failed for some other reason");

    assertThrows(
        SnowflakeSQLLoggedException.class,
        () -> {
          RestRequest.handlingNotRetryableException(nonProtocolVersionException, mockContext);
        },
        "SSL exception without protocol version error should throw SnowflakeSQLLoggedException");

    // Test case 3: Different SSL exception types with protocol version error
    SSLProtocolException sslProtocolException =
        new SSLProtocolException("Received fatal alert: protocol_version");

    Exception result2 =
        RestRequest.handlingNotRetryableException(sslProtocolException, mockContext);
    assertEquals(
        sslProtocolException,
        result2,
        "SSLProtocolException with protocol version error should return original exception");

    // Test case 4: Different SSL exception types without protocol version error
    SSLKeyException sslKeyException = new SSLKeyException("SSL key verification failed");

    assertThrows(
        SnowflakeSQLLoggedException.class,
        () -> {
          RestRequest.handlingNotRetryableException(sslKeyException, mockContext);
        },
        "SSLKeyException without protocol version error should throw SnowflakeSQLLoggedException");

    // Test case 5: SSLPeerUnverifiedException with protocol version error
    SSLPeerUnverifiedException peerUnverifiedException =
        new SSLPeerUnverifiedException("Received fatal alert: protocol_version");

    Exception result3 =
        RestRequest.handlingNotRetryableException(peerUnverifiedException, mockContext);
    assertEquals(
        peerUnverifiedException,
        result3,
        "SSLPeerUnverifiedException with protocol version error should return original exception");

    // Test case 6: Non-SSL exception - should return the original exception
    IOException ioException = new IOException("Some IO error");

    Exception result4 = RestRequest.handlingNotRetryableException(ioException, mockContext);
    assertEquals(ioException, result4, "Non-SSL exception should return original exception");
  }

  @Test
  public void testIsProtocolVersionError() {
    // Test true cases
    assertTrue(
        RestRequest.isProtocolVersionError(
            new SSLHandshakeException("Received fatal alert: protocol_version")),
        "Should return true for exception with protocol_version message");

    assertTrue(
        RestRequest.isProtocolVersionError(
            new SSLProtocolException(
                "Some prefix Received fatal alert: protocol_version some suffix")),
        "Should return true for exception containing protocol_version message");

    // Test false cases
    assertFalse(
        RestRequest.isProtocolVersionError(new SSLHandshakeException("Some other SSL error")),
        "Should return false for exception without protocol_version message");

    assertFalse(
        RestRequest.isProtocolVersionError(
            new SSLHandshakeException("Received fatal alert: handshake_failure")),
        "Should return false for different SSL alert type");

    assertFalse(
        RestRequest.isProtocolVersionError(new IOException("Some IO error")),
        "Should return false for non-SSL exception");

    // Test null message
    assertFalse(
        RestRequest.isProtocolVersionError(new SSLHandshakeException((String) null)),
        "Should return false for exception with null message");
  }

  @Test
  public void testIsExceptionInGroup() {
    // Test SSL exceptions are in the sslExceptions group
    assertTrue(
        RestRequest.isExceptionInGroup(
            new SSLHandshakeException("test"), RestRequest.sslExceptions),
        "SSLHandshakeException should be in SSL exceptions group");

    assertTrue(
        RestRequest.isExceptionInGroup(new SSLKeyException("test"), RestRequest.sslExceptions),
        "SSLKeyException should be in SSL exceptions group");

    assertTrue(
        RestRequest.isExceptionInGroup(
            new SSLPeerUnverifiedException("test"), RestRequest.sslExceptions),
        "SSLPeerUnverifiedException should be in SSL exceptions group");

    assertTrue(
        RestRequest.isExceptionInGroup(new SSLProtocolException("test"), RestRequest.sslExceptions),
        "SSLProtocolException should be in SSL exceptions group");

    // Test non-SSL exceptions are not in the group
    assertFalse(
        RestRequest.isExceptionInGroup(new IOException("test"), RestRequest.sslExceptions),
        "IOException should not be in SSL exceptions group");

    assertFalse(
        RestRequest.isExceptionInGroup(new RuntimeException("test"), RestRequest.sslExceptions),
        "RuntimeException should not be in SSL exceptions group");
  }

  @Test
  public void testSendIBHttpErrorEventWithNullSessionNoException() throws Exception {
    HttpRequestBase mockRequest = mock(HttpRequestBase.class);
    CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
    HttpExecutingContext mockContext = mock(HttpExecutingContext.class);

    when(mockContext.getSfSession()).thenReturn(null);

    java.lang.reflect.Method method =
        RestRequest.class.getDeclaredMethod(
            "sendIBHttpErrorEvent",
            HttpRequestBase.class,
            CloseableHttpResponse.class,
            HttpExecutingContext.class);
    method.setAccessible(true);

    method.invoke(null, mockRequest, mockResponse, mockContext);

    Mockito.verify(mockContext).getSfSession();
  }

  @Test
  public void testSendIBHttpErrorEventWithValidSession() throws Exception {
    HttpRequestBase mockRequest = mock(HttpRequestBase.class);
    CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
    StatusLine mockStatusLine = mock(StatusLine.class);
    HttpExecutingContext mockContext = mock(HttpExecutingContext.class);
    SFBaseSession mockSession = mock(SFBaseSession.class);
    Telemetry mockTelemetryClient = mock(Telemetry.class);

    when(mockContext.getSfSession()).thenReturn(mockSession);
    when(mockSession.getTelemetryClient()).thenReturn(mockTelemetryClient);
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockStatusLine.getStatusCode()).thenReturn(500);
    when(mockStatusLine.getReasonPhrase()).thenReturn("Internal Server Error");

    when(mockRequest.getMethod()).thenReturn("POST");
    URI mockURI = new URI("https://test.snowflakecomputing.com/session/v1/login-request");
    when(mockRequest.getURI()).thenReturn(mockURI);

    try (MockedStatic<TelemetryUtil> mockedTelemetryUtil =
        Mockito.mockStatic(TelemetryUtil.class)) {
      ObjectNode mockIbValue = mock(ObjectNode.class);
      TelemetryData mockTelemetryData = mock(TelemetryData.class);

      mockedTelemetryUtil
          .when(
              () -> TelemetryUtil.createIBValue(any(), any(), anyInt(), any(), anyString(), any()))
          .thenReturn(mockIbValue);
      mockedTelemetryUtil
          .when(() -> TelemetryUtil.buildJobData(any(ObjectNode.class)))
          .thenReturn(mockTelemetryData);

      java.lang.reflect.Method method =
          RestRequest.class.getDeclaredMethod(
              "sendIBHttpErrorEvent",
              HttpRequestBase.class,
              CloseableHttpResponse.class,
              HttpExecutingContext.class);
      method.setAccessible(true);

      method.invoke(null, mockRequest, mockResponse, mockContext);

      Mockito.verify(mockContext).getSfSession();
      Mockito.verify(mockSession).getTelemetryClient();
      Mockito.verify(mockResponse).getStatusLine();
      Mockito.verify(mockStatusLine, Mockito.atLeast(1)).getStatusCode();
      Mockito.verify(mockStatusLine, Mockito.atLeast(1)).getReasonPhrase();
      Mockito.verify(mockRequest).getMethod();
      Mockito.verify(mockRequest, Mockito.atLeast(1)).getURI();
      Mockito.verify(mockTelemetryClient).addLogToBatch(mockTelemetryData);

      mockedTelemetryUtil.verify(
          () ->
              TelemetryUtil.createIBValue(
                  eq(null),
                  eq(SqlState.INTERNAL_ERROR),
                  eq(ErrorCode.HTTP_GENERAL_ERROR.getMessageCode() + 500),
                  eq(TelemetryField.HTTP_EXCEPTION),
                  eq(
                      "HTTP 500 Internal Server Error: POST test.snowflakecomputing.com/session/v1/login-request"),
                  eq(null)));

      mockedTelemetryUtil.verify(() -> TelemetryUtil.buildJobData(mockIbValue));
    }
  }

  @Test
  public void testExecuteRequestFailsWithEventEmission() throws IOException {
    StatusLine statusLine =
        new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), 500, "Internal Server Error");

    HttpRequestBase mockRequest = mock(HttpRequestBase.class);
    when(mockRequest.getMethod()).thenReturn("POST");
    URI mockURI = URI.create("https://test.snowflakecomputing.com/session/v1/login-request");
    when(mockRequest.getURI()).thenReturn(mockURI);

    HttpExecutingContext mockContext = mock(HttpExecutingContext.class);
    SFBaseSession mockSession = mock(SFBaseSession.class);
    TelemetryClient mockTelemetryClient = mock(TelemetryClient.class);

    when(mockSession.getTelemetryClient()).thenReturn(mockTelemetryClient);
    when(mockContext.getSfSession()).thenReturn(mockSession);

    HttpExecutingContext mockHttpExecutingContext = mock(HttpExecutingContext.class);
    when(mockHttpExecutingContext.getSfSession()).thenReturn(mockSession);

    try (CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
        CloseableHttpClient mockHttpClient = mock(CloseableHttpClient.class); ) {
      when(mockHttpClient.execute(any(HttpUriRequest.class))).thenReturn(mockResponse);
      when(mockResponse.getStatusLine()).thenReturn(statusLine);
      when(mockResponse.getStatusLine())
          .thenReturn(
              new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), 500, "Internal Server Error"));

      assertThrows(
          SnowflakeSQLException.class,
          () ->
              RestRequest.executeWithRetries(
                  mockHttpClient,
                  mockRequest,
                  mockHttpExecutingContext,
                  new ExecTimeTelemetryData(),
                  null,
                  null,
                  null,
                  false));
    }

    ArgumentCaptor<TelemetryData> telemetryDataCaptor =
        ArgumentCaptor.forClass(TelemetryData.class);
    Mockito.verify(mockSession).getTelemetryClient();
    Mockito.verify(mockTelemetryClient).addLogToBatch(telemetryDataCaptor.capture());

    TelemetryData capturedData = telemetryDataCaptor.getValue();
    assertNotNull(capturedData, "TelemetryData should not be null");

    ObjectNode message = capturedData.getMessage();
    assertNotNull(message, "TelemetryData message should not be null");

    assertEquals(
        "client_http_exception",
        message.get("type").asText(),
        "Type should be client_http_exception");
    assertEquals("JDBC", message.get("DriverType").asText(), "DriverType should be JDBC");
    assertNotNull(message.get("DriverVersion"), "DriverVersion should be present");
    assertEquals(
        "XX000", message.get("SQLState").asText(), "SQLState should be XX000 (INTERNAL_ERROR)");
    assertEquals(
        290500, message.get("ErrorNumber").asInt(), "ErrorNumber should be 290500 (290000 + 500)");
    assertEquals(
        "HTTP 500 Internal Server Error: POST test.snowflakecomputing.com/session/v1/login-request",
        message.get("ErrorMessage").asText(),
        "ErrorMessage should match expected format");

    assertTrue(capturedData.getTimeStamp() > 0, "Timestamp should be positive");
    assertTrue(
        capturedData.getTimeStamp() <= System.currentTimeMillis(),
        "Timestamp should not be in the future");
  }

  @Test
  public void testOCSPCheckFailsWithEventEmission() throws IOException {
    System.setProperty(SFTrustManager.SF_OCSP_TEST_INJECT_VALIDITY_ERROR, Boolean.TRUE.toString());
    System.setProperty(
        SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_ENABLED, Boolean.FALSE.toString());

    try {
      SFBaseSession mockSession = mock(SFBaseSession.class);
      TelemetryClient mockTelemetryClient = mock(TelemetryClient.class);
      when(mockSession.getTelemetryClient()).thenReturn(mockTelemetryClient);

      HttpClientSettingsKey settingsKey = new HttpClientSettingsKey(OCSPMode.FAIL_CLOSED);
      CloseableHttpClient httpClient = HttpUtil.buildHttpClient(settingsKey, null, false);

      HttpRequestBase httpRequest = new HttpGet("https://test.snowflakecomputing.com/");

      HttpExecutingContext context =
          HttpExecutingContextBuilder.withRequest("test-request-id", "test-request-info")
              .withSfSession(mockSession)
              .build();

      assertThrows(
          SnowflakeSQLException.class,
          () ->
              RestRequest.executeWithRetries(
                  httpClient,
                  httpRequest,
                  context,
                  new ExecTimeTelemetryData(),
                  null,
                  settingsKey,
                  null,
                  false));

      ArgumentCaptor<TelemetryData> telemetryDataCaptor =
          ArgumentCaptor.forClass(TelemetryData.class);
      Mockito.verify(mockSession).getTelemetryClient();
      Mockito.verify(mockTelemetryClient).addLogToBatch(telemetryDataCaptor.capture());

      TelemetryData capturedData = telemetryDataCaptor.getValue();
      assertNotNull(capturedData, "TelemetryData should not be null");

      ObjectNode message = capturedData.getMessage();
      assertNotNull(message, "TelemetryData message should not be null");

      assertEquals(
          "client_ocsp_exception",
          message.get("type").asText(),
          "Type should be client_ocsp_exception");
      assertEquals("JDBC", message.get("DriverType").asText(), "DriverType should be JDBC");
      assertNotNull(message.get("DriverVersion"), "DriverVersion should be present");
      assertEquals(
          "XX000", message.get("SQLState").asText(), "SQLState should be XX000 (INTERNAL_ERROR)");
      assertEquals(254000, message.get("ErrorNumber").asInt(), "ErrorNumber should be 254000");
      assertTrue(
          message
              .get("ErrorMessage")
              .asText()
              .contains("The OCSP response validity is out of range"),
          "ErrorMessage should contain OCSP validity error message");
      assertTrue(
          message.get("reason").asText().contains("INVALID_OCSP_RESPONSE_VALIDITY"),
          "Reason should contain INVALID_OCSP_RESPONSE_VALIDITY");

      assertTrue(capturedData.getTimeStamp() > 0, "Timestamp should be positive");
      assertTrue(
          capturedData.getTimeStamp() <= System.currentTimeMillis(),
          "Timestamp should not be in the future");
    } finally {
      SFTrustManager.cleanTestSystemParameters();
    }
  }
}
