package net.snowflake.client.internal.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import net.snowflake.client.api.exception.ErrorCode;
import net.snowflake.client.api.exception.SnowflakeSQLException;
import net.snowflake.client.api.http.HttpHeadersCustomizer;
import net.snowflake.client.internal.jdbc.RestRequest;
import net.snowflake.client.internal.jdbc.telemetry.ExecTimeTelemetryData;
import net.snowflake.client.internal.jdbc.telemetryOOB.TelemetryService;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.AbstractHttpEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

/** Requests after Apache HttpClient shut the driver's cached connection pool down. */
public class HttpClientPoolShutdownTest {
  private static final Duration TIMEOUT = Duration.ofSeconds(60);

  // unique per test: cached clients are keyed by settings, not by header customizers
  private final HttpClientSettingsKey key =
      new HttpClientSettingsKey(OCSPMode.DISABLE_OCSP_CHECKS, UUID.randomUUID().toString(), false);
  private HttpServer server;
  private String url;

  @BeforeEach
  public void startServer() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/",
        exchange -> {
          byte[] body = "{}".getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(200, body.length);
          try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
          }
        });
    server.start();
    url = "http://127.0.0.1:" + server.getAddress().getPort() + "/";
  }

  @AfterEach
  public void cleanUp() {
    server.stop(0);
    HttpUtil.removeHttpClients(key);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void shouldRetryOnNewPoolAfterErrorShutsCachedPoolDown(boolean withoutDecompression) {
    CloseableHttpClient client = cachedClient(withoutDecompression, null);
    shutDownPoolWithError(client);

    HttpResponseContextDto response =
        assertTimeoutPreemptively(
            TIMEOUT, () -> execute(client, withoutDecompression, null, 30, 3));

    assertEquals(200, response.getHttpResponse().getStatusLine().getStatusCode());
    assertNotSame(client, cachedClient(withoutDecompression, null));
  }

  @Test
  public void shouldReplaceClientsOfShutDownPoolOnLookup() {
    CloseableHttpClient client = cachedClient(false, null);
    CloseableHttpClient clientWithoutDecompression = cachedClient(true, null);
    shutDownPoolWithError(client);

    assertNotSame(client, cachedClient(false, null));
    assertNotSame(clientWithoutDecompression, cachedClient(true, null));
  }

  @Test
  public void shouldStopRetryingIllegalStateExceptionAtMaxRetries() {
    assertFailsWithIllegalStateException(30, 1, 2);
  }

  @Test
  public void shouldStopRetryingIllegalStateExceptionWithoutRetryLimits() {
    assertFailsWithIllegalStateException(0, 0, RestRequest.MAX_ILLEGAL_STATE_EXCEPTION_RETRIES + 1);
  }

  @Test
  public void shouldKeepIllegalStateExceptionOutOfTelemetry() {
    TelemetryService telemetryService = mock(TelemetryService.class);
    // also sends the retry event, on the first retry
    when(telemetryService.getNumOfRetryToTriggerTelemetry()).thenReturn(1);
    List<HttpHeadersCustomizer> customizers = Collections.singletonList(new FailingCustomizer());
    CloseableHttpClient client = cachedClient(false, customizers);

    assertTimeoutPreemptively(
        TIMEOUT,
        () -> {
          try (MockedStatic<TelemetryService> telemetry = mockStatic(TelemetryService.class)) {
            telemetry.when(TelemetryService::getInstance).thenReturn(telemetryService);
            assertThrows(
                SnowflakeSQLException.class, () -> execute(client, false, customizers, 30, 1));
          }
        });

    ArgumentCaptor<Exception> savedEx = ArgumentCaptor.forClass(Exception.class);
    verify(telemetryService, atLeastOnce())
        .logHttpRequestTelemetryEvent(
            any(),
            any(),
            anyInt(),
            any(),
            anyBoolean(),
            anyBoolean(),
            anyBoolean(),
            any(),
            savedEx.capture(),
            any(),
            anyLong(),
            anyInt(),
            any(),
            anyInt());
    assertEquals(Collections.nCopies(3, null), savedEx.getAllValues());
  }

  /** The ISE comes from a header customizer, so the pool stays up and the client is kept. */
  private void assertFailsWithIllegalStateException(
      long retryTimeout, int maxRetries, int expectedAttempts) {
    FailingCustomizer customizer = new FailingCustomizer();
    List<HttpHeadersCustomizer> customizers = Collections.singletonList(customizer);
    CloseableHttpClient client = cachedClient(false, customizers);

    SnowflakeSQLException e =
        assertTimeoutPreemptively(
            TIMEOUT,
            () ->
                assertThrows(
                    SnowflakeSQLException.class,
                    () -> execute(client, false, customizers, retryTimeout, maxRetries)));

    assertEquals(ErrorCode.NETWORK_ERROR.getMessageCode(), e.getErrorCode());
    assertInstanceOf(IllegalStateException.class, e.getCause());
    assertEquals(expectedAttempts, customizer.attempts.get());
    assertSame(client, cachedClient(false, customizers));
  }

  private CloseableHttpClient cachedClient(
      boolean withoutDecompression, List<HttpHeadersCustomizer> customizers) {
    return withoutDecompression
        ? HttpUtil.getHttpClientWithoutDecompression(key, customizers)
        : HttpUtil.getHttpClient(key, customizers);
  }

  /** Apache's MainClientExec shuts the pool down when an Error escapes a request. */
  private void shutDownPoolWithError(CloseableHttpClient client) {
    HttpPost request = new HttpPost(url);
    request.setEntity(
        new AbstractHttpEntity() {
          @Override
          public boolean isRepeatable() {
            return false;
          }

          @Override
          public long getContentLength() {
            return -1;
          }

          @Override
          public InputStream getContent() {
            throw new UnsupportedOperationException();
          }

          @Override
          public void writeTo(OutputStream out) {
            throw new OutOfMemoryError("simulated");
          }

          @Override
          public boolean isStreaming() {
            return false;
          }
        });
    assertThrows(OutOfMemoryError.class, () -> client.execute(request));
  }

  private HttpResponseContextDto execute(
      CloseableHttpClient client,
      boolean withoutDecompression,
      List<HttpHeadersCustomizer> customizers,
      long retryTimeout,
      int maxRetries)
      throws SnowflakeSQLException {
    return RestRequest.executeWithRetries(
        client,
        new HttpGet(url),
        retryTimeout, // s
        0, // auth timeout
        0, // socket timeout
        maxRetries,
        0, // inject socket timeout
        new AtomicBoolean(false), // canceling
        false, // without cookies
        false, // include retry parameters
        false, // include Snowflake headers
        false, // retry HTTP 403
        false, // no retry
        false, // unpack response
        new ExecTimeTelemetryData(),
        null, // session
        key,
        customizers,
        withoutDecompression);
  }

  private static class FailingCustomizer implements HttpHeadersCustomizer {
    // applies() runs once per attempt
    private final AtomicInteger attempts = new AtomicInteger();

    @Override
    public boolean applies(String method, String uri, Map<String, List<String>> currentHeaders) {
      attempts.incrementAndGet();
      throw new IllegalStateException("customizer failure");
    }

    @Override
    public Map<String, List<String>> newHeaders() {
      return Collections.emptyMap();
    }

    @Override
    public boolean invokeOnce() {
      return false;
    }
  }
}
