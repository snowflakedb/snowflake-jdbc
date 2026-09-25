package net.snowflake.client.internal.core;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import net.snowflake.client.AbstractDriverIT;
import net.snowflake.client.api.datasource.SnowflakeDataSource;
import net.snowflake.client.api.datasource.SnowflakeDataSourceFactory;
import net.snowflake.client.api.exception.SnowflakeSQLException;
import net.snowflake.client.internal.exception.SnowflakeSQLLoggedException;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

class MockAuthExternalBrowserHandlers
    implements SessionUtilExternalBrowser.AuthExternalBrowserHandlers {
  @Override
  public HttpPost build(URI uri) {
    HttpPost httpPost = mock(HttpPost.class);
    when(httpPost.getMethod()).thenReturn("POST");
    return httpPost;
  }

  @Override
  public void openBrowser(String ssoUrl) throws SFException {
    // nop. Don't open browser
  }

  @Override
  public void output(String msg) {
    // nop. No output
  }
}

/** Simulates SessionUtilExternalBrower without popping up a browser window */
class FakeSessionUtilExternalBrowser extends SessionUtilExternalBrowser {
  static final String MOCK_SAML_TOKEN = "MOCK_SAML_TOKEN";
  private final ServerSocket mockServerSocket;

  public static SessionUtilExternalBrowser createInstance(SFLoginInput loginInput, boolean isPost) {
    return new FakeSessionUtilExternalBrowser(loginInput, isPost);
  }

  private FakeSessionUtilExternalBrowser(SFLoginInput loginInput, boolean isPost) {
    super(loginInput, new MockAuthExternalBrowserHandlers());
    try {
      this.mockServerSocket = initMockServerSocket(isPost);
    } catch (IOException ex) {
      throw new RuntimeException("Failed to initialize ServerSocket mock");
    }
  }

  /** Drives the real accept loop against a test-supplied (real) ServerSocket. */
  FakeSessionUtilExternalBrowser(SFLoginInput loginInput, ServerSocket serverSocket) {
    super(loginInput, new MockAuthExternalBrowserHandlers());
    this.mockServerSocket = serverSocket;
  }

  /**
   * Mock ServerSocket and Socket.
   *
   * <p>Socket mock will be included in ServerSocket mock.
   *
   * @param isPost true if the response is POST request otherwise GET request
   * @return Server socket
   * @throws IOException if any IO error occurs
   */
  private static ServerSocket initMockServerSocket(boolean isPost) throws IOException {
    // mock client socket
    final Socket mockSocket = mock(Socket.class);
    String str;
    if (isPost) {
      str =
          String.format(
              "POST / HTTP/1.1\r\n"
                  + "USER-AGENT: snowflake client\r\n"
                  + "Origin: https://testaccount.snowflakecomputing.com\r\n"
                  + "\r\n"
                  + "token=%s&confirm=1",
              MOCK_SAML_TOKEN);
    } else {
      str =
          String.format(
              "GET /?token=%s&confirm=1 HTTP/1.1\r\n" + "USER-AGENT: snowflake client",
              MOCK_SAML_TOKEN);
    }
    InputStream stream = new ByteArrayInputStream(str.getBytes(StandardCharsets.UTF_8));
    when(mockSocket.getInputStream()).thenReturn(stream);
    when(mockSocket.getOutputStream()).thenReturn(new NullOutputStream());

    // mock server socket
    final ServerSocket mockServerSocket = mock(ServerSocket.class);
    when(mockServerSocket.getLocalPort()).thenReturn(12345);
    when(mockServerSocket.accept()).thenReturn(mockSocket);
    return mockServerSocket;
  }

  static class NullOutputStream extends OutputStream {
    @Override
    public void write(int b) throws IOException {}
  }

  @Override
  protected ServerSocket getServerSocket() throws SFException {
    return mockServerSocket;
  }

  @Override
  protected int getLocalPort(ServerSocket ssocket) {
    return super.getLocalPort(ssocket);
  }
}

public class SessionUtilExternalBrowserTest {
  private static final String MOCK_PROOF_KEY = "specialkey";
  private static final String MOCK_SSO_URL = "https://sso.someidp.net/";

  /** A token large enough (with headers) to force the SSO request across multiple TCP reads. */
  private static final String LARGE_TOKEN = new String(new char[3800]).replace('\0', 'A');

  /**
   * Unit test for SessionUtilExternalBrowser
   *
   * @throws Throwable if any error occurs
   */
  @Test
  public void testSessionUtilExternalBrowser() throws Throwable {
    final SFLoginInput loginInput = initMockLoginInput();

    try (MockedStatic<HttpUtil> mockedHttpUtil = mockStatic(HttpUtil.class)) {
      mockedHttpUtil
          .when(
              () ->
                  HttpUtil.executeGeneralRequestWithContext(
                      Mockito.any(HttpRequestBase.class),
                      Mockito.anyInt(),
                      Mockito.anyInt(),
                      Mockito.anyInt(),
                      Mockito.anyInt(),
                      Mockito.anyInt(),
                      Mockito.nullable(HttpClientSettingsKey.class),
                      Mockito.nullable(SFBaseSession.class)))
          .thenReturn(
              new HttpResponseWithHeaders(
                  "{\"success\":\"true\",\"data\":{\"proofKey\":\""
                      + MOCK_PROOF_KEY
                      + "\","
                      + " \"ssoUrl\":\""
                      + MOCK_SSO_URL
                      + "\"}}",
                  new HashMap<>()));

      SessionUtilExternalBrowser sub =
          FakeSessionUtilExternalBrowser.createInstance(loginInput, false);
      sub.authenticate();
      MatcherAssert.assertThat(
          "", sub.getToken(), equalTo(FakeSessionUtilExternalBrowser.MOCK_SAML_TOKEN));

      sub = FakeSessionUtilExternalBrowser.createInstance(loginInput, true);
      sub.authenticate();
      MatcherAssert.assertThat(
          "", sub.getToken(), equalTo(FakeSessionUtilExternalBrowser.MOCK_SAML_TOKEN));

      sub = FakeSessionUtilExternalBrowser.createInstance(loginInput, false);
      Mockito.when(loginInput.getDisableConsoleLogin()).thenReturn(false);
      sub.authenticate();
      MatcherAssert.assertThat(
          "", sub.getToken(), equalTo(FakeSessionUtilExternalBrowser.MOCK_SAML_TOKEN));

      sub = FakeSessionUtilExternalBrowser.createInstance(loginInput, false);
      Mockito.when(loginInput.getDisableConsoleLogin())
          .thenAnswer(
              invocation -> {
                throw new SocketTimeoutException("Test exception");
              });
      try {
        sub.authenticate();
        fail("should have failed with an exception.");
      } catch (SFException ex) {
        assertTrue(ex.getMessage().contains("External browser authentication failed"));
      }
    }
  }

  /**
   * Unit test for SessionUtilExternalBrowser (fail)
   *
   * @throws Throwable if any error occurs
   */
  @Test
  public void testSessionUtilExternalBrowserFail() throws Throwable {
    final SFLoginInput loginInput = initMockLoginInput();

    try (MockedStatic<HttpUtil> mockedHttpUtil = mockStatic(HttpUtil.class)) {
      mockedHttpUtil
          .when(
              () ->
                  HttpUtil.executeGeneralRequestWithContext(
                      Mockito.any(HttpRequestBase.class),
                      Mockito.anyInt(),
                      Mockito.anyInt(),
                      Mockito.anyInt(),
                      Mockito.anyInt(),
                      Mockito.anyInt(),
                      Mockito.nullable(HttpClientSettingsKey.class),
                      Mockito.nullable(SFBaseSession.class)))
          .thenReturn(
              new HttpResponseWithHeaders(
                  "{\"success\":\"false\",\"code\":\"123456\",\"message\":\"errormes\"}",
                  new HashMap<>()));

      SessionUtilExternalBrowser sub =
          FakeSessionUtilExternalBrowser.createInstance(loginInput, false);
      SnowflakeSQLException ex =
          assertThrows(
              SnowflakeSQLException.class,
              () -> {
                sub.authenticate();
              });
      MatcherAssert.assertThat("Error is expected", ex.getErrorCode(), equalTo(123456));
    }
  }

  @Test
  public void testBuildDefaultHandler() throws URISyntaxException {
    SessionUtilExternalBrowser.DefaultAuthExternalBrowserHandlers handler =
        new SessionUtilExternalBrowser.DefaultAuthExternalBrowserHandlers();
    URI uri =
        new URI("https://testaccount.snowflakecomputing.com:443/session/authenticator-request");
    HttpPost postReq = handler.build(uri);
    assertEquals(
        "POST https://testaccount.snowflakecomputing.com:443/session/authenticator-request HTTP/1.1",
        postReq.toString());
  }

  @Test
  public void testInvalidSSOUrl() {
    SessionUtilExternalBrowser.DefaultAuthExternalBrowserHandlers handler =
        new SessionUtilExternalBrowser.DefaultAuthExternalBrowserHandlers();
    SFException ex =
        assertThrows(
            SFException.class,
            () -> {
              handler.openBrowser("file://invalidUrl");
            });
    assertTrue(ex.getMessage().contains("Invalid SSOUrl found"));
  }

  /**
   * SNOW-3704231: a browser preconnect socket that closes without data, then a large GET request
   * split across two TCP fragments, over real OS sockets.
   */
  @Test
  public void testAuthenticateOverRealSocket() throws Throwable {
    byte[] requestBytes =
        String.format(
                "GET /?token=%s&confirm=1 HTTP/1.1\r\n"
                    + "USER-AGENT: snowflake client\r\n"
                    + "Sec-Fetch-Site: none\r\n"
                    + "Sec-GPC: 1\r\n"
                    + "\r\n",
                LARGE_TOKEN)
            .getBytes(StandardCharsets.UTF_8);
    // Split the request into two TCP fragments to force a partial first read.
    int half = requestBytes.length / 2;
    byte[] fragment1 = Arrays.copyOfRange(requestBytes, 0, half);
    byte[] fragment2 = Arrays.copyOfRange(requestBytes, half, requestBytes.length);

    String token = driveRealSocketCallback(true, fragment1, fragment2);
    assertEquals(LARGE_TOKEN, token);
  }

  /**
   * SNOW-3704231: a POST callback with the token in the body, delivered in a TCP fragment separate
   * from the headers, so the read loop must not stop at the {@code \r\n\r\n} terminator.
   */
  @Test
  public void testAuthenticatePostBodyOverRealSocket() throws Throwable {
    byte[] body = String.format("token=%s&confirm=1", LARGE_TOKEN).getBytes(StandardCharsets.UTF_8);
    byte[] headerBytes =
        String.format(
                "POST / HTTP/1.1\r\n"
                    + "USER-AGENT: snowflake client\r\n"
                    + "Origin: https://testaccount.snowflakecomputing.com\r\n"
                    + "Content-Length: %d\r\n"
                    + "\r\n",
                body.length)
            .getBytes(StandardCharsets.UTF_8);

    // Fragment 1: headers ending exactly at the \r\n\r\n terminator; fragment 2: the body.
    String token = driveRealSocketCallback(false, headerBytes, body);
    assertEquals(LARGE_TOKEN, token);
  }

  @Test
  public void testAuthenticateAcceptsExactAndDefaultPortOrigins() throws Throwable {
    CallbackResult exact =
        driveRealSocketCallbacks(
            "https://testaccount.snowflakecomputing.com/",
            postRequest("exact_token", "https://testaccount.snowflakecomputing.com", "\r\n"));
    assertEquals("exact_token", exact.token);

    CallbackResult defaultPort =
        driveRealSocketCallbacks(
            "https://testaccount.snowflakecomputing.com:443/",
            postRequest(
                "default_port_token", "https://testaccount.snowflakecomputing.com", "\r\n"));
    assertEquals("default_port_token", defaultPort.token);
  }

  @Test
  public void testAuthenticateAcceptsOriginWithDefaultPath() throws Throwable {
    CallbackResult result =
        driveRealSocketCallbacks(
            "https://testaccount.snowflakecomputing.com/session/authenticator-request",
            postRequest("slash_token", "https://testaccount.snowflakecomputing.com/", "\r\n"));
    assertEquals("slash_token", result.token);
  }

  @Test
  public void testAuthenticateAcceptsUnderscoreHostOrigin() throws Throwable {
    CallbackResult result =
        driveRealSocketCallbacks(
            "https://foo_bar.snowflakecomputing.com/",
            postRequest("underscore_token", "https://foo_bar.snowflakecomputing.com", "\r\n"));
    assertEquals("underscore_token", result.token);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "http://testaccount.snowflakecomputing.com",
        "https://other.snowflakecomputing.com",
        "https://testaccount.snowflakecomputing.com:8443",
        "https://testaccount.snowflakecomputing.com.invalid",
        "https://user@testaccount.snowflakecomputing.com",
        "https://testaccount.snowflakecomputing.com/extra",
        "https://testaccount.snowflakecomputing.com?q=1",
        "https://testaccount.snowflakecomputing.com#frag",
        "ftp://testaccount.snowflakecomputing.com"
      })
  public void testAuthenticateIgnoresForeignOriginThenAcceptsValidCallback(String foreignOrigin)
      throws Throwable {
    CallbackResult result =
        driveRealSocketCallbacks(
            "https://testaccount.snowflakecomputing.com/",
            postRequest("foreign_token", foreignOrigin, "\r\n"),
            getRequest("valid_token", null, "\r\n"));

    assertEquals("valid_token", result.token);
    assertEquals(2, result.responses.size());
    assertTrue(result.responses.get(0).isEmpty());
    assertTrue(result.responses.get(1).contains("Your identity was confirmed"));
  }

  @Test
  public void testAuthenticateIgnoresBodyOriginThenAcceptsValidCallback() throws Throwable {
    String body = "token=body_token&Origin=https%3A%2F%2Ftestaccount.snowflakecomputing.com";
    String bodyOriginRequest =
        String.format(
            "POST / HTTP/1.1\r\n"
                + "Host: localhost\r\n"
                + "Content-Type: application/x-www-form-urlencoded\r\n"
                + "Content-Length: %d\r\n\r\n%s",
            body.getBytes(StandardCharsets.UTF_8).length, body);

    CallbackResult result =
        driveRealSocketCallbacks(
            "https://testaccount.snowflakecomputing.com/",
            bodyOriginRequest,
            getRequest("valid_token", null, "\r\n"));

    assertEquals("valid_token", result.token);
    assertTrue(result.responses.get(0).isEmpty());
  }

  @Test
  public void testAuthenticateAcceptsOriginlessAndNullOriginGet() throws Throwable {
    CallbackResult originless =
        driveRealSocketCallbacks(
            "https://testaccount.snowflakecomputing.com/",
            getRequest("originless_token", null, "\r\n"));
    assertEquals("originless_token", originless.token);
    assertFalse(originless.responses.get(0).contains("Access-Control-Allow-Origin"));

    CallbackResult nullOrigin =
        driveRealSocketCallbacks(
            "https://testaccount.snowflakecomputing.com/",
            getRequest("null_origin_token", "null", "\r\n"));
    assertEquals("null_origin_token", nullOrigin.token);
    assertFalse(nullOrigin.responses.get(0).contains("Access-Control-Allow-Origin"));
  }

  @Test
  public void testAuthenticateKeepsListeningAfterOriginAllowedGetWithoutToken() throws Throwable {
    CallbackResult result =
        driveRealSocketCallbacks(
            "https://testaccount.snowflakecomputing.com/",
            "GET / HTTP/1.1\r\n"
                + "Host: localhost\r\n"
                + "Origin: https://testaccount.snowflakecomputing.com\r\n\r\n",
            getRequest("after_tokenless_get", null, "\r\n"));

    assertEquals("after_tokenless_get", result.token);
    assertTrue(result.responses.get(0).isEmpty());
    assertTrue(result.responses.get(1).contains("Your identity was confirmed"));
  }

  @Test
  public void testAuthenticateKeepsListeningAfterFaviconThenAcceptsToken() throws Throwable {
    CallbackResult result =
        driveRealSocketCallbacks(
            "https://testaccount.snowflakecomputing.com/",
            "GET /favicon.ico HTTP/1.1\r\nHost: localhost\r\n\r\n",
            getRequest("after_favicon", null, "\r\n"));

    assertEquals("after_favicon", result.token);
    assertTrue(result.responses.get(0).isEmpty());
  }

  @Test
  public void testAuthenticateKeepsListeningAfterJsonPostWithoutToken() throws Throwable {
    String emptyJson = "{}";
    String emptyPost =
        "POST / HTTP/1.1\r\n"
            + "Host: localhost\r\n"
            + "Origin: https://testaccount.snowflakecomputing.com\r\n"
            + "Content-Type: application/json\r\n"
            + "Content-Length: "
            + emptyJson.getBytes(StandardCharsets.UTF_8).length
            + "\r\n\r\n"
            + emptyJson;

    CallbackResult result =
        driveRealSocketCallbacks(
            "https://testaccount.snowflakecomputing.com/",
            emptyPost,
            getRequest("after_empty_json", null, "\r\n"));

    assertEquals("after_empty_json", result.token);
    assertTrue(result.responses.get(0).isEmpty());
  }

  @Test
  public void testAuthenticateAcceptsMatchingOriginJsonPost() throws Throwable {
    String body = "{\"token\":\"json_token\",\"consent\":true}";
    String request =
        "POST / HTTP/1.1\r\n"
            + "Host: localhost\r\n"
            + "Origin: https://testaccount.snowflakecomputing.com\r\n"
            + "Content-Type: application/json\r\n"
            + "Content-Length: "
            + body.getBytes(StandardCharsets.UTF_8).length
            + "\r\n\r\n"
            + body;

    CallbackResult result =
        driveRealSocketCallbacks("https://testaccount.snowflakecomputing.com/", request);

    assertEquals("json_token", result.token);
    assertTrue(result.responses.get(0).contains("Access-Control-Allow-Origin"));
  }

  @Test
  public void testCallbackSocketBacklogIsFive() {
    assertEquals(5, SessionUtilExternalBrowser.CALLBACK_SOCKET_BACKLOG);
  }

  @Test
  public void testRemainingTimeoutMillisIsZeroAfterDeadline() {
    assertEquals(0, SessionUtilExternalBrowser.remainingTimeoutMillis(System.nanoTime() - 1));
  }

  @Test
  public void testRemainingTimeoutMillisIsPositiveBeforeDeadline() {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    int remaining = SessionUtilExternalBrowser.remainingTimeoutMillis(deadline);
    assertTrue(remaining > 0);
    assertTrue(remaining <= 30_000);
  }

  @Test
  public void testAuthenticateParsesLfHeaders() throws Throwable {
    CallbackResult result =
        driveRealSocketCallbacks(
            "https://testaccount.snowflakecomputing.com/",
            postRequest("lf_token", "https://testaccount.snowflakecomputing.com", "\n"));

    assertEquals("lf_token", result.token);
  }

  @Test
  public void testCorsPreflightRequiresPostAllowedHeadersAndMatchingOrigin() throws Throwable {
    String validPreflight =
        optionsRequest(
            "https://testaccount.snowflakecomputing.com", "post", "content-type", "\r\n");
    CallbackResult valid =
        driveRealSocketCallbacks(
            "https://testaccount.snowflakecomputing.com/",
            validPreflight,
            getRequest("valid_token", null, "\r\n"));
    assertTrue(
        valid
            .responses
            .get(0)
            .contains(
                "Access-Control-Allow-Origin: " + "https://testaccount.snowflakecomputing.com"));
    assertTrue(valid.responses.get(0).contains("Access-Control-Allow-Headers: Content-Type"));

    CallbackResult invalid =
        driveRealSocketCallbacks(
            "https://testaccount.snowflakecomputing.com/",
            optionsRequest("https://other.snowflakecomputing.com", "POST", "Content-Type", "\r\n"),
            optionsRequest(
                "https://testaccount.snowflakecomputing.com", "GET", "Content-Type", "\r\n"),
            optionsRequest(
                "https://testaccount.snowflakecomputing.com",
                "POST",
                "Content-Type, X-Custom",
                "\r\n"),
            getRequest("valid_token", null, "\r\n"));
    assertEquals("valid_token", invalid.token);
    assertTrue(invalid.responses.get(0).isEmpty());
    assertTrue(invalid.responses.get(1).isEmpty());
    assertTrue(invalid.responses.get(2).isEmpty());
  }

  private static String postRequest(String token, String origin, String lineEnding) {
    String body = "token=" + token + "&confirm=1";
    return "POST / HTTP/1.1"
        + lineEnding
        + "Host: localhost"
        + lineEnding
        + "Origin: "
        + origin
        + lineEnding
        + "Content-Type: application/x-www-form-urlencoded"
        + lineEnding
        + "Content-Length: "
        + body.getBytes(StandardCharsets.UTF_8).length
        + lineEnding
        + lineEnding
        + body;
  }

  private static String getRequest(String token, String origin, String lineEnding) {
    return "GET /?token="
        + token
        + "&confirm=1 HTTP/1.1"
        + lineEnding
        + "Host: localhost"
        + lineEnding
        + (origin == null ? "" : "Origin: " + origin + lineEnding)
        + lineEnding;
  }

  private static String optionsRequest(
      String origin, String method, String headers, String lineEnding) {
    return "OPTIONS / HTTP/1.1"
        + lineEnding
        + "Host: localhost"
        + lineEnding
        + "Origin: "
        + origin
        + lineEnding
        + "Access-Control-Request-Method: "
        + method
        + lineEnding
        + "Access-Control-Request-Headers: "
        + headers
        + lineEnding
        + lineEnding;
  }

  private static final class CallbackResult {
    private final String token;
    private final List<String> responses;

    private CallbackResult(String token, List<String> responses) {
      this.token = token;
      this.responses = responses;
    }
  }

  private CallbackResult driveRealSocketCallbacks(String serverUrl, String... requests)
      throws Throwable {
    final SFLoginInput loginInput = initMockLoginInput();
    when(loginInput.getServerUrl()).thenReturn(serverUrl);
    when(loginInput.getBrowserResponseTimeout()).thenReturn(Duration.ofSeconds(5));

    try (MockedStatic<HttpUtil> mockedHttpUtil = mockStatic(HttpUtil.class);
        ServerSocket serverSocket = new ServerSocket(0, 0, InetAddress.getByName("localhost"))) {
      mockSsoUrlResponse(mockedHttpUtil);
      int port = serverSocket.getLocalPort();
      AtomicReference<Throwable> clientError = new AtomicReference<>();
      List<String> responses = new ArrayList<>();
      Thread browser =
          new Thread(
              () -> {
                try {
                  for (String request : requests) {
                    try (Socket socket = new Socket("localhost", port);
                        ByteArrayOutputStream response = new ByteArrayOutputStream()) {
                      socket.getOutputStream().write(request.getBytes(StandardCharsets.UTF_8));
                      socket.shutdownOutput();
                      byte[] buffer = new byte[4096];
                      int count;
                      while ((count = socket.getInputStream().read(buffer)) >= 0) {
                        response.write(buffer, 0, count);
                      }
                      responses.add(response.toString(StandardCharsets.UTF_8.name()));
                    }
                  }
                } catch (Throwable t) {
                  clientError.set(t);
                }
              });
      browser.setDaemon(true);
      browser.start();

      SessionUtilExternalBrowser subject =
          new FakeSessionUtilExternalBrowser(loginInput, serverSocket);
      subject.authenticate();

      browser.join(10_000);
      assertFalse(browser.isAlive(), "Browser client thread did not finish");
      if (clientError.get() != null) {
        throw new AssertionError("Browser client thread failed", clientError.get());
      }
      return new CallbackResult(subject.getToken(), responses);
    }
  }

  /**
   * Drives the real {@code authenticate()} loop against a real localhost {@link ServerSocket}, with
   * a background thread that writes {@code fragments} over TCP ({@link Thread#sleep} between them
   * forces a partial read), optionally preceded by a preconnect socket closed without data. Returns
   * the extracted token.
   */
  private String driveRealSocketCallback(boolean sendPreconnect, byte[]... fragments)
      throws Throwable {
    final SFLoginInput loginInput = initMockLoginInput();
    // A real (non-zero) timeout so the test fails fast instead of hanging if the fix regresses.
    when(loginInput.getBrowserResponseTimeout()).thenReturn(Duration.ofSeconds(30));

    try (MockedStatic<HttpUtil> mockedHttpUtil = mockStatic(HttpUtil.class);
        ServerSocket serverSocket = new ServerSocket(0, 0, InetAddress.getByName("localhost"))) {
      mockSsoUrlResponse(mockedHttpUtil);
      int port = serverSocket.getLocalPort();

      AtomicReference<Throwable> clientError = new AtomicReference<>();
      Thread browser =
          new Thread(
              () -> {
                try {
                  if (sendPreconnect) {
                    // Speculative/preconnect socket that closes without sending data (the crash).
                    new Socket("localhost", port).close();
                  }
                  try (Socket s = new Socket("localhost", port)) {
                    OutputStream out = s.getOutputStream();
                    for (int i = 0; i < fragments.length; i++) {
                      if (i > 0) {
                        Thread.sleep(100);
                      }
                      out.write(fragments[i]);
                      out.flush();
                    }
                    // Drain the driver's HTTP response so it can finish cleanly.
                    s.getInputStream().read(new byte[4096]);
                  }
                } catch (Throwable t) {
                  clientError.set(t);
                }
              });
      browser.setDaemon(true);
      browser.start();

      SessionUtilExternalBrowser sub = new FakeSessionUtilExternalBrowser(loginInput, serverSocket);
      sub.authenticate();

      browser.join(10_000);
      if (clientError.get() != null) {
        throw new AssertionError("Browser client thread failed", clientError.get());
      }
      return sub.getToken();
    }
  }

  private static void mockSsoUrlResponse(MockedStatic<HttpUtil> mockedHttpUtil) throws Exception {
    mockedHttpUtil
        .when(
            () ->
                HttpUtil.executeGeneralRequestWithContext(
                    Mockito.any(HttpRequestBase.class),
                    Mockito.anyInt(),
                    Mockito.anyInt(),
                    Mockito.anyInt(),
                    Mockito.anyInt(),
                    Mockito.anyInt(),
                    Mockito.nullable(HttpClientSettingsKey.class),
                    Mockito.nullable(SFBaseSession.class)))
        .thenReturn(
            new HttpResponseWithHeaders(
                "{\"success\":\"true\",\"data\":{\"proofKey\":\""
                    + MOCK_PROOF_KEY
                    + "\", \"ssoUrl\":\""
                    + MOCK_SSO_URL
                    + "\"}}",
                new HashMap<>()));
  }

  /**
   * Mock HttpUtil and SFLoginInput
   *
   * @return a mock object for SFLoginInput
   */
  private SFLoginInput initMockLoginInput() {
    // mock SFLoginInput
    SFLoginInput loginInput = mock(SFLoginInput.class);
    when(loginInput.getServerUrl()).thenReturn("https://testaccount.snowflakecomputing.com/");
    when(loginInput.getAuthenticator()).thenReturn("externalbrowser");
    when(loginInput.getAccountName()).thenReturn("testaccount");
    when(loginInput.getUserName()).thenReturn("testuser");
    when(loginInput.getDisableConsoleLogin()).thenReturn(true);
    when(loginInput.getBrowserResponseTimeout()).thenReturn(Duration.ofSeconds(120));
    return loginInput;
  }

  // Run this test manually to test disabling storing temporary credetials with external browser
  // auth. This is valid for versions after 3.18.0.
  @Test
  @Disabled
  public void testEnableClientStoreTemporaryCredential() throws Exception {
    Map<String, String> params = AbstractDriverIT.getConnectionParameters();
    SnowflakeDataSource ds = SnowflakeDataSourceFactory.createDataSource();
    ds.setServerName(params.get("host"));
    ds.setAccount(params.get("account"));
    ds.setPortNumber(Integer.parseInt(params.get("port")));
    ds.setUser(params.get("user"));
    ds.setEnableClientStoreTemporaryCredential(false);

    for (int i = 0; i < 3; i++) {
      try (Connection con = ds.getConnection();
          ResultSet rs = con.createStatement().executeQuery("SELECT 1")) {
        assertTrue(rs.next());
      }
    }
  }

  // Run this test manually to confirm external browser timeout is working. When test runs it will
  // open a browser window for authentication, close the window, and you should get the expected
  // error message within the set timeout. Valid for driver versions after 3.18.0.
  @Test
  @Disabled
  public void testExternalBrowserTimeout() throws Exception {
    Map<String, String> params = AbstractDriverIT.getConnectionParameters();
    SnowflakeDataSource ds = SnowflakeDataSourceFactory.createDataSource();
    ds.setServerName(params.get("host"));
    ds.setAccount(params.get("account"));
    ds.setPortNumber(Integer.parseInt(params.get("port")));
    ds.setUser(params.get("user"));
    ds.setBrowserResponseTimeout(10);
    SnowflakeSQLLoggedException e =
        assertThrows(
            SnowflakeSQLLoggedException.class,
            () -> {
              ds.getConnection();
            });
    assertTrue(e.getMessage().contains("External browser authentication failed"));
  }
}
