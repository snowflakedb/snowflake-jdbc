package net.snowflake.client.internal.core;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;
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
   * SNOW-3704231: the localhost callback server used to assume a single socket read returned the
   * whole HTTP request. Browsers routinely open speculative/preconnect sockets to the callback port
   * and close them without sending any data, so {@code in.read(buf)} returns -1 (EOF) and {@code
   * new String(buf, 0, -1)} threw StringIndexOutOfBoundsException before the real request was ever
   * processed. The server must ignore such empty connections and keep listening.
   *
   * @throws Throwable if any error occurs
   */
  @Test
  public void testAuthenticateIgnoresEmptyPreconnectSocket() throws Throwable {
    final SFLoginInput loginInput = initMockLoginInput();

    try (MockedStatic<HttpUtil> mockedHttpUtil = mockStatic(HttpUtil.class)) {
      mockSsoUrlResponse(mockedHttpUtil);

      // First connection sends no data (browser preconnect closed immediately) -> read() == -1.
      // Second connection carries the real token.
      Socket emptySocket = mockSocket(new ByteArrayInputStream(new byte[0]));
      Socket tokenSocket =
          mockSocket(
              toStream(
                  String.format(
                      "GET /?token=%s&confirm=1 HTTP/1.1\r\n" + "USER-AGENT: snowflake client",
                      FakeSessionUtilExternalBrowser.MOCK_SAML_TOKEN)));

      SessionUtilExternalBrowser sub =
          new SequencedFakeSessionUtilExternalBrowser(
              loginInput, mockServerSocket(emptySocket, tokenSocket));
      sub.authenticate();
      assertEquals(FakeSessionUtilExternalBrowser.MOCK_SAML_TOKEN, sub.getToken());
    }
  }

  /**
   * SNOW-3704231: large SSO requests (big tokens plus Sec-Fetch and Sec-GPC headers) fill the
   * request line and headers with several kilobytes of data. This confirms the callback server
   * still parses the token out of such a large request.
   *
   * @throws Throwable if any error occurs
   */
  @Test
  public void testAuthenticateHandlesLargeRequest() throws Throwable {
    final SFLoginInput loginInput = initMockLoginInput();

    try (MockedStatic<HttpUtil> mockedHttpUtil = mockStatic(HttpUtil.class)) {
      mockSsoUrlResponse(mockedHttpUtil);

      // A large token (mimicking a real SSO token) plus large privacy headers, ~4 KB total.
      String largeToken = IntStream.range(0, 3000).mapToObj(i -> "A").reduce("", String::concat);
      String request =
          String.format(
              "GET /?token=%s&confirm=1 HTTP/1.1\r\n"
                  + "USER-AGENT: snowflake client\r\n"
                  + "Sec-Fetch-Site: none\r\n"
                  + "Sec-GPC: 1\r\n"
                  + "\r\n",
              largeToken);

      Socket socket = mockSocket(toStream(request));

      SessionUtilExternalBrowser sub =
          new SequencedFakeSessionUtilExternalBrowser(loginInput, mockServerSocket(socket));
      sub.authenticate();
      assertEquals(largeToken, sub.getToken());
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

  private static InputStream toStream(String data) {
    return new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8));
  }

  private static Socket mockSocket(InputStream in) throws IOException {
    Socket socket = mock(Socket.class);
    when(socket.getInputStream()).thenReturn(in);
    when(socket.getOutputStream())
        .thenReturn(new FakeSessionUtilExternalBrowser.NullOutputStream());
    return socket;
  }

  private static ServerSocket mockServerSocket(Socket first, Socket... rest) throws IOException {
    ServerSocket serverSocket = mock(ServerSocket.class);
    when(serverSocket.getLocalPort()).thenReturn(12345);
    when(serverSocket.accept()).thenReturn(first, rest);
    return serverSocket;
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

/**
 * SessionUtilExternalBrowser whose ServerSocket is supplied by the test, so that the sequence of
 * accepted connections (e.g. an empty preconnect socket followed by the real request) can be
 * controlled.
 */
class SequencedFakeSessionUtilExternalBrowser extends SessionUtilExternalBrowser {
  private final ServerSocket serverSocket;

  SequencedFakeSessionUtilExternalBrowser(SFLoginInput loginInput, ServerSocket serverSocket) {
    super(loginInput, new MockAuthExternalBrowserHandlers());
    this.serverSocket = serverSocket;
  }

  @Override
  protected ServerSocket getServerSocket() {
    return serverSocket;
  }
}
