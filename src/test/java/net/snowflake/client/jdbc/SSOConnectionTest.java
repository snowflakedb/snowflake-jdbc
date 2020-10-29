/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.snowflake.client.core.HttpUtil;
import net.snowflake.client.core.OCSPMode;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SFLoginInput;
import net.snowflake.client.core.SessionUtil;
import net.snowflake.client.core.SessionUtilExternalBrowser;
import net.snowflake.common.core.ClientAuthnDTO;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

class MockAuthExternalBrowserHandlers
    implements SessionUtilExternalBrowser.AuthExternalBrowserHandlers
{
  @Override
  public HttpPost build(URI uri)
  {
    HttpPost httpPost = mock(HttpPost.class);
    when(httpPost.getMethod()).thenReturn("POST");
    return httpPost;
  }

  @Override
  public void openBrowser(String ssoUrl) throws SFException
  {
    // nop. Don't open browser
  }

  @Override
  public void output(String msg)
  {
    // nop. No output
  }
}


class FakeSessionUtilExternalBrowser extends SessionUtilExternalBrowser
{
  private final static String MOCK_SAML_TOKEN = "MOCK_SAML_TOKEN";
  private final ServerSocket mockServerSocket;

  FakeSessionUtilExternalBrowser(SFLoginInput loginInput)
  {
    super(loginInput, new MockAuthExternalBrowserHandlers());
    try
    {
      this.mockServerSocket = initMockServerSocket();
    }
    catch (IOException ex)
    {
      throw new RuntimeException("Failed to initialize ServerSocket mock");
    }
  }

  /**
   * Mock ServerSocket and Socket.
   * <p>
   * Socket mock will be included in ServerSocket mock.
   *
   * @return Server socket
   * @throws IOException if any IO error occurs
   */
  private static ServerSocket initMockServerSocket() throws IOException
  {
    // mock client socket
    final Socket mockSocket = mock(Socket.class);
    final String str = String.format(
        "GET /?token=%s HTTP/1.1\r\nUSER-AGENT: snowflake client",
        MOCK_SAML_TOKEN);
    InputStream stream = new ByteArrayInputStream(
        str.getBytes(StandardCharsets.UTF_8));
    when(mockSocket.getInputStream()).thenReturn(stream);
    when(mockSocket.getOutputStream()).thenReturn(new NullOutputStream());

    // mock server socket
    final ServerSocket mockServerSocket = mock(ServerSocket.class);
    when(mockServerSocket.getLocalPort()).thenReturn(12345);
    when(mockServerSocket.accept()).thenReturn(mockSocket);
    return mockServerSocket;
  }

  static class NullOutputStream extends OutputStream
  {
    @Override
    public void write(int b) throws IOException
    {
    }
  }

  @Override
  protected ServerSocket getServerSocket() throws SFException
  {
    return mockServerSocket;
  }

  @Override
  protected int getLocalPort(ServerSocket ssocket)
  {
    return super.getLocalPort(ssocket);
  }

}

@Ignore("powermock incompat with JDK>=9, see https://github.com/powermock/powermock/issues/901")
@RunWith(PowerMockRunner.class)
public class SSOConnectionTest
{
  private final static String MOCK_PROOF_KEY = "specialkey";
  private final static String MOCK_SSO_URL = "https://sso.someidp.net/";
  private final static String MOCK_MASTER_TOKEN = "MOCK_MASTER_TOKEN";
  private final static String MOCK_SESSION_TOKEN = "MOCK_SESSION_TOKEN";
  private final static String MOCK_ID_TOKEN = "MOCK_ID_TOKEN";
  private final static String MOCK_NEW_SESSION_TOKEN = "MOCK_NEW_SESSION_TOKEN";
  private static ObjectMapper mapper = new ObjectMapper();

  @BeforeClass
  public static void setUpClass() throws Throwable
  {
    Class.forName("net.snowflake.client.jdbc.SnowflakeDriver");
  }

  @Test
  @PrepareForTest({
                      java.net.ServerSocket.class,
                      Socket.class,
                      net.snowflake.client.core.HttpUtil.class,
                      net.snowflake.client.core.SessionUtilExternalBrowser.class,
                      org.apache.http.impl.client.CloseableHttpClient.class
                  })
  public void testIdTokenInSSO() throws Throwable
  {
    initMock();
    SessionUtil.deleteIdTokenCache("testaccount.snowflakecomputing.com", "testuser");

    Properties properties = new Properties();
    properties.put("user", "testuser");
    properties.put("password", "testpassword");
    properties.put("account", "testaccount");
    properties.put("insecureMode", true);
    properties.put("authenticator", "externalbrowser");
    properties.put("CLIENT_STORE_TEMPORARY_CREDENTIAL", true);

    // connect url
    String url = "jdbc:snowflake://testaccount.snowflakecomputing.com";

    // initial connection getting id token and storing in the cache file.
    Connection con = DriverManager.getConnection(url, properties);
    SnowflakeConnectionV1 sfcon = (SnowflakeConnectionV1) con;
    assertThat("token", sfcon.getSfSession()
        .getSessionToken(), equalTo(MOCK_SESSION_TOKEN));
    assertThat("idToken", sfcon.getSfSession()
        .getIdToken(), equalTo(MOCK_ID_TOKEN));

    // second connection reads the cache and use the id token to get the
    // session token.
    Connection conSecond = DriverManager.getConnection(url, properties);
    SnowflakeConnectionV1 sfconSecond = (SnowflakeConnectionV1) conSecond;
    assertThat("token", sfconSecond
        .getSfSession().getSessionToken(), equalTo(MOCK_NEW_SESSION_TOKEN));
  }

  class HttpUtilResponseDataSSODTO
  {
    public String proofKey;
    public String ssoUrl;

    public HttpUtilResponseDataSSODTO(String proofKey, String ssoUrl)
    {
      this.proofKey = proofKey;
      this.ssoUrl = ssoUrl;
    }
  }

  class HttpUtilResponseDataAuthDTO
  {
    public String token;
    public String masterToken;
    public String idToken;

    HttpUtilResponseDataAuthDTO(
        String token, String masterToken, String idToken)
    {
      this.token = token;
      this.masterToken = masterToken;
      this.idToken = idToken;
    }
  }

  class HttpUtilResponseDataTokenRequestDTO
  {
    public String sessionToken;

    HttpUtilResponseDataTokenRequestDTO(String sessionToken)
    {
      this.sessionToken = sessionToken;
    }
  }

  class HttpUtilResponseDataQueryRequestDTO
  {
    public String value;

    HttpUtilResponseDataQueryRequestDTO(String value)
    {
      this.value = value;
    }
  }

  class HttpUtilResponseDTO
  {
    public boolean success;
    public String message;
    public Object data;

    HttpUtilResponseDTO(boolean success, String message, Object data)
    {
      this.success = success;
      this.message = message;
      this.data = data;
    }
  }

  private void initMock() throws Throwable
  {
    initMockHttpUtil();
    SFLoginInput loginInput = initMockLoginInput();
    initMockSessionUtilExternalBrowser(loginInput);
  }

  private void initMockHttpUtil() throws SnowflakeSQLException, IOException
  {
    mockStatic(HttpUtil.class);

    CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
    // initHttpClient always returns a mock CloseableHttpClient
    when(HttpUtil.initHttpClient(
        Mockito.any(OCSPMode.class),
        Mockito.any(File.class))).thenReturn(httpClient);

    // connect to SSO for the first connection
    String retInitialSSO = mapper.writeValueAsString(
        new HttpUtilResponseDTO(
            true, null,
            new HttpUtilResponseDataSSODTO(
                MOCK_PROOF_KEY, MOCK_SSO_URL)));

    // connect to Snowflake for the first connection
    String retInitialAuthentication = mapper.writeValueAsString(
        new HttpUtilResponseDTO(
            true, null,
            new HttpUtilResponseDataAuthDTO(
                MOCK_SESSION_TOKEN,
                MOCK_MASTER_TOKEN,
                MOCK_ID_TOKEN)));
    // token-requests ISSUE for the second connection
    String retTokenRequestIssue = mapper.writeValueAsString(
        new HttpUtilResponseDTO(
            true, null,
            new HttpUtilResponseDataTokenRequestDTO(
                MOCK_NEW_SESSION_TOKEN)));

    // select '1' used internally to refresh the current objects
    // for the second connection
    String retSelectOne = mapper.writeValueAsString(
        new HttpUtilResponseDTO(
            true, null,
            new HttpUtilResponseDataQueryRequestDTO(
                "value")));

    when(HttpUtil.executeGeneralRequest(
        Mockito.any(HttpRequestBase.class),
        Mockito.anyInt(),
        Mockito.any(OCSPMode.class))).thenReturn(
        retInitialSSO,
        retInitialAuthentication,
        retTokenRequestIssue);

    // the last query, "select '1'", hits the query-request endpoint,
    // so it calls executeRequest with the includeRetryParameters parameter
    when(HttpUtil.executeGeneralRequest(
        Mockito.any(HttpRequestBase.class),
        Mockito.anyInt(),
        Mockito.any(OCSPMode.class))).thenReturn(
        retSelectOne);
  }

  private void initMockSessionUtilExternalBrowser(
      SFLoginInput loginInput)
  {
    mockStatic(SessionUtilExternalBrowser.class);

    SessionUtilExternalBrowser fakeExternalBrowser =
        new FakeSessionUtilExternalBrowser(loginInput);
    when(SessionUtilExternalBrowser.createInstance(
        Mockito.any(SFLoginInput.class))).
        thenReturn(fakeExternalBrowser);
  }

  private SFLoginInput initMockLoginInput()
  {
    SFLoginInput loginInput = mock(SFLoginInput.class);
    when(loginInput.getServerUrl()).thenReturn(
        "https://testaccount.snowflakecomputing.com/");
    when(loginInput.getAuthenticator()).thenReturn(
        ClientAuthnDTO.AuthenticatorType.EXTERNALBROWSER.name());
    when(loginInput.getAccountName()).thenReturn("testaccount");
    when(loginInput.getUserName()).thenReturn("testuser");
    return loginInput;
  }

}
