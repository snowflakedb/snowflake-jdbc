/*
 * Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Properties;
import net.snowflake.client.core.*;
import net.snowflake.common.core.ClientAuthnDTO;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.HttpPost;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

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

class FakeSessionUtilExternalBrowser extends SessionUtilExternalBrowser {
  private static final String MOCK_SAML_TOKEN = "MOCK_SAML_TOKEN";
  private final ServerSocket mockServerSocket;

  FakeSessionUtilExternalBrowser(SFLoginInput loginInput) {
    super(loginInput, new MockAuthExternalBrowserHandlers());
    try {
      this.mockServerSocket = initMockServerSocket();
    } catch (IOException ex) {
      throw new RuntimeException("Failed to initialize ServerSocket mock");
    }
  }

  /**
   * Mock ServerSocket and Socket.
   *
   * <p>Socket mock will be included in ServerSocket mock.
   *
   * @return Server socket
   * @throws IOException if any IO error occurs
   */
  private static ServerSocket initMockServerSocket() throws IOException {
    // mock client socket
    final Socket mockSocket = mock(Socket.class);
    final String str =
        String.format("GET /?token=%s HTTP/1.1\r\nUSER-AGENT: snowflake client", MOCK_SAML_TOKEN);
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

public class SSOConnectionTest {
  private static final String MOCK_PROOF_KEY = "specialkey";
  private static final String MOCK_SSO_URL = "https://sso.someidp.net/";
  private static final String MOCK_MASTER_TOKEN = "MOCK_MASTER_TOKEN";
  private static final String MOCK_SESSION_TOKEN = "MOCK_SESSION_TOKEN";
  private static final String MOCK_ID_TOKEN = "MOCK_ID_TOKEN";
  private static final String MOCK_NEW_SESSION_TOKEN = "MOCK_NEW_SESSION_TOKEN";
  private static final String MOCK_NEW_MASTER_TOKEN = "MOCK_NEW_MASTER_TOKEN";
  private static final String ID_TOKEN_AUTHENTICATOR = "ID_TOKEN";
  private static ObjectMapper mapper = new ObjectMapper();

  class HttpUtilResponseDataSSODTO {
    public String proofKey;
    public String ssoUrl;

    public HttpUtilResponseDataSSODTO(String proofKey, String ssoUrl) {
      this.proofKey = proofKey;
      this.ssoUrl = ssoUrl;
    }
  }

  class HttpUtilResponseDataParaDTO {
    public String name;
    public String value;

    HttpUtilResponseDataParaDTO(String name, String value) {
      this.name = name;
      this.value = value;
    }
  }

  class HttpUtilResponseDataAuthDTO {
    public String token;
    public String masterToken;
    public String idToken;
    public ArrayList<HttpUtilResponseDataParaDTO> parameters =
        new ArrayList<HttpUtilResponseDataParaDTO>();

    HttpUtilResponseDataAuthDTO(String token, String masterToken, String idToken) {
      this.token = token;
      this.masterToken = masterToken;
      this.idToken = idToken;

      parameters.add(new HttpUtilResponseDataParaDTO("AUTOCOMMIT", "true"));
    }
  }

  class HttpUtilResponseDTO {
    public boolean success;
    public String message;
    public Object data;

    HttpUtilResponseDTO(boolean success, String message, Object data) {
      this.success = success;
      this.message = message;
      this.data = data;
    }
  }

  private void initMock(
      MockedStatic<HttpUtil> mockedHttpUtil,
      MockedStatic<SessionUtilExternalBrowser> mockedSessionUtilExternalBrowser)
      throws Throwable {
    initMockHttpUtil(mockedHttpUtil);
    SFLoginInput loginInput = initMockLoginInput();
    initMockSessionUtilExternalBrowser(mockedSessionUtilExternalBrowser, loginInput);
  }

  private JsonNode parseRequest(HttpPost post) throws IOException {
    StringWriter writer = null;
    String theString;
    try {
      writer = new StringWriter();
      try (InputStream ins = post.getEntity().getContent()) {
        IOUtils.copy(ins, writer, "UTF-8");
      }
      theString = writer.toString();
    } finally {
      IOUtils.closeQuietly(writer);
    }

    JsonNode jsonNode = mapper.readTree(theString);
    return jsonNode;
  }

  private void initMockHttpUtil(MockedStatic<HttpUtil> mockedHttpUtil) throws IOException {
    // connect to SSO for the first connection
    String retInitialSSO =
        mapper.writeValueAsString(
            new HttpUtilResponseDTO(
                true, null, new HttpUtilResponseDataSSODTO(MOCK_PROOF_KEY, MOCK_SSO_URL)));

    // connect to Snowflake for the first connection
    String retInitialAuthentication =
        mapper.writeValueAsString(
            new HttpUtilResponseDTO(
                true,
                null,
                new HttpUtilResponseDataAuthDTO(
                    MOCK_SESSION_TOKEN, MOCK_MASTER_TOKEN, MOCK_ID_TOKEN)));
    // connect too Snowflake with the cached idToken
    String retSecondAuthentication =
        mapper.writeValueAsString(
            new HttpUtilResponseDTO(
                true,
                null,
                new HttpUtilResponseDataAuthDTO(
                    MOCK_NEW_SESSION_TOKEN, MOCK_NEW_MASTER_TOKEN, "")));

    mockedHttpUtil
        .when(
            () ->
                HttpUtil.executeGeneralRequest(
                    any(HttpPost.class),
                    anyInt(),
                    anyInt(),
                    anyInt(),
                    anyInt(),
                    nullable(HttpClientSettingsKey.class)))
        .thenAnswer(
            new Answer<String>() {
              int callCount = 0;

              @Override
              public String answer(InvocationOnMock invocation) throws IOException {
                String resp = "";
                final Object[] args = invocation.getArguments();
                JsonNode jsonNode;
                if (callCount == 0) {
                  resp = retInitialSSO;
                } else if (callCount == 1) {
                  jsonNode = parseRequest((HttpPost) args[0]);
                  assertTrue(
                      jsonNode
                          .path("data")
                          .path("SESSION_PARAMETERS")
                          .path("CLIENT_STORE_TEMPORARY_CREDENTIAL")
                          .asBoolean());
                  assertThat(
                      "authenticator",
                      jsonNode.path("data").path("AUTHENTICATOR").asText(),
                      equalTo(ClientAuthnDTO.AuthenticatorType.EXTERNALBROWSER.name()));
                  resp = retInitialAuthentication;
                } else if (callCount == 2) {
                  jsonNode = parseRequest((HttpPost) args[0]);
                  assertTrue(
                      jsonNode
                          .path("data")
                          .path("SESSION_PARAMETERS")
                          .path("CLIENT_STORE_TEMPORARY_CREDENTIAL")
                          .asBoolean());
                  assertThat(
                      "authenticator",
                      jsonNode.path("data").path("AUTHENTICATOR").asText(),
                      equalTo(ID_TOKEN_AUTHENTICATOR));
                  assertThat(
                      "idToken",
                      jsonNode.path("data").path("TOKEN").asText(),
                      equalTo(MOCK_ID_TOKEN));
                  resp = retSecondAuthentication;
                }

                callCount++;
                return resp;
              }
            });
  }

  private void initMockSessionUtilExternalBrowser(
      MockedStatic<SessionUtilExternalBrowser> mockedSessionUtilExternalBrowser,
      SFLoginInput loginInput) {
    SessionUtilExternalBrowser fakeExternalBrowser = new FakeSessionUtilExternalBrowser(loginInput);
    mockedSessionUtilExternalBrowser
        .when(() -> SessionUtilExternalBrowser.createInstance(Mockito.any(SFLoginInput.class)))
        .thenReturn(fakeExternalBrowser);
  }

  private SFLoginInput initMockLoginInput() {
    SFLoginInput loginInput = mock(SFLoginInput.class);
    when(loginInput.getServerUrl()).thenReturn("https://testaccount.snowflakecomputing.com/");
    when(loginInput.getAuthenticator())
        .thenReturn(ClientAuthnDTO.AuthenticatorType.EXTERNALBROWSER.name());
    when(loginInput.getAccountName()).thenReturn("testaccount");
    when(loginInput.getUserName()).thenReturn("testuser");
    when(loginInput.getDisableConsoleLogin()).thenReturn(true);
    return loginInput;
  }

  @Test
  public void testIdTokenInSSO() throws Throwable {
    try (MockedStatic<HttpUtil> mockedHttpUtil = mockStatic(HttpUtil.class);
        MockedStatic<SessionUtilExternalBrowser> mockedSessionUtilExternalBrowser =
            mockStatic(SessionUtilExternalBrowser.class)) {

      initMock(mockedHttpUtil, mockedSessionUtilExternalBrowser);
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
      assertThat("token", sfcon.getSfSession().getSessionToken(), equalTo(MOCK_SESSION_TOKEN));
      assertThat("idToken", sfcon.getSfSession().getIdToken(), equalTo(MOCK_ID_TOKEN));

      // second connection reads the cache and use the id token to get the
      // session token.
      Connection conSecond = DriverManager.getConnection(url, properties);
      SnowflakeConnectionV1 sfconSecond = (SnowflakeConnectionV1) conSecond;
      assertThat(
          "token", sfconSecond.getSfSession().getSessionToken(), equalTo(MOCK_NEW_SESSION_TOKEN));
      // we won't get a new id_token here
      assertThat("idToken", sfcon.getSfSession().getIdToken(), equalTo(MOCK_ID_TOKEN));
    }
  }
}
