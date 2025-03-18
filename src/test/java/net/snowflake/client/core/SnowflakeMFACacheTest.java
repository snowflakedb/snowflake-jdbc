package net.snowflake.client.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import net.snowflake.client.AbstractDriverIT;
import net.snowflake.client.jdbc.SnowflakeBasicDataSource;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.HttpPost;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class SnowflakeMFACacheTest {
  private static final ObjectMapper mapper = ObjectMapperFactory.getObjectMapper();
  private static final String[] mockedMfaToken = {"mockedMfaToken0", "mockedMfaToken1"};
  private static final String host = "TESTACCOUNT.SNOWFLAKECOMPUTING.COM";
  private static final String account = "testaccount";
  private static final String user = "testuser";
  private static final String pwd = "testpassword";
  private static final String authenticator = "username_password_mfa";
  private static final boolean client_request_mfa_token = true;

  private ObjectNode getNormalMockedHttpResponse(boolean success, int mfaTokenIdx) {
    ObjectNode respNode = mapper.createObjectNode();
    ObjectNode dataNode = mapper.createObjectNode();
    ArrayNode paraArray = mapper.createArrayNode();
    ObjectNode autocommit = mapper.createObjectNode();

    autocommit.put("name", "AUTOCOMMIT");
    autocommit.put("value", true);
    paraArray.add(autocommit);

    dataNode.set("parameters", paraArray);
    dataNode.put("masterToken", "mockedMasterToken");
    dataNode.put("token", "mockedToken");
    if (mfaTokenIdx >= 0) {
      dataNode.put("mfaToken", mockedMfaToken[mfaTokenIdx]);
    }

    respNode.set("data", dataNode);
    respNode.put("success", success);
    respNode.put("message", "msg");

    return respNode;
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

  private Properties getBaseProp() {
    Properties prop = new Properties();
    prop.put("account", account);
    prop.put("user", user);
    prop.put("password", pwd);
    prop.put("authenticator", authenticator);
    prop.put("CLIENT_REQUEST_MFA_TOKEN", client_request_mfa_token);
    return prop;
  }

  @Test
  public void testMFAFunctionality() throws SQLException {
    SessionUtil.deleteMfaTokenCache(host, user);
    try (MockedStatic<HttpUtil> mockedHttpUtil = Mockito.mockStatic(HttpUtil.class)) {
      mockedHttpUtil
          .when(
              () ->
                  HttpUtil.executeGeneralRequest(
                      any(HttpPost.class),
                      anyInt(),
                      anyInt(),
                      anyInt(),
                      anyInt(),
                      any(HttpClientSettingsKey.class)))
          .thenAnswer(
              new Answer<String>() {
                int callCount = 0;

                @Override
                public String answer(InvocationOnMock invocation) throws Throwable {
                  String res;
                  JsonNode jsonNode;
                  final Object[] args = invocation.getArguments();

                  if (callCount == 0) {
                    // First connection request
                    jsonNode = parseRequest((HttpPost) args[0]);
                    assertTrue(
                        jsonNode
                            .path("data")
                            .path("SESSION_PARAMETERS")
                            .path("CLIENT_REQUEST_MFA_TOKEN")
                            .asBoolean());
                    // return first mfa token
                    res = getNormalMockedHttpResponse(true, 0).toString();
                  } else if (callCount == 1) {
                    // First close() request
                    res = getNormalMockedHttpResponse(true, -1).toString();
                  } else if (callCount == 2) {
                    // Second connection request
                    jsonNode = parseRequest((HttpPost) args[0]);
                    assertTrue(
                        jsonNode
                            .path("data")
                            .path("SESSION_PARAMETERS")
                            .path("CLIENT_REQUEST_MFA_TOKEN")
                            .asBoolean());
                    assertEquals(mockedMfaToken[0], jsonNode.path("data").path("TOKEN").asText());
                    // Normally backend won't send a new mfa token in this case. For testing
                    // purpose, we issue a new token to test whether the mfa token can be
                    // refreshed
                    // when receiving a new one from server.
                    res = getNormalMockedHttpResponse(true, 1).toString();
                  } else if (callCount == 3) {
                    // Second close() request
                    res = getNormalMockedHttpResponse(true, -1).toString();
                  } else if (callCount == 4) {
                    // Third connection request
                    // Check for the new mfa token
                    jsonNode = parseRequest((HttpPost) args[0]);
                    assertTrue(
                        jsonNode
                            .path("data")
                            .path("SESSION_PARAMETERS")
                            .path("CLIENT_REQUEST_MFA_TOKEN")
                            .asBoolean());
                    assertEquals(mockedMfaToken[1], jsonNode.path("data").path("TOKEN").asText());
                    res = getNormalMockedHttpResponse(true, -1).toString();
                  } else if (callCount == 5) {
                    // Third close() request
                    res = getNormalMockedHttpResponse(true, -1).toString();
                  } else if (callCount == 6) {
                    // test if failed log in response can delete the cached mfa token
                    res = getNormalMockedHttpResponse(false, -1).toString();
                  } else if (callCount == 7) {
                    jsonNode = parseRequest((HttpPost) args[0]);
                    assertTrue(
                        jsonNode
                            .path("data")
                            .path("SESSION_PARAMETERS")
                            .path("CLIENT_REQUEST_MFA_TOKEN")
                            .asBoolean());
                    // no token should be included this time.
                    assertEquals("", jsonNode.path("data").path("TOKEN").asText());
                    res = getNormalMockedHttpResponse(true, -1).toString();
                  } else if (callCount == 8) {
                    // final close()
                    res = getNormalMockedHttpResponse(true, -1).toString();
                  } else {
                    // unexpected request
                    res = getNormalMockedHttpResponse(false, -1).toString();
                  }

                  callCount += 1; // this will be incremented on both connecting and closing
                  return res;
                }
              });

      Properties prop = getBaseProp();

      // connect url
      String url = "jdbc:snowflake://testaccount.snowflakecomputing.com";

      // The first connection contains no mfa token. After the connection, a mfa token will be
      // saved
      Connection con = DriverManager.getConnection(url, prop);
      con.close();

      // The second connection is expected to include the mfa token issued for the first
      // connection
      // and a new mfa token is issued
      Connection con1 = DriverManager.getConnection(url, prop);
      con1.close();

      // The third connection is expected to include the new mfa token.
      Connection con2 = DriverManager.getConnection(url, prop);
      con2.close();

      // This connection would receive an exception and then should clean up the mfa cache
      assertThrows(SnowflakeSQLException.class, () -> DriverManager.getConnection(url, prop));

      // This connect request should not contain mfa cached token
      Connection con4 = DriverManager.getConnection(url, prop);
      con4.close();
    }
    SessionUtil.deleteMfaTokenCache(host, user);
  }

  class MockUnavailableAdvapi32Lib implements SecureStorageWindowsManager.Advapi32Lib {
    @Override
    public boolean CredReadW(String targetName, int type, int flags, PointerByReference pcred) {
      return false;
    }

    @Override
    public boolean CredWriteW(
        SecureStorageWindowsManager.SecureStorageWindowsCredential cred, int flags) {
      return false;
    }

    @Override
    public boolean CredDeleteW(String targetName, int type, int flags) {
      return false;
    }

    @Override
    public void CredFree(Pointer cred) {}
  }

  private void unavailableLSSWindowsTestBody() throws SQLException {
    SessionUtil.deleteMfaTokenCache(host, user);
    try (MockedStatic<HttpUtil> mockedHttpUtil = Mockito.mockStatic(HttpUtil.class)) {
      mockedHttpUtil
          .when(
              () ->
                  HttpUtil.executeGeneralRequest(
                      any(HttpPost.class),
                      anyInt(),
                      anyInt(),
                      anyInt(),
                      anyInt(),
                      any(HttpClientSettingsKey.class)))
          .thenAnswer(
              new Answer<String>() {
                int callCount = 0;

                private String validationHelper(Object[] args) throws IOException {
                  JsonNode node = parseRequest((HttpPost) args[0]);
                  assertTrue(
                      node.path("data")
                          .path("SESSION_PARAMETERS")
                          .path("CLIENT_REQUEST_MFA_TOKEN")
                          .asBoolean());
                  // no token should be included.
                  assertEquals("", node.path("data").path("TOKEN").asText());
                  return getNormalMockedHttpResponse(true, 0).toString();
                }

                @Override
                public String answer(InvocationOnMock invocation) throws Throwable {
                  String res = "";
                  final Object[] args = invocation.getArguments();

                  if (callCount == 0) {
                    // First connection request
                    res = validationHelper(args);
                  } else if (callCount == 1) {
                    // First close() request
                    res = getNormalMockedHttpResponse(true, -1).toString();
                  } else if (callCount == 2) {
                    // Second connection request
                    res = validationHelper(args);
                  } else if (callCount == 3) {
                    // Second close() request
                    res = getNormalMockedHttpResponse(true, -1).toString();
                  }
                  callCount += 1; // this will be incremented on both connecting and closing
                  return res;
                }
              });

      Properties prop = getBaseProp();
      String url = "jdbc:snowflake://testaccount.snowflakecomputing.com";

      // Both of below two connections will try to use unavailable secure local storage to store mfa
      // cache. We need to make sure this situation won't break.
      Connection con = DriverManager.getConnection(url, prop);
      con.close();
      Connection con1 = DriverManager.getConnection(url, prop);
      con1.close();
    }
    SessionUtil.deleteMfaTokenCache(host, user);
  }

  private void testUnavailableLSSWindowsHelper() throws SQLException {
    try {
      SecureStorageWindowsManager.Advapi32LibManager.setInstance(new MockUnavailableAdvapi32Lib());
      SecureStorageWindowsManager manager = SecureStorageWindowsManager.builder();
      CredentialManager.injectSecureStorageManager(manager);
      unavailableLSSWindowsTestBody();
    } finally {
      SecureStorageWindowsManager.Advapi32LibManager.resetInstance();
    }
  }

  @Test
  public void testUnavailableLocalSecureStorage() throws SQLException {
    try {
      testUnavailableLSSWindowsHelper();
    } finally {
      CredentialManager.resetSecureStorageManager();
    }
  }

  // Run this test manually to test disabling the client request MFA token. Use an MFA
  // authentication enabled user. This is valid for versions after 3.18.0.
  @Test
  @Disabled
  public void testEnableClientRequestMfaToken() throws SQLException {
    Map<String, String> params = AbstractDriverIT.getConnectionParameters();
    SnowflakeBasicDataSource ds = new SnowflakeBasicDataSource();
    ds.setServerName(params.get("host"));
    ds.setAccount(params.get("account"));
    ds.setPortNumber(Integer.parseInt(params.get("port")));
    ds.setUser(params.get("user"));
    ds.setPassword(params.get("password"));
    ds.setEnableClientRequestMfaToken(false);

    for (int i = 0; i < 3; i++) {
      try (Connection con = ds.getConnection();
          ResultSet rs = con.createStatement().executeQuery("SELECT 1")) {
        assertTrue(rs.next());
      }
    }
  }
}
