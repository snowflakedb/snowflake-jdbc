/*
 * Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import static net.snowflake.client.TestUtil.systemGetEnv;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import net.snowflake.client.category.TestCategoryCore;
import net.snowflake.client.jdbc.BaseJDBCTest;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.common.core.ClientAuthnDTO;
import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.MockedStatic;
import org.mockito.MockedStatic.Verification;
import org.mockito.Mockito;

@Category(TestCategoryCore.class)
public class SessionUtilLatestIT extends BaseJDBCTest {

  /**
   * Tests the JWT renew functionality when retrying login requests. To run, update environment
   * variables to use connect with JWT authentication.
   *
   * @throws SFException
   * @throws SnowflakeSQLException
   */
  @Ignore
  @Test
  public void testJwtAuthTimeoutRetry() throws SFException, SnowflakeSQLException {
    final SFLoginInput loginInput = initMockLoginInput();
    Map<SFSessionProperty, Object> connectionPropertiesMap = initConnectionPropertiesMap();
    MockedStatic<HttpUtil> mockedHttpUtil = mockStatic(HttpUtil.class);
    SnowflakeSQLException ex =
        new SnowflakeSQLException(ErrorCode.AUTHENTICATOR_REQUEST_TIMEOUT, 0, true, 0);

    mockedHttpUtil
        .when(
            () ->
                HttpUtil.executeGeneralRequest(
                    Mockito.any(HttpRequestBase.class),
                    Mockito.anyInt(),
                    Mockito.anyInt(),
                    Mockito.anyInt(),
                    Mockito.anyInt(),
                    Mockito.nullable(HttpClientSettingsKey.class)))
        .thenThrow(ex) // fail first
        .thenReturn(
            "{\"data\":null,\"code\":null,\"message\":null,\"success\":true}"); // succeed on retry

    SessionUtil.openSession(loginInput, connectionPropertiesMap, "ALL");
  }

  /**
   * Mock SFLoginInput
   *
   * @return a mock object for SFLoginInput
   */
  private SFLoginInput initMockLoginInput() {
    // mock SFLoginInput
    SFLoginInput loginInput = mock(SFLoginInput.class);
    when(loginInput.getServerUrl()).thenReturn(systemGetEnv("SNOWFLAKE_TEST_HOST"));
    when(loginInput.getAuthenticator())
        .thenReturn(ClientAuthnDTO.AuthenticatorType.SNOWFLAKE_JWT.name());
    when(loginInput.getPrivateKeyFile())
        .thenReturn(systemGetEnv("SNOWFLAKE_TEST_PRIVATE_KEY_FILE"));
    when(loginInput.getPrivateKeyFilePwd())
        .thenReturn(systemGetEnv("SNOWFLAKE_TEST_PRIVATE_KEY_FILE_PWD"));
    when(loginInput.getUserName()).thenReturn(systemGetEnv("SNOWFLAKE_TEST_USER"));
    when(loginInput.getAccountName()).thenReturn("testaccount");
    when(loginInput.getAppId()).thenReturn("testid");
    when(loginInput.getOCSPMode()).thenReturn(OCSPMode.FAIL_OPEN);
    when(loginInput.getHttpClientSettingsKey())
        .thenReturn(new HttpClientSettingsKey(OCSPMode.FAIL_OPEN));
    return loginInput;
  }

  /**
   * Initialize the connection properties map.
   *
   * @return connectionPropertiesMap
   */
  private Map<SFSessionProperty, Object> initConnectionPropertiesMap() {
    Map<SFSessionProperty, Object> connectionPropertiesMap = new HashMap<>();
    connectionPropertiesMap.put(SFSessionProperty.TRACING, "ALL");
    return connectionPropertiesMap;
  }

  @Test
  public void testConvertSystemPropertyToIntValue() {
    // SNOW-760642 - Test that new default for net.snowflake.jdbc.ttl is 60 seconds.
    assertEquals(
        60, HttpUtil.convertSystemPropertyToIntValue(HttpUtil.JDBC_TTL, HttpUtil.DEFAULT_TTL));

    // Test that TTL can be disabled
    System.setProperty(HttpUtil.JDBC_TTL, "-1");
    assertEquals(
        -1, HttpUtil.convertSystemPropertyToIntValue(HttpUtil.JDBC_TTL, HttpUtil.DEFAULT_TTL));
  }

  /**
   * SNOW-862760 Tests that when additional headers are set on a login request, they are forwarded
   * to the recipient.
   */
  @Test
  public void testForwardedHeaders() throws Throwable {
    SFLoginInput input = createLoginInput();
    Map<String, String> additionalHeaders = new HashMap<>();
    additionalHeaders.put("Extra-Snowflake-Header", "present");

    input.setAdditionalHttpHeadersForSnowsight(additionalHeaders);

    Map<SFSessionProperty, Object> connectionPropertiesMap = initConnectionPropertiesMap();
    try (MockedStatic<HttpUtil> mockedHttpUtil = mockStatic(HttpUtil.class)) {
      // Both mocks the call _and_ verifies that the headers are forwarded.
      Verification httpCalledWithHeaders =
          () ->
              HttpUtil.executeGeneralRequest(
                  Mockito.argThat(
                      arg -> {
                        for (Entry<String, String> definedHeader : additionalHeaders.entrySet()) {
                          Header actualHeader = arg.getLastHeader(definedHeader.getKey());
                          if (actualHeader == null) {
                            return false;
                          }

                          if (!definedHeader.getValue().equals(actualHeader.getValue())) {
                            return false;
                          }
                        }

                        return true;
                      }),
                  Mockito.anyInt(),
                  Mockito.anyInt(),
                  Mockito.anyInt(),
                  Mockito.anyInt(),
                  Mockito.nullable(HttpClientSettingsKey.class));
      mockedHttpUtil
          .when(httpCalledWithHeaders)
          .thenReturn("{\"data\":null,\"code\":null,\"message\":null,\"success\":true}");

      mockedHttpUtil
          .when(() -> HttpUtil.applyAdditionalHeadersForSnowsight(any(), any()))
          .thenCallRealMethod();

      SessionUtil.openSession(input, connectionPropertiesMap, "ALL");

      // After login, the only invocation to http should have been with the new
      // headers.
      // No calls should have happened without additional headers.
      mockedHttpUtil.verify(times(1), httpCalledWithHeaders);
    }
  }

  /**
   * SNOW-862760 Verifies that, if inFlightCtx is provided to the login input, it's forwarded as
   * part of the message body.
   */
  @Test
  public void testForwardInflightCtx() throws Throwable {
    SFLoginInput input = createLoginInput();
    String inflightCtx = UUID.randomUUID().toString();
    input.setInFlightCtx(inflightCtx);

    Map<SFSessionProperty, Object> connectionPropertiesMap = initConnectionPropertiesMap();

    try (MockedStatic<HttpUtil> mockedHttpUtil = mockStatic(HttpUtil.class)) {
      // Both mocks the call _and_ verifies that the headers are forwarded.
      Verification httpCalledWithHeaders =
          () ->
              HttpUtil.executeGeneralRequest(
                  Mockito.argThat(
                      arg -> {
                        try {
                          // This gets tricky because the entity is a string.
                          // To not fail on JSON parsing changes, we'll verify that the key
                          // inFlightCtx is present and the random UUID body
                          HttpEntity entity = ((HttpPost) arg).getEntity();
                          InputStream is = entity.getContent();
                          ByteArrayOutputStream out = new ByteArrayOutputStream();
                          IOUtils.copy(is, out);
                          String body = new String(out.toByteArray());
                          return body.contains("inFlightCtx") && body.contains(inflightCtx);
                        } catch (UnsupportedOperationException | IOException e) {
                        }
                        return false;
                      }),
                  Mockito.anyInt(),
                  Mockito.anyInt(),
                  Mockito.anyInt(),
                  Mockito.anyInt(),
                  Mockito.nullable(HttpClientSettingsKey.class));
      mockedHttpUtil
          .when(httpCalledWithHeaders)
          .thenReturn("{\"data\":null,\"code\":null,\"message\":null,\"success\":true}");

      mockedHttpUtil
          .when(() -> HttpUtil.applyAdditionalHeadersForSnowsight(any(), any()))
          .thenCallRealMethod();

      SessionUtil.openSession(input, connectionPropertiesMap, "ALL");

      // After login, the only invocation to http should have been with the new
      // headers.
      // No calls should have happened without additional headers.
      mockedHttpUtil.verify(times(1), httpCalledWithHeaders);
    }
  }

  private SFLoginInput createLoginInput() {
    SFLoginInput input = new SFLoginInput();
    input.setServerUrl("MOCK_TEST_HOST");
    input.setUserName("MOCK_USERNAME");
    input.setPassword("MOCK_PASSWORD");
    input.setAccountName("MOCK_ACCOUNT_NAME");
    input.setAppId("MOCK_APP_ID");
    input.setOCSPMode(OCSPMode.FAIL_OPEN);
    input.setHttpClientSettingsKey(new HttpClientSettingsKey(OCSPMode.FAIL_OPEN));
    input.setLoginTimeout(1000);
    input.setSessionParameters(new HashMap<>());

    return input;
  }
}
