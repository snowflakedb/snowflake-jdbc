/*
 * Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import static net.snowflake.client.TestUtil.systemGetEnv;
import static org.mockito.Mockito.*;

import java.util.HashMap;
import java.util.Map;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.common.core.ClientAuthnDTO;
import org.apache.http.client.methods.HttpRequestBase;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class SessionUtilLatestIT {

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
}
