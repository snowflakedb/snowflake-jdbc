package net.snowflake.client.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import net.snowflake.client.core.auth.AuthenticatorType;
import net.snowflake.client.jdbc.BaseWiremockTest;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

public class OAuthTokenCacheLatestIT extends BaseWiremockTest {

  private static final String SCENARIOS_BASE_DIR = MAPPINGS_BASE_DIR + "/oauth/token_caching";
  private static final String CACHING_TOKENS_AFTER_CONNECTING_SCENARIO_MAPPINGS =
      SCENARIOS_BASE_DIR + "/caching_tokens_after_connecting.json";
  private static final String REUSING_CACHED_ACCESS_TOKEN_SCENARIO_MAPPINGS =
      SCENARIOS_BASE_DIR + "/reusing_cached_access_token_to_authenticate.json";
  private static final String REFRESHING_EXPIRED_ACCESS_TOKEN_SCENARIO_MAPPINGS =
      SCENARIOS_BASE_DIR + "/refreshing_expired_access_token.json";
  private static final String
      CACHING_REFRESHED_ACCESS_TOKEN_AND_NEW_REFRESH_TOKEN_SCENARIO_MAPPINGS =
          SCENARIOS_BASE_DIR + "/caching_refreshed_access_token_and_new_refresh_token.json";
  private static final String RESTARTING_FULL_FLOW_ON_EXPIRATION_AND_ERROR_WHEN_REFRESHING =
      SCENARIOS_BASE_DIR + "/restarting_full_flow_on_refresh_token_error.json";
  private static final String RESTARTING_FULL_FLOW_ON_EXPIRATION_AND_NO_REFRESH_TOKEN =
      SCENARIOS_BASE_DIR + "/restarting_full_flow_on_expiration_and_no_refresh_token.json";

  @Test
  public void shouldCacheAccessTokenAfterConnecting() throws SFException, SnowflakeSQLException {
    importMappingFromResources(CACHING_TOKENS_AFTER_CONNECTING_SCENARIO_MAPPINGS);
    CredentialManager credentialManager = mock(CredentialManager.class);
    try (MockedStatic<CredentialManager> credentialManagerMockedStatic =
        mockStatic(CredentialManager.class)) {
      credentialManagerMockedStatic
          .when(CredentialManager::getInstance)
          .thenReturn(credentialManager);
      SFLoginInput loginInput = createLoginInputStub();
      SessionUtil.openSession(loginInput, new HashMap<>(), "INFO");

      captureAndAssertSavedTokenValues(credentialManager, "access-token-123", "refresh-token-123");
    }
  }

  @Test
  public void shouldReuseCachedAccessTokenWhenConnecting()
      throws SFException, SnowflakeSQLException {
    importMappingFromResources(REUSING_CACHED_ACCESS_TOKEN_SCENARIO_MAPPINGS);
    CredentialManager credentialManager = mock(CredentialManager.class);
    mockLoadingTokensFromCache(
        credentialManager, "reused-access-token-123", "reused-refresh-token-123");

    try (MockedStatic<CredentialManager> credentialManagerMockedStatic =
        mockStatic(CredentialManager.class)) {
      credentialManagerMockedStatic
          .when(CredentialManager::getInstance)
          .thenReturn(credentialManager);
      SFLoginInput loginInput = createLoginInputStub();
      SFLoginOutput loginOutput = SessionUtil.openSession(loginInput, new HashMap<>(), "INFO");
      assertEquals("reused-access-token-123", loginOutput.getOauthAccessToken());
      assertEquals("reused-refresh-token-123", loginOutput.getOauthRefreshToken());
    }
  }

  @Test
  public void shouldRefreshExpiredAccessTokenAndConnectSuccessfully()
      throws SFException, SnowflakeSQLException {
    importMappingFromResources(REFRESHING_EXPIRED_ACCESS_TOKEN_SCENARIO_MAPPINGS);
    CredentialManager credentialManager = mock(CredentialManager.class);
    mockLoadingTokensFromCache(
        credentialManager, "expired-access-token-123", "some-refresh-token-123");

    try (MockedStatic<CredentialManager> credentialManagerMockedStatic =
        mockStatic(CredentialManager.class)) {
      credentialManagerMockedStatic
          .when(CredentialManager::getInstance)
          .thenReturn(credentialManager);
      SFLoginInput loginInput = createLoginInputStub();
      SFLoginOutput loginOutput = SessionUtil.openSession(loginInput, new HashMap<>(), "INFO");

      verify(credentialManager, times(1))
          .deleteOAuthAccessTokenCache(loginInput.getHostFromServerUrl(), loginInput.getUserName());
      verify(credentialManager, never())
          .deleteOAuthRefreshTokenCache(
              loginInput.getHostFromServerUrl(), loginInput.getUserName());
      assertEquals("new-refreshed-access-token-123", loginOutput.getOauthAccessToken());
      captureAndAssertSavedTokenValues(
          credentialManager, "new-refreshed-access-token-123", "some-refresh-token-123");
    }
  }

  @Test
  public void shouldCacheRefreshedAccessTokenAndNewRefreshToken()
      throws SFException, SnowflakeSQLException {
    importMappingFromResources(
        CACHING_REFRESHED_ACCESS_TOKEN_AND_NEW_REFRESH_TOKEN_SCENARIO_MAPPINGS);
    CredentialManager credentialManager = mock(CredentialManager.class);
    mockLoadingTokensFromCache(
        credentialManager, "expired-access-token-123", "some-refresh-token-123");

    try (MockedStatic<CredentialManager> credentialManagerMockedStatic =
        mockStatic(CredentialManager.class)) {
      credentialManagerMockedStatic
          .when(CredentialManager::getInstance)
          .thenReturn(credentialManager);
      SFLoginInput loginInput = createLoginInputStub();
      SFLoginOutput loginOutput = SessionUtil.openSession(loginInput, new HashMap<>(), "INFO");

      verify(credentialManager, times(1))
          .deleteOAuthAccessTokenCache(loginInput.getHostFromServerUrl(), loginInput.getUserName());
      assertEquals("new-refreshed-access-token-123", loginOutput.getOauthAccessToken());
      captureAndAssertSavedTokenValues(
          credentialManager, "new-refreshed-access-token-123", "new-refresh-token-123");
    }
  }

  @Test
  public void shouldRestartFullFlowOnAccessTokenExpirationAndErrorWhenRefreshing()
      throws SFException, SnowflakeSQLException {
    importMappingFromResources(RESTARTING_FULL_FLOW_ON_EXPIRATION_AND_ERROR_WHEN_REFRESHING);
    CredentialManager credentialManager = mock(CredentialManager.class);
    mockLoadingTokensFromCache(
        credentialManager, "expired-access-token-123", "some-refresh-token-123");

    try (MockedStatic<CredentialManager> credentialManagerMockedStatic =
        mockStatic(CredentialManager.class)) {
      credentialManagerMockedStatic
          .when(CredentialManager::getInstance)
          .thenReturn(credentialManager);
      SFLoginInput loginInput = createLoginInputStub();
      SFLoginOutput loginOutput = SessionUtil.openSession(loginInput, new HashMap<>(), "INFO");

      verify(credentialManager, times(1))
          .deleteOAuthAccessTokenCache(loginInput.getHostFromServerUrl(), loginInput.getUserName());
      verify(credentialManager, times(1))
          .deleteOAuthRefreshTokenCache(
              loginInput.getHostFromServerUrl(), loginInput.getUserName());
      assertEquals("newly-obtained-access-token-123", loginOutput.getOauthAccessToken());
      captureAndAssertSavedTokenValues(
          credentialManager, "newly-obtained-access-token-123", "newly-obtained-refresh-token");
    }
  }

  @Test
  public void shouldRestartFullFlowOnAccessTokenExpirationAndNoRefreshToken()
      throws SFException, SnowflakeSQLException {
    importMappingFromResources(RESTARTING_FULL_FLOW_ON_EXPIRATION_AND_NO_REFRESH_TOKEN);
    CredentialManager credentialManager = mock(CredentialManager.class);
    mockLoadingTokensFromCache(credentialManager, "expired-access-token-123", null);

    try (MockedStatic<CredentialManager> credentialManagerMockedStatic =
        mockStatic(CredentialManager.class)) {
      credentialManagerMockedStatic
          .when(CredentialManager::getInstance)
          .thenReturn(credentialManager);
      SFLoginInput loginInput = createLoginInputStub();
      SFLoginOutput loginOutput = SessionUtil.openSession(loginInput, new HashMap<>(), "INFO");

      verify(credentialManager, times(1))
          .deleteOAuthAccessTokenCache(loginInput.getHostFromServerUrl(), loginInput.getUserName());
      verify(credentialManager, never())
          .deleteOAuthRefreshTokenCache(
              loginInput.getHostFromServerUrl(), loginInput.getUserName());
      assertEquals("newly-obtained-access-token-123", loginOutput.getOauthAccessToken());
      captureAndAssertSavedTokenValues(credentialManager, "newly-obtained-access-token-123", null);
    }
  }

  private SFLoginInput createLoginInputStub() {
    SFLoginInput input = new SFLoginInput();
    input.setAuthenticator(AuthenticatorType.OAUTH_CLIENT_CREDENTIALS.name());
    input.setOriginAuthenticator(AuthenticatorType.OAUTH_CLIENT_CREDENTIALS.name());
    input.setServerUrl(String.format("http://%s:%d/", WIREMOCK_HOST, wiremockHttpPort));
    input.setUserName("MOCK_USERNAME");
    input.setAccountName("MOCK_ACCOUNT_NAME");
    input.setAppId("MOCK_APP_ID");
    input.setAppVersion("MOCK_APP_VERSION");
    input.setOCSPMode(OCSPMode.FAIL_OPEN);
    input.setHttpClientSettingsKey(new HttpClientSettingsKey(OCSPMode.FAIL_OPEN));
    input.setBrowserResponseTimeout(Duration.ofSeconds(20));
    input.setLoginTimeout(1000);
    input.setSessionParameters(Collections.singletonMap("CLIENT_STORE_TEMPORARY_CREDENTIAL", true));
    input.setOauthLoginInput(
        new SFOauthLoginInput(
            "123",
            "123",
            null,
            null,
            String.format("http://%s:%d/oauth/token-request", WIREMOCK_HOST, wiremockHttpPort),
            "session:role:ANALYST"));
    return input;
  }

  private static void mockLoadingTokensFromCache(
      CredentialManager credentialManagerMock, String oauthAccessToken, String oauthRefreshToken)
      throws SFException {
    doAnswer(
            invocation -> {
              ((SFLoginInput) invocation.getArguments()[0]).setOauthAccessToken(oauthAccessToken);
              return null;
            })
        .when(credentialManagerMock)
        .fillCachedOAuthAccessToken(any(SFLoginInput.class));
    doAnswer(
            invocation -> {
              ((SFLoginInput) invocation.getArguments()[0]).setOauthRefreshToken(oauthRefreshToken);
              return null;
            })
        .when(credentialManagerMock)
        .fillCachedOAuthRefreshToken(any(SFLoginInput.class));
  }

  private static void captureAndAssertSavedTokenValues(
      CredentialManager credentialManagerMock,
      String expectedAccessToken,
      String expectedRefreshToken)
      throws SFException {
    ArgumentCaptor<SFLoginInput> accessTokenInputCaptor =
        ArgumentCaptor.forClass(SFLoginInput.class);
    verify(credentialManagerMock).writeOAuthAccessToken(accessTokenInputCaptor.capture());
    assertEquals(expectedAccessToken, accessTokenInputCaptor.getValue().getOauthAccessToken());

    if (expectedRefreshToken != null) {
      ArgumentCaptor<SFLoginInput> refreshTokenInputCaptor =
          ArgumentCaptor.forClass(SFLoginInput.class);
      verify(credentialManagerMock).writeOAuthRefreshToken(refreshTokenInputCaptor.capture());
      assertEquals(expectedRefreshToken, refreshTokenInputCaptor.getValue().getOauthRefreshToken());
    } else {
      verify(credentialManagerMock, never()).writeOAuthRefreshToken(any(SFLoginInput.class));
    }
  }
}
