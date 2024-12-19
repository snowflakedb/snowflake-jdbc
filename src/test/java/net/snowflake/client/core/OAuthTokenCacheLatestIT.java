package net.snowflake.client.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import net.snowflake.client.core.auth.AuthenticatorType;
import net.snowflake.client.jdbc.BaseWiremockTest;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.stubbing.Answer;

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
    try (MockedStatic<CredentialManager> credentialManagerMockedStatic =
        mockStatic(CredentialManager.class)) {
      SFLoginInput loginInput = createLoginInputStub();
      SessionUtil.openSession(loginInput, new HashMap<>(), "INFO");
      captureAndAssertSavedTokenValues(
          credentialManagerMockedStatic, "access-token-123", "refresh-token-123");
    }
  }

  @Test
  public void shouldReuseCachedAccessTokenWhenConnecting()
      throws SFException, SnowflakeSQLException {
    importMappingFromResources(REUSING_CACHED_ACCESS_TOKEN_SCENARIO_MAPPINGS);
    try (MockedStatic<CredentialManager> credentialManagerMockedStatic =
        mockStatic(CredentialManager.class)) {
      mockLoadingTokensFromCache(
          credentialManagerMockedStatic, "reused-access-token-123", "reused-refresh-token-123");
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
    try (MockedStatic<CredentialManager> credentialManagerMockedStatic =
        mockStatic(CredentialManager.class)) {
      mockLoadingTokensFromCache(
          credentialManagerMockedStatic, "expired-access-token-123", "some-refresh-token-123");
      SFLoginInput loginInput = createLoginInputStub();
      SFLoginOutput loginOutput = SessionUtil.openSession(loginInput, new HashMap<>(), "INFO");

      credentialManagerMockedStatic.verify(
          () ->
              CredentialManager.deleteOAuthAccessTokenCache(
                  loginInput.getHostFromServerUrl(), loginInput.getUserName()),
          times(1));
      credentialManagerMockedStatic.verify(
          () ->
              CredentialManager.deleteOAuthRefreshTokenCache(
                  loginInput.getHostFromServerUrl(), loginInput.getUserName()),
          never());

      assertEquals("new-refreshed-access-token-123", loginOutput.getOauthAccessToken());
      captureAndAssertSavedTokenValues(
          credentialManagerMockedStatic,
          "new-refreshed-access-token-123",
          "some-refresh-token-123");
    }
  }

  @Test
  public void shouldCacheRefreshedAccessTokenAndNewRefreshToken()
      throws SFException, SnowflakeSQLException {
    importMappingFromResources(
        CACHING_REFRESHED_ACCESS_TOKEN_AND_NEW_REFRESH_TOKEN_SCENARIO_MAPPINGS);

    try (MockedStatic<CredentialManager> credentialManagerMockedStatic =
        mockStatic(CredentialManager.class)) {
      mockLoadingTokensFromCache(
          credentialManagerMockedStatic, "expired-access-token-123", "some-refresh-token-123");
      SFLoginInput loginInput = createLoginInputStub();
      SFLoginOutput loginOutput = SessionUtil.openSession(loginInput, new HashMap<>(), "INFO");

      credentialManagerMockedStatic.verify(
          () ->
              CredentialManager.deleteOAuthAccessTokenCache(
                  loginInput.getHostFromServerUrl(), loginInput.getUserName()),
          times(1));
      assertEquals("new-refreshed-access-token-123", loginOutput.getOauthAccessToken());
      captureAndAssertSavedTokenValues(
          credentialManagerMockedStatic, "new-refreshed-access-token-123", "new-refresh-token-123");
    }
  }

  @Test
  public void shouldRestartFullFlowOnAccessTokenExpirationAndErrorWhenRefreshing()
      throws SFException, SnowflakeSQLException {
    importMappingFromResources(RESTARTING_FULL_FLOW_ON_EXPIRATION_AND_ERROR_WHEN_REFRESHING);
    try (MockedStatic<CredentialManager> credentialManagerMockedStatic =
        mockStatic(CredentialManager.class)) {
      mockLoadingTokensFromCache(
          credentialManagerMockedStatic, "expired-access-token-123", "some-refresh-token-123");
      SFLoginInput loginInput = createLoginInputStub();
      SFLoginOutput loginOutput = SessionUtil.openSession(loginInput, new HashMap<>(), "INFO");

      credentialManagerMockedStatic.verify(
          () ->
              CredentialManager.deleteOAuthAccessTokenCache(
                  loginInput.getHostFromServerUrl(), loginInput.getUserName()),
          times(1));
      credentialManagerMockedStatic.verify(
          () ->
              CredentialManager.deleteOAuthRefreshTokenCache(
                  loginInput.getHostFromServerUrl(), loginInput.getUserName()),
          times(1));
      assertEquals("newly-obtained-access-token-123", loginOutput.getOauthAccessToken());
      captureAndAssertSavedTokenValues(
          credentialManagerMockedStatic,
          "newly-obtained-access-token-123",
          "newly-obtained-refresh-token");
    }
  }

  @Test
  public void shouldRestartFullFlowOnAccessTokenExpirationAndNoRefreshToken()
      throws SFException, SnowflakeSQLException {
    importMappingFromResources(RESTARTING_FULL_FLOW_ON_EXPIRATION_AND_NO_REFRESH_TOKEN);

    try (MockedStatic<CredentialManager> credentialManagerMockedStatic =
        mockStatic(CredentialManager.class)) {
      mockLoadingTokensFromCache(credentialManagerMockedStatic, "expired-access-token-123", null);
      SFLoginInput loginInput = createLoginInputStub();
      SFLoginOutput loginOutput = SessionUtil.openSession(loginInput, new HashMap<>(), "INFO");

      credentialManagerMockedStatic.verify(
          () ->
              CredentialManager.deleteOAuthAccessTokenCache(
                  loginInput.getHostFromServerUrl(), loginInput.getUserName()),
          times(1));
      credentialManagerMockedStatic.verify(
          () ->
              CredentialManager.deleteOAuthRefreshTokenCache(
                  loginInput.getHostFromServerUrl(), loginInput.getUserName()),
          never());
      assertEquals("newly-obtained-access-token-123", loginOutput.getOauthAccessToken());
      captureAndAssertSavedTokenValues(
          credentialManagerMockedStatic, "newly-obtained-access-token-123", null);
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
      MockedStatic<CredentialManager> credentialManagerMock,
      String oauthAccessToken,
      String oauthRefreshToken) {
    Answer<Object> fillCachedOAuthAccessTokenInvocation =
        invocation -> {
          ((SFLoginInput) invocation.getArguments()[0]).setOauthAccessToken(oauthAccessToken);
          return null;
        };
    Answer<Object> fillCachedOAuthRefreshTokenInvocation =
        invocation -> {
          ((SFLoginInput) invocation.getArguments()[0]).setOauthRefreshToken(oauthRefreshToken);
          return null;
        };
    credentialManagerMock
        .when(() -> CredentialManager.fillCachedOAuthAccessToken(any(SFLoginInput.class)))
        .then(fillCachedOAuthAccessTokenInvocation);
    credentialManagerMock
        .when(() -> CredentialManager.fillCachedOAuthRefreshToken(any(SFLoginInput.class)))
        .then(fillCachedOAuthRefreshTokenInvocation);
  }

  private static void captureAndAssertSavedTokenValues(
      MockedStatic<CredentialManager> credentialManagerMock,
      String expectedAccessToken,
      String expectedRefreshToken) {
    ArgumentCaptor<SFLoginInput> accessTokenInputCaptor =
        ArgumentCaptor.forClass(SFLoginInput.class);
    credentialManagerMock.verify(
        () -> CredentialManager.writeOAuthAccessToken(accessTokenInputCaptor.capture()));
    assertEquals(expectedAccessToken, accessTokenInputCaptor.getValue().getOauthAccessToken());

    if (expectedRefreshToken != null) {
      ArgumentCaptor<SFLoginInput> refreshTokenInputCaptor =
          ArgumentCaptor.forClass(SFLoginInput.class);
      credentialManagerMock.verify(
          () -> CredentialManager.writeOAuthRefreshToken(refreshTokenInputCaptor.capture()));
      assertEquals(expectedRefreshToken, refreshTokenInputCaptor.getValue().getOauthRefreshToken());
    } else {
      credentialManagerMock.verify(
          () -> CredentialManager.writeOAuthRefreshToken(any(SFLoginInput.class)), never());
    }
  }
}
