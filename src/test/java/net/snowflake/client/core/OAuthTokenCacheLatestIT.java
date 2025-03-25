package net.snowflake.client.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.core.auth.AuthenticatorType;
import net.snowflake.client.jdbc.BaseWiremockTest;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.stubbing.Answer;

@Tag(TestTags.CORE)
public class OAuthTokenCacheLatestIT extends BaseWiremockTest {

  public static final String MOCK_DPOP_PUBLIC_KEY =
      "{\"kty\":\"EC\",\"d\":\"j5-J-nLE4J1I8ZWtArP8eQbxUbYMPmRvaEjEkHFlHds\",\"crv\":\"P-256\",\"x\":\"RL5cE-TC4Jr6CxtT4lEI2Yu6wT6LbwojPQsgHUg01F0\",\"y\":\"UAdLUSWTJ6czXaS3SfEFUZzKPcVVq4OZAD8e7Rp75y4\"}";

  private static final String SCENARIOS_BASE_DIR = MAPPINGS_BASE_DIR + "/oauth/token_caching";
  private static final String CACHING_TOKENS_AFTER_CONNECTING_SCENARIO_MAPPINGS =
      SCENARIOS_BASE_DIR + "/caching_tokens_after_connecting.json";
  private static final String CACHING_TOKENS_AND_DPOP_KEY_AFTER_CONNECTING_SCENARIO_MAPPINGS =
      SCENARIOS_BASE_DIR + "/caching_tokens_and_dpop_key_after_connecting.json";
  private static final String REUSING_CACHED_ACCESS_TOKEN_SCENARIO_MAPPINGS =
      SCENARIOS_BASE_DIR + "/reusing_cached_access_token_to_authenticate.json";
  private static final String REFRESHING_EXPIRED_ACCESS_TOKEN_SCENARIO_MAPPINGS =
      SCENARIOS_BASE_DIR + "/refreshing_expired_access_token.json";
  private static final String REFRESHING_EXPIRED_ACCESS_TOKEN_SCENARIO_MAPPINGS_DPOP =
      SCENARIOS_BASE_DIR + "/refreshing_expired_access_token_dpop.json";
  private static final String REFRESHING_EXPIRED_ACCESS_TOKEN_SCENARIO_MAPPINGS_DPOP_NONCE_ERROR =
      SCENARIOS_BASE_DIR + "/refreshing_expired_access_token_dpop_nonce_error.json";
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
  public void shouldCacheAccessTokenAndDPoPKeyAfterConnecting()
      throws SFException, SnowflakeSQLException {
    importMappingFromResources(CACHING_TOKENS_AND_DPOP_KEY_AFTER_CONNECTING_SCENARIO_MAPPINGS);
    try (MockedStatic<CredentialManager> credentialManagerMockedStatic =
        mockStatic(CredentialManager.class)) {
      SFLoginInput loginInput = createLoginInputStubWithDPoPEnabled();
      SessionUtil.openSession(loginInput, new HashMap<>(), "INFO");
      captureAndAssertSavedTokenValuesAndDPoPKey(
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
          () -> CredentialManager.deleteOAuthAccessTokenCacheEntry(loginInput), times(1));
      credentialManagerMockedStatic.verify(
          () -> CredentialManager.deleteOAuthRefreshTokenCacheEntry(loginInput), never());

      assertEquals("new-refreshed-access-token-123", loginOutput.getOauthAccessToken());
      captureAndAssertSavedTokenValues(
          credentialManagerMockedStatic,
          "new-refreshed-access-token-123",
          "some-refresh-token-123");
    }
  }

  @Test
  public void shouldRefreshExpiredAccessTokenWithDPoPAndConnectSuccessfully()
      throws SFException, SnowflakeSQLException {
    importMappingFromResources(REFRESHING_EXPIRED_ACCESS_TOKEN_SCENARIO_MAPPINGS_DPOP);
    try (MockedStatic<CredentialManager> credentialManagerMockedStatic =
        mockStatic(CredentialManager.class)) {
      mockLoadingTokensFromCache(credentialManagerMockedStatic, null, "some-refresh-token-123");
      mockLoadingDPoPPublicKeyFromCache(credentialManagerMockedStatic, "expired-access-token-123");
      SFLoginInput loginInput = createLoginInputStubWithDPoPEnabled();
      SFLoginOutput loginOutput = SessionUtil.openSession(loginInput, new HashMap<>(), "INFO");

      credentialManagerMockedStatic.verify(
          () -> CredentialManager.deleteOAuthAccessTokenCacheEntry(loginInput), times(1));
      credentialManagerMockedStatic.verify(
          () -> CredentialManager.deleteDPoPBundledAccessTokenCacheEntry(loginInput), times(1));
      credentialManagerMockedStatic.verify(
          () -> CredentialManager.deleteOAuthRefreshTokenCacheEntry(loginInput), never());

      assertEquals("new-refreshed-access-token-123", loginOutput.getOauthAccessToken());
      captureAndAssertSavedTokenValuesAndDPoPKey(
          credentialManagerMockedStatic,
          "new-refreshed-access-token-123",
          "some-refresh-token-123");
    }
  }

  @Test
  public void shouldRefreshExpiredAccessTokenWithDPoPNonceErrorAndConnectSuccessfully()
      throws SFException, SnowflakeSQLException {
    importMappingFromResources(REFRESHING_EXPIRED_ACCESS_TOKEN_SCENARIO_MAPPINGS_DPOP_NONCE_ERROR);
    try (MockedStatic<CredentialManager> credentialManagerMockedStatic =
        mockStatic(CredentialManager.class)) {
      mockLoadingTokensFromCache(credentialManagerMockedStatic, null, "some-refresh-token-123");
      mockLoadingDPoPPublicKeyFromCache(credentialManagerMockedStatic, "expired-access-token-123");
      SFLoginInput loginInput = createLoginInputStubWithDPoPEnabled();
      SFLoginOutput loginOutput = SessionUtil.openSession(loginInput, new HashMap<>(), "INFO");

      credentialManagerMockedStatic.verify(
          () -> CredentialManager.deleteOAuthAccessTokenCacheEntry(loginInput), times(1));
      credentialManagerMockedStatic.verify(
          () -> CredentialManager.deleteDPoPBundledAccessTokenCacheEntry(loginInput), times(1));
      credentialManagerMockedStatic.verify(
          () -> CredentialManager.deleteOAuthRefreshTokenCacheEntry(loginInput), never());

      assertEquals("new-refreshed-access-token-123", loginOutput.getOauthAccessToken());
      captureAndAssertSavedTokenValuesAndDPoPKey(
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
          () -> CredentialManager.deleteOAuthAccessTokenCacheEntry(loginInput), times(1));
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
          () -> CredentialManager.deleteOAuthAccessTokenCacheEntry(loginInput), times(1));
      credentialManagerMockedStatic.verify(
          () -> CredentialManager.deleteOAuthRefreshTokenCacheEntry(loginInput), times(1));
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
          () -> CredentialManager.deleteOAuthAccessTokenCacheEntry(loginInput), times(1));
      credentialManagerMockedStatic.verify(
          () -> CredentialManager.deleteOAuthRefreshTokenCacheEntry(loginInput), never());
      assertEquals("newly-obtained-access-token-123", loginOutput.getOauthAccessToken());
      captureAndAssertSavedTokenValues(
          credentialManagerMockedStatic, "newly-obtained-access-token-123", null);
    }
  }

  private SFLoginInput createLoginInputStub() {
    SFLoginInput input = new SFLoginInput();
    input.setAuthenticator(AuthenticatorType.OAUTH_CLIENT_CREDENTIALS.name());
    input.setOriginalAuthenticator(AuthenticatorType.OAUTH_CLIENT_CREDENTIALS.name());
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

  private SFLoginInput createLoginInputStubWithDPoPEnabled() {
    SFLoginInput input = createLoginInputStub();
    input.setDPoPEnabled(true);
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

  private static void mockLoadingDPoPPublicKeyFromCache(
      MockedStatic<CredentialManager> credentialManagerMock, String oauthAccessToken) {
    Answer<Object> fillCachedDPoPPublicKeyInvocation =
        invocation -> {
          ((SFLoginInput) invocation.getArguments()[0]).setOauthAccessToken(oauthAccessToken);
          ((SFLoginInput) invocation.getArguments()[0]).setDPoPPublicKey(MOCK_DPOP_PUBLIC_KEY);
          return null;
        };
    credentialManagerMock
        .when(() -> CredentialManager.fillCachedDPoPBundledAccessToken(any(SFLoginInput.class)))
        .then(fillCachedDPoPPublicKeyInvocation);
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

  private static void captureAndAssertSavedTokenValuesAndDPoPKey(
      MockedStatic<CredentialManager> credentialManagerMock,
      String expectedAccessToken,
      String expectedRefreshToken) {
    if (expectedRefreshToken != null) {
      ArgumentCaptor<SFLoginInput> refreshTokenInputCaptor =
          ArgumentCaptor.forClass(SFLoginInput.class);
      credentialManagerMock.verify(
          () -> CredentialManager.writeOAuthRefreshToken(refreshTokenInputCaptor.capture()));
      assertEquals(expectedRefreshToken, refreshTokenInputCaptor.getValue().getOauthRefreshToken());
    }
    ArgumentCaptor<SFLoginInput> dpopBundledAccessTokenInputCaptor =
        ArgumentCaptor.forClass(SFLoginInput.class);
    credentialManagerMock.verify(
        () ->
            CredentialManager.writeDPoPBundledAccessToken(
                dpopBundledAccessTokenInputCaptor.capture()));
    assertEquals(
        expectedAccessToken, dpopBundledAccessTokenInputCaptor.getValue().getOauthAccessToken());
    assertNotNull(dpopBundledAccessTokenInputCaptor.getValue().getDPoPPublicKey());
  }
}
