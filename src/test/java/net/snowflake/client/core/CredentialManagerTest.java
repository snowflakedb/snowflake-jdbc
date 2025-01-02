/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class CredentialManagerTest {

  public static final String SNOWFLAKE_HOST = "some-account.us-west-2.aws.snowflakecomputing.com";
  public static final String EXTERNAL_OAUTH_HOST = "some-external-oauth-host.com";
  public static final String SOME_ACCESS_TOKEN = "some-oauth-access-token";
  public static final String SOME_REFRESH_TOKEN = "some-refresh-token";
  public static final String SOME_ID_TOKEN_FROM_CACHE = "some-id-token";
  public static final String SOME_MFA_TOKEN_FROM_CACHE = "some-mfa-token";
  public static final String SOME_USER = "some-user";

  public static final String ACCESS_TOKEN_FROM_CACHE = "access-token-from-cache";
  public static final String REFRESH_TOKEN_FROM_CACHE = "refresh-token-from-cache";
  public static final String EXTERNAL_ACCESS_TOKEN_FROM_CACHE = "external-access-token-from-cache";
  public static final String EXTERNAL_REFRESH_TOKEN_FROM_CACHE =
      "external-refresh-token-from-cache";

  private static final SecureStorageManager mockSecureStorageManager =
      mock(SecureStorageManager.class);

  @BeforeAll
  public static void setUp() {
    CredentialManager.injectSecureStorageManager(mockSecureStorageManager);
  }

  @AfterAll
  public static void tearDown() {
    CredentialManager.resetSecureStorageManager();
  }

  @Test
  public void shouldCreateHostBasedOnExternalIdpUrl() throws SFException {
    SFLoginInput loginInput = createLoginInputWithExternalOAuth();
    String host = CredentialManager.getHostForOAuthCacheKey(loginInput);
    assertEquals(EXTERNAL_OAUTH_HOST, host);
  }

  @Test
  public void shouldCreateHostBasedOnSnowflakeServerUrl() throws SFException {
    SFLoginInput loginInput = createLoginInputWithSnowflakeServer();
    String host = CredentialManager.getHostForOAuthCacheKey(loginInput);
    assertEquals(SNOWFLAKE_HOST, host);
  }

  @Test
  public void shouldProperlyWriteTokensToCache() throws SFException {
    SFLoginInput loginInputSnowflakeOAuth = createLoginInputWithSnowflakeServer();
    CredentialManager.writeIdToken(loginInputSnowflakeOAuth, SOME_ID_TOKEN_FROM_CACHE);
    verify(mockSecureStorageManager, times(1))
        .setCredential(
            SNOWFLAKE_HOST,
            SOME_USER,
            CachedCredentialType.ID_TOKEN.getValue(),
            SOME_ID_TOKEN_FROM_CACHE);
    CredentialManager.writeMfaToken(loginInputSnowflakeOAuth, SOME_MFA_TOKEN_FROM_CACHE);
    verify(mockSecureStorageManager, times(1))
        .setCredential(
            SNOWFLAKE_HOST,
            SOME_USER,
            CachedCredentialType.MFA_TOKEN.getValue(),
            SOME_MFA_TOKEN_FROM_CACHE);

    CredentialManager.writeOAuthAccessToken(loginInputSnowflakeOAuth);
    verify(mockSecureStorageManager, times(1))
        .setCredential(
            SNOWFLAKE_HOST,
            SOME_USER,
            CachedCredentialType.OAUTH_ACCESS_TOKEN.getValue(),
            SOME_ACCESS_TOKEN);
    CredentialManager.writeOAuthRefreshToken(loginInputSnowflakeOAuth);
    verify(mockSecureStorageManager, times(1))
        .setCredential(
            SNOWFLAKE_HOST,
            SOME_USER,
            CachedCredentialType.OAUTH_REFRESH_TOKEN.getValue(),
            SOME_REFRESH_TOKEN);

    SFLoginInput loginInputExternalOAuth = createLoginInputWithExternalOAuth();
    CredentialManager.writeOAuthAccessToken(loginInputExternalOAuth);
    verify(mockSecureStorageManager, times(1))
        .setCredential(
            EXTERNAL_OAUTH_HOST,
            SOME_USER,
            CachedCredentialType.OAUTH_ACCESS_TOKEN.getValue(),
            SOME_ACCESS_TOKEN);
    CredentialManager.writeOAuthRefreshToken(loginInputExternalOAuth);
    verify(mockSecureStorageManager, times(1))
        .setCredential(
            EXTERNAL_OAUTH_HOST,
            SOME_USER,
            CachedCredentialType.OAUTH_REFRESH_TOKEN.getValue(),
            SOME_REFRESH_TOKEN);
  }

  @Test
  public void shouldProperlyDeleteTokensFromCache() throws SFException {
    SFLoginInput loginInputSnowflakeOAuth = createLoginInputWithSnowflakeServer();
    CredentialManager.deleteIdTokenCache(
        loginInputSnowflakeOAuth.getHostFromServerUrl(), loginInputSnowflakeOAuth.getUserName());
    verify(mockSecureStorageManager, times(1))
        .deleteCredential(SNOWFLAKE_HOST, SOME_USER, CachedCredentialType.ID_TOKEN.getValue());
    CredentialManager.deleteMfaTokenCache(
        loginInputSnowflakeOAuth.getHostFromServerUrl(), loginInputSnowflakeOAuth.getUserName());
    verify(mockSecureStorageManager, times(1))
        .deleteCredential(SNOWFLAKE_HOST, SOME_USER, CachedCredentialType.MFA_TOKEN.getValue());

    CredentialManager.deleteOAuthAccessTokenCache(loginInputSnowflakeOAuth);
    verify(mockSecureStorageManager, times(1))
        .deleteCredential(
            SNOWFLAKE_HOST, SOME_USER, CachedCredentialType.OAUTH_ACCESS_TOKEN.getValue());
    CredentialManager.deleteOAuthRefreshTokenCache(loginInputSnowflakeOAuth);
    verify(mockSecureStorageManager, times(1))
        .deleteCredential(
            SNOWFLAKE_HOST, SOME_USER, CachedCredentialType.OAUTH_REFRESH_TOKEN.getValue());

    SFLoginInput loginInputExternalOAuth = createLoginInputWithExternalOAuth();
    CredentialManager.deleteOAuthAccessTokenCache(loginInputExternalOAuth);
    verify(mockSecureStorageManager, times(1))
        .deleteCredential(
            EXTERNAL_OAUTH_HOST, SOME_USER, CachedCredentialType.OAUTH_ACCESS_TOKEN.getValue());
    CredentialManager.deleteOAuthRefreshTokenCache(loginInputExternalOAuth);
    verify(mockSecureStorageManager, times(1))
        .deleteCredential(
            EXTERNAL_OAUTH_HOST, SOME_USER, CachedCredentialType.OAUTH_REFRESH_TOKEN.getValue());
  }

  @Test
  public void shouldProperlyUpdateInputWithTokensFromCache() throws SFException {
    SFLoginInput loginInputSnowflakeOAuth = createLoginInputWithSnowflakeServer();
    when(mockSecureStorageManager.getCredential(
            SNOWFLAKE_HOST, SOME_USER, CachedCredentialType.ID_TOKEN.getValue()))
        .thenReturn(SOME_ID_TOKEN_FROM_CACHE);
    CredentialManager.fillCachedIdToken(loginInputSnowflakeOAuth);
    when(mockSecureStorageManager.getCredential(
            SNOWFLAKE_HOST, SOME_USER, CachedCredentialType.MFA_TOKEN.getValue()))
        .thenReturn(SOME_MFA_TOKEN_FROM_CACHE);
    CredentialManager.fillCachedMfaToken(loginInputSnowflakeOAuth);
    assertEquals(SOME_ID_TOKEN_FROM_CACHE, loginInputSnowflakeOAuth.getIdToken());
    assertEquals(SOME_MFA_TOKEN_FROM_CACHE, loginInputSnowflakeOAuth.getMfaToken());

    when(mockSecureStorageManager.getCredential(
            SNOWFLAKE_HOST, SOME_USER, CachedCredentialType.OAUTH_ACCESS_TOKEN.getValue()))
        .thenReturn(ACCESS_TOKEN_FROM_CACHE);
    CredentialManager.fillCachedOAuthAccessToken(loginInputSnowflakeOAuth);
    when(mockSecureStorageManager.getCredential(
            SNOWFLAKE_HOST, SOME_USER, CachedCredentialType.OAUTH_REFRESH_TOKEN.getValue()))
        .thenReturn(REFRESH_TOKEN_FROM_CACHE);
    CredentialManager.fillCachedOAuthRefreshToken(loginInputSnowflakeOAuth);
    assertEquals(ACCESS_TOKEN_FROM_CACHE, loginInputSnowflakeOAuth.getOauthAccessToken());
    assertEquals(REFRESH_TOKEN_FROM_CACHE, loginInputSnowflakeOAuth.getOauthRefreshToken());

    SFLoginInput loginInputExternalOAuth = createLoginInputWithExternalOAuth();
    when(mockSecureStorageManager.getCredential(
            EXTERNAL_OAUTH_HOST, SOME_USER, CachedCredentialType.OAUTH_ACCESS_TOKEN.getValue()))
        .thenReturn(EXTERNAL_ACCESS_TOKEN_FROM_CACHE);
    CredentialManager.fillCachedOAuthAccessToken(loginInputExternalOAuth);
    when(mockSecureStorageManager.getCredential(
            EXTERNAL_OAUTH_HOST, SOME_USER, CachedCredentialType.OAUTH_REFRESH_TOKEN.getValue()))
        .thenReturn(EXTERNAL_REFRESH_TOKEN_FROM_CACHE);
    CredentialManager.fillCachedOAuthRefreshToken(loginInputExternalOAuth);
    assertEquals(EXTERNAL_ACCESS_TOKEN_FROM_CACHE, loginInputExternalOAuth.getOauthAccessToken());
    assertEquals(EXTERNAL_REFRESH_TOKEN_FROM_CACHE, loginInputExternalOAuth.getOauthRefreshToken());
  }

  private SFLoginInput createLoginInputWithExternalOAuth() {
    SFLoginInput loginInput = createGenericLoginInput();
    loginInput.setOauthLoginInput(
        new SFOauthLoginInput(
            null, null, null, null, "https://some-external-oauth-host.com/oauth/token", null));
    return loginInput;
  }

  private SFLoginInput createLoginInputWithSnowflakeServer() {
    SFLoginInput loginInput = createGenericLoginInput();
    loginInput.setOauthLoginInput(new SFOauthLoginInput(null, null, null, null, null, null));
    loginInput.setServerUrl("https://some-account.us-west-2.aws.snowflakecomputing.com:443/");

    return loginInput;
  }

  private SFLoginInput createGenericLoginInput() {
    SFLoginInput loginInput = new SFLoginInput();
    loginInput.setOauthAccessToken(SOME_ACCESS_TOKEN);
    loginInput.setOauthRefreshToken(SOME_REFRESH_TOKEN);
    loginInput.setUserName(SOME_USER);
    return loginInput;
  }
}
