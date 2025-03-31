package net.snowflake.client.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
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
  public static final String SOME_DPOP_PUBLIC_KEY =
      "{\"kty\":\"EC\",\"d\":\"j5-J-nLE4J1I8ZWtArP8eQbxUbYMPmRvaEjEkHFlHds\",\"crv\":\"P-256\",\"x\":\"RL5cE-TC4Jr6CxtT4lEI2Yu6wT6LbwojPQsgHUg01F0\",\"y\":\"UAdLUSWTJ6czXaS3SfEFUZzKPcVVq4OZAD8e7Rp75y4\"}";
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
    Base64.Encoder encoder = Base64.getEncoder();
    SFLoginInput loginInputSnowflakeOAuth = createLoginInputWithSnowflakeServer();
    CredentialManager.writeIdToken(loginInputSnowflakeOAuth, SOME_ID_TOKEN_FROM_CACHE);
    verify(mockSecureStorageManager, times(1))
        .setCredential(
            SNOWFLAKE_HOST,
            SOME_USER,
            CachedCredentialType.ID_TOKEN.getValue(),
            encoder.encodeToString(SOME_ID_TOKEN_FROM_CACHE.getBytes(StandardCharsets.UTF_8)));
    CredentialManager.writeMfaToken(loginInputSnowflakeOAuth, SOME_MFA_TOKEN_FROM_CACHE);
    verify(mockSecureStorageManager, times(1))
        .setCredential(
            SNOWFLAKE_HOST,
            SOME_USER,
            CachedCredentialType.MFA_TOKEN.getValue(),
            encoder.encodeToString(SOME_MFA_TOKEN_FROM_CACHE.getBytes(StandardCharsets.UTF_8)));

    CredentialManager.writeOAuthAccessToken(loginInputSnowflakeOAuth);
    verify(mockSecureStorageManager, times(1))
        .setCredential(
            SNOWFLAKE_HOST,
            SOME_USER,
            CachedCredentialType.OAUTH_ACCESS_TOKEN.getValue(),
            encoder.encodeToString(SOME_ACCESS_TOKEN.getBytes(StandardCharsets.UTF_8)));
    CredentialManager.writeOAuthRefreshToken(loginInputSnowflakeOAuth);
    verify(mockSecureStorageManager, times(1))
        .setCredential(
            SNOWFLAKE_HOST,
            SOME_USER,
            CachedCredentialType.OAUTH_REFRESH_TOKEN.getValue(),
            encoder.encodeToString(SOME_REFRESH_TOKEN.getBytes(StandardCharsets.UTF_8)));

    SFLoginInput loginInputExternalOAuth = createLoginInputWithExternalOAuth();
    CredentialManager.writeOAuthAccessToken(loginInputExternalOAuth);
    verify(mockSecureStorageManager, times(1))
        .setCredential(
            EXTERNAL_OAUTH_HOST,
            SOME_USER,
            CachedCredentialType.OAUTH_ACCESS_TOKEN.getValue(),
            encoder.encodeToString(SOME_ACCESS_TOKEN.getBytes(StandardCharsets.UTF_8)));
    CredentialManager.writeOAuthRefreshToken(loginInputExternalOAuth);
    verify(mockSecureStorageManager, times(1))
        .setCredential(
            EXTERNAL_OAUTH_HOST,
            SOME_USER,
            CachedCredentialType.OAUTH_REFRESH_TOKEN.getValue(),
            encoder.encodeToString(SOME_REFRESH_TOKEN.getBytes(StandardCharsets.UTF_8)));

    SFLoginInput loginInputDPoP = createLoginInputWithDPoPPublicKey();
    String dpopBundledToken =
        encoder.encodeToString(SOME_ACCESS_TOKEN.getBytes(StandardCharsets.UTF_8))
            + "."
            + encoder.encodeToString(SOME_DPOP_PUBLIC_KEY.getBytes(StandardCharsets.UTF_8));
    CredentialManager.writeDPoPBundledAccessToken(loginInputDPoP);
    verify(mockSecureStorageManager, times(1))
        .setCredential(
            SNOWFLAKE_HOST,
            SOME_USER,
            CachedCredentialType.DPOP_BUNDLED_ACCESS_TOKEN.getValue(),
            dpopBundledToken);
  }

  @Test
  public void shouldProperlyDeleteTokensFromCache() throws SFException {
    SFLoginInput loginInputSnowflakeOAuth = createLoginInputWithSnowflakeServer();
    CredentialManager.deleteIdTokenCacheEntry(
        loginInputSnowflakeOAuth.getHostFromServerUrl(), loginInputSnowflakeOAuth.getUserName());
    verify(mockSecureStorageManager, times(1))
        .deleteCredential(SNOWFLAKE_HOST, SOME_USER, CachedCredentialType.ID_TOKEN.getValue());
    CredentialManager.deleteMfaTokenCacheEntry(
        loginInputSnowflakeOAuth.getHostFromServerUrl(), loginInputSnowflakeOAuth.getUserName());
    verify(mockSecureStorageManager, times(1))
        .deleteCredential(SNOWFLAKE_HOST, SOME_USER, CachedCredentialType.MFA_TOKEN.getValue());

    CredentialManager.deleteOAuthAccessTokenCacheEntry(loginInputSnowflakeOAuth);
    verify(mockSecureStorageManager, times(1))
        .deleteCredential(
            SNOWFLAKE_HOST, SOME_USER, CachedCredentialType.OAUTH_ACCESS_TOKEN.getValue());
    CredentialManager.deleteOAuthRefreshTokenCacheEntry(loginInputSnowflakeOAuth);
    verify(mockSecureStorageManager, times(1))
        .deleteCredential(
            SNOWFLAKE_HOST, SOME_USER, CachedCredentialType.OAUTH_REFRESH_TOKEN.getValue());

    SFLoginInput loginInputExternalOAuth = createLoginInputWithExternalOAuth();
    CredentialManager.deleteOAuthAccessTokenCacheEntry(loginInputExternalOAuth);
    verify(mockSecureStorageManager, times(1))
        .deleteCredential(
            EXTERNAL_OAUTH_HOST, SOME_USER, CachedCredentialType.OAUTH_ACCESS_TOKEN.getValue());
    CredentialManager.deleteOAuthRefreshTokenCacheEntry(loginInputExternalOAuth);
    verify(mockSecureStorageManager, times(1))
        .deleteCredential(
            EXTERNAL_OAUTH_HOST, SOME_USER, CachedCredentialType.OAUTH_REFRESH_TOKEN.getValue());

    SFLoginInput loginInputDPoP = createLoginInputWithDPoPPublicKey();
    CredentialManager.deleteDPoPBundledAccessTokenCacheEntry(loginInputDPoP);
    verify(mockSecureStorageManager, times(1))
        .deleteCredential(
            SNOWFLAKE_HOST, SOME_USER, CachedCredentialType.DPOP_BUNDLED_ACCESS_TOKEN.getValue());
  }

  @Test
  public void shouldProperlyUpdateInputWithTokensFromCache() throws SFException {
    Base64.Encoder encoder = Base64.getEncoder();
    SFLoginInput loginInputSnowflakeOAuth = createLoginInputWithSnowflakeServer();
    when(mockSecureStorageManager.getCredential(
            SNOWFLAKE_HOST, SOME_USER, CachedCredentialType.ID_TOKEN.getValue()))
        .thenReturn(
            encoder.encodeToString(SOME_ID_TOKEN_FROM_CACHE.getBytes(StandardCharsets.UTF_8)));
    CredentialManager.fillCachedIdToken(loginInputSnowflakeOAuth);
    when(mockSecureStorageManager.getCredential(
            SNOWFLAKE_HOST, SOME_USER, CachedCredentialType.MFA_TOKEN.getValue()))
        .thenReturn(
            encoder.encodeToString(SOME_MFA_TOKEN_FROM_CACHE.getBytes(StandardCharsets.UTF_8)));
    CredentialManager.fillCachedMfaToken(loginInputSnowflakeOAuth);
    assertEquals(SOME_ID_TOKEN_FROM_CACHE, loginInputSnowflakeOAuth.getIdToken());
    assertEquals(SOME_MFA_TOKEN_FROM_CACHE, loginInputSnowflakeOAuth.getMfaToken());

    when(mockSecureStorageManager.getCredential(
            SNOWFLAKE_HOST, SOME_USER, CachedCredentialType.OAUTH_ACCESS_TOKEN.getValue()))
        .thenReturn(
            encoder.encodeToString(ACCESS_TOKEN_FROM_CACHE.getBytes(StandardCharsets.UTF_8)));
    CredentialManager.fillCachedOAuthAccessToken(loginInputSnowflakeOAuth);
    when(mockSecureStorageManager.getCredential(
            SNOWFLAKE_HOST, SOME_USER, CachedCredentialType.OAUTH_REFRESH_TOKEN.getValue()))
        .thenReturn(
            encoder.encodeToString(REFRESH_TOKEN_FROM_CACHE.getBytes(StandardCharsets.UTF_8)));
    CredentialManager.fillCachedOAuthRefreshToken(loginInputSnowflakeOAuth);
    assertEquals(ACCESS_TOKEN_FROM_CACHE, loginInputSnowflakeOAuth.getOauthAccessToken());
    assertEquals(REFRESH_TOKEN_FROM_CACHE, loginInputSnowflakeOAuth.getOauthRefreshToken());

    SFLoginInput loginInputExternalOAuth = createLoginInputWithExternalOAuth();
    when(mockSecureStorageManager.getCredential(
            EXTERNAL_OAUTH_HOST, SOME_USER, CachedCredentialType.OAUTH_ACCESS_TOKEN.getValue()))
        .thenReturn(
            encoder.encodeToString(
                EXTERNAL_ACCESS_TOKEN_FROM_CACHE.getBytes(StandardCharsets.UTF_8)));
    CredentialManager.fillCachedOAuthAccessToken(loginInputExternalOAuth);
    when(mockSecureStorageManager.getCredential(
            EXTERNAL_OAUTH_HOST, SOME_USER, CachedCredentialType.OAUTH_REFRESH_TOKEN.getValue()))
        .thenReturn(
            encoder.encodeToString(
                EXTERNAL_REFRESH_TOKEN_FROM_CACHE.getBytes(StandardCharsets.UTF_8)));
    CredentialManager.fillCachedOAuthRefreshToken(loginInputExternalOAuth);
    assertEquals(EXTERNAL_ACCESS_TOKEN_FROM_CACHE, loginInputExternalOAuth.getOauthAccessToken());
    assertEquals(EXTERNAL_REFRESH_TOKEN_FROM_CACHE, loginInputExternalOAuth.getOauthRefreshToken());

    SFLoginInput loginInputDPoP = createLoginInputWithDPoPPublicKey();
    String dpopBundledToken =
        encoder.encodeToString(SOME_ACCESS_TOKEN.getBytes(StandardCharsets.UTF_8))
            + "."
            + encoder.encodeToString(SOME_DPOP_PUBLIC_KEY.getBytes(StandardCharsets.UTF_8));
    when(mockSecureStorageManager.getCredential(
            SNOWFLAKE_HOST, SOME_USER, CachedCredentialType.DPOP_BUNDLED_ACCESS_TOKEN.getValue()))
        .thenReturn(dpopBundledToken);
    CredentialManager.fillCachedDPoPBundledAccessToken(loginInputDPoP);
    assertEquals(SOME_ACCESS_TOKEN, loginInputDPoP.getOauthAccessToken());
    assertEquals(SOME_DPOP_PUBLIC_KEY, loginInputDPoP.getDPoPPublicKey());
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

  private SFLoginInput createLoginInputWithDPoPPublicKey() {
    SFLoginInput loginInput = createLoginInputWithSnowflakeServer();
    loginInput.setDPoPPublicKey(SOME_DPOP_PUBLIC_KEY);
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
