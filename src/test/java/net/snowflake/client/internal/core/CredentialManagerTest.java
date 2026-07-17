package net.snowflake.client.internal.core;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
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
  public static final String SNOWFLAKE_SERVER_URL =
      "https://some-account.us-west-2.aws.snowflakecomputing.com:443/";
  public static final String EXTERNAL_OAUTH_TOKEN_URL =
      "https://some-external-oauth-host.com/oauth/token";
  public static final String SOME_ACCESS_TOKEN = "some-oauth-access-token";
  public static final String SOME_REFRESH_TOKEN = "some-refresh-token";
  public static final String SOME_ID_TOKEN_FROM_CACHE = "some-id-token";
  public static final String SOME_MFA_TOKEN_FROM_CACHE = "some-mfa-token";
  public static final String SOME_DPOP_PUBLIC_KEY =
      // pragma: allowlist nextline secret
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

  // -------------------------------------------------------------------------
  // IdP URL extraction
  // -------------------------------------------------------------------------

  @Test
  void shouldReturnFullExternalIdpUrlForOAuthCacheKey() throws SFException {
    SFLoginInput loginInput = createLoginInputWithExternalOAuth();
    String idpUrl = CredentialManager.getIdpUrlForOAuthCacheKey(loginInput);
    assertEquals(EXTERNAL_OAUTH_TOKEN_URL, idpUrl);
  }

  @Test
  void shouldReturnSnowflakeServerUrlWhenNoExternalIdp() throws SFException {
    SFLoginInput loginInput = createLoginInputWithSnowflakeServer();
    String idpUrl = CredentialManager.getIdpUrlForOAuthCacheKey(loginInput);
    assertEquals(SNOWFLAKE_SERVER_URL, idpUrl);
  }

  // -------------------------------------------------------------------------
  // Write tokens to cache
  // -------------------------------------------------------------------------

  @Test
  void shouldProperlyWriteTokensToCache() throws SFException {
    Base64.Encoder encoder = Base64.getEncoder();
    SFLoginInput loginInputSnowflakeOAuth = createLoginInputWithSnowflakeServer();

    // ID token — idp == snowflake (Snowflake is the IdP for browser flow)
    String idTokenKey = cacheKey(CachedCredentialType.ID_TOKEN, SNOWFLAKE_HOST, SNOWFLAKE_HOST, "");
    CredentialManager.writeIdToken(loginInputSnowflakeOAuth, SOME_ID_TOKEN_FROM_CACHE);
    verify(mockSecureStorageManager, times(1))
        .setCredential(
            idTokenKey,
            encoder.encodeToString(SOME_ID_TOKEN_FROM_CACHE.getBytes(StandardCharsets.UTF_8)));

    // MFA token — role always empty
    String mfaTokenKey = cacheKey(CachedCredentialType.MFA_TOKEN, SNOWFLAKE_HOST, SNOWFLAKE_HOST, "");
    CredentialManager.writeMfaToken(loginInputSnowflakeOAuth, SOME_MFA_TOKEN_FROM_CACHE);
    verify(mockSecureStorageManager, times(1))
        .setCredential(
            mfaTokenKey,
            encoder.encodeToString(SOME_MFA_TOKEN_FROM_CACHE.getBytes(StandardCharsets.UTF_8)));

    // OAuth access/refresh — idp derived from Snowflake server URL (no external IdP)
    String snowflakeIdpNormalized = SecureStorageManager.normalizeUrl(SNOWFLAKE_SERVER_URL);
    String oauthAccessKey =
        SecureStorageManager.buildCacheKey(
            new CacheKeyInput(
                CachedCredentialType.OAUTH_ACCESS_TOKEN.getValue(),
                SNOWFLAKE_SERVER_URL,
                SNOWFLAKE_HOST,
                SOME_USER,
                ""));
    CredentialManager.writeOAuthAccessToken(loginInputSnowflakeOAuth);
    verify(mockSecureStorageManager, times(1))
        .setCredential(
            oauthAccessKey,
            encoder.encodeToString(SOME_ACCESS_TOKEN.getBytes(StandardCharsets.UTF_8)));

    String oauthRefreshKey =
        SecureStorageManager.buildCacheKey(
            new CacheKeyInput(
                CachedCredentialType.OAUTH_REFRESH_TOKEN.getValue(),
                SNOWFLAKE_SERVER_URL,
                SNOWFLAKE_HOST,
                SOME_USER,
                ""));
    CredentialManager.writeOAuthRefreshToken(loginInputSnowflakeOAuth);
    verify(mockSecureStorageManager, times(1))
        .setCredential(
            oauthRefreshKey,
            encoder.encodeToString(SOME_REFRESH_TOKEN.getBytes(StandardCharsets.UTF_8)));

    // OAuth with external IdP
    SFLoginInput loginInputExternalOAuth = createLoginInputWithExternalOAuth();

    String extOauthAccessKey =
        SecureStorageManager.buildCacheKey(
            new CacheKeyInput(
                CachedCredentialType.OAUTH_ACCESS_TOKEN.getValue(),
                EXTERNAL_OAUTH_TOKEN_URL,
                SNOWFLAKE_HOST,
                SOME_USER,
                ""));
    CredentialManager.writeOAuthAccessToken(loginInputExternalOAuth);
    verify(mockSecureStorageManager, times(1))
        .setCredential(
            extOauthAccessKey,
            encoder.encodeToString(SOME_ACCESS_TOKEN.getBytes(StandardCharsets.UTF_8)));

    String extOauthRefreshKey =
        SecureStorageManager.buildCacheKey(
            new CacheKeyInput(
                CachedCredentialType.OAUTH_REFRESH_TOKEN.getValue(),
                EXTERNAL_OAUTH_TOKEN_URL,
                SNOWFLAKE_HOST,
                SOME_USER,
                ""));
    CredentialManager.writeOAuthRefreshToken(loginInputExternalOAuth);
    verify(mockSecureStorageManager, times(1))
        .setCredential(
            extOauthRefreshKey,
            encoder.encodeToString(SOME_REFRESH_TOKEN.getBytes(StandardCharsets.UTF_8)));

    // DPoP bundled access token
    SFLoginInput loginInputDPoP = createLoginInputWithDPoPPublicKey();
    String dpopKey =
        SecureStorageManager.buildCacheKey(
            new CacheKeyInput(
                CachedCredentialType.DPOP_BUNDLED_ACCESS_TOKEN.getValue(),
                SNOWFLAKE_SERVER_URL,
                SNOWFLAKE_HOST,
                SOME_USER,
                ""));
    String dpopBundledToken =
        encoder.encodeToString(SOME_ACCESS_TOKEN.getBytes(StandardCharsets.UTF_8))
            + "."
            + encoder.encodeToString(SOME_DPOP_PUBLIC_KEY.getBytes(StandardCharsets.UTF_8));
    CredentialManager.writeDPoPBundledAccessToken(loginInputDPoP);
    verify(mockSecureStorageManager, times(1)).setCredential(dpopKey, dpopBundledToken);
  }

  // -------------------------------------------------------------------------
  // Delete tokens from cache
  // -------------------------------------------------------------------------

  @Test
  void shouldProperlyDeleteTokensFromCache() throws SFException {
    SFLoginInput loginInputSnowflakeOAuth = createLoginInputWithSnowflakeServer();

    String idTokenKey = cacheKey(CachedCredentialType.ID_TOKEN, SNOWFLAKE_HOST, SNOWFLAKE_HOST, "");
    CredentialManager.deleteIdTokenCacheEntry(
        loginInputSnowflakeOAuth.getHostFromServerUrl(), loginInputSnowflakeOAuth.getUserName());
    verify(mockSecureStorageManager, times(1)).deleteCredential(idTokenKey);

    String mfaTokenKey = cacheKey(CachedCredentialType.MFA_TOKEN, SNOWFLAKE_HOST, SNOWFLAKE_HOST, "");
    CredentialManager.deleteMfaTokenCacheEntry(
        loginInputSnowflakeOAuth.getHostFromServerUrl(), loginInputSnowflakeOAuth.getUserName());
    verify(mockSecureStorageManager, times(1)).deleteCredential(mfaTokenKey);

    String oauthAccessKey =
        SecureStorageManager.buildCacheKey(
            new CacheKeyInput(
                CachedCredentialType.OAUTH_ACCESS_TOKEN.getValue(),
                SNOWFLAKE_SERVER_URL,
                SNOWFLAKE_HOST,
                SOME_USER,
                ""));
    CredentialManager.deleteOAuthAccessTokenCacheEntry(loginInputSnowflakeOAuth);
    verify(mockSecureStorageManager, times(1)).deleteCredential(oauthAccessKey);

    String oauthRefreshKey =
        SecureStorageManager.buildCacheKey(
            new CacheKeyInput(
                CachedCredentialType.OAUTH_REFRESH_TOKEN.getValue(),
                SNOWFLAKE_SERVER_URL,
                SNOWFLAKE_HOST,
                SOME_USER,
                ""));
    CredentialManager.deleteOAuthRefreshTokenCacheEntry(loginInputSnowflakeOAuth);
    verify(mockSecureStorageManager, times(1)).deleteCredential(oauthRefreshKey);

    SFLoginInput loginInputExternalOAuth = createLoginInputWithExternalOAuth();

    String extOauthAccessKey =
        SecureStorageManager.buildCacheKey(
            new CacheKeyInput(
                CachedCredentialType.OAUTH_ACCESS_TOKEN.getValue(),
                EXTERNAL_OAUTH_TOKEN_URL,
                SNOWFLAKE_HOST,
                SOME_USER,
                ""));
    CredentialManager.deleteOAuthAccessTokenCacheEntry(loginInputExternalOAuth);
    verify(mockSecureStorageManager, times(1)).deleteCredential(extOauthAccessKey);

    String extOauthRefreshKey =
        SecureStorageManager.buildCacheKey(
            new CacheKeyInput(
                CachedCredentialType.OAUTH_REFRESH_TOKEN.getValue(),
                EXTERNAL_OAUTH_TOKEN_URL,
                SNOWFLAKE_HOST,
                SOME_USER,
                ""));
    CredentialManager.deleteOAuthRefreshTokenCacheEntry(loginInputExternalOAuth);
    verify(mockSecureStorageManager, times(1)).deleteCredential(extOauthRefreshKey);

    SFLoginInput loginInputDPoP = createLoginInputWithDPoPPublicKey();
    String dpopKey =
        SecureStorageManager.buildCacheKey(
            new CacheKeyInput(
                CachedCredentialType.DPOP_BUNDLED_ACCESS_TOKEN.getValue(),
                SNOWFLAKE_SERVER_URL,
                SNOWFLAKE_HOST,
                SOME_USER,
                ""));
    CredentialManager.deleteDPoPBundledAccessTokenCacheEntry(loginInputDPoP);
    verify(mockSecureStorageManager, times(1)).deleteCredential(dpopKey);
  }

  // -------------------------------------------------------------------------
  // Fill cached tokens into loginInput
  // -------------------------------------------------------------------------

  @Test
  void shouldProperlyUpdateInputWithTokensFromCache() throws SFException {
    Base64.Encoder encoder = Base64.getEncoder();
    SFLoginInput loginInputSnowflakeOAuth = createLoginInputWithSnowflakeServer();

    String idTokenKey = cacheKey(CachedCredentialType.ID_TOKEN, SNOWFLAKE_HOST, SNOWFLAKE_HOST, "");
    when(mockSecureStorageManager.getCredential(idTokenKey))
        .thenReturn(
            encoder.encodeToString(SOME_ID_TOKEN_FROM_CACHE.getBytes(StandardCharsets.UTF_8)));
    CredentialManager.fillCachedIdToken(loginInputSnowflakeOAuth);

    String mfaTokenKey = cacheKey(CachedCredentialType.MFA_TOKEN, SNOWFLAKE_HOST, SNOWFLAKE_HOST, "");
    when(mockSecureStorageManager.getCredential(mfaTokenKey))
        .thenReturn(
            encoder.encodeToString(SOME_MFA_TOKEN_FROM_CACHE.getBytes(StandardCharsets.UTF_8)));
    CredentialManager.fillCachedMfaToken(loginInputSnowflakeOAuth);

    assertEquals(SOME_ID_TOKEN_FROM_CACHE, loginInputSnowflakeOAuth.getIdToken());
    assertEquals(SOME_MFA_TOKEN_FROM_CACHE, loginInputSnowflakeOAuth.getMfaToken());

    String oauthAccessKey =
        SecureStorageManager.buildCacheKey(
            new CacheKeyInput(
                CachedCredentialType.OAUTH_ACCESS_TOKEN.getValue(),
                SNOWFLAKE_SERVER_URL,
                SNOWFLAKE_HOST,
                SOME_USER,
                ""));
    when(mockSecureStorageManager.getCredential(oauthAccessKey))
        .thenReturn(
            encoder.encodeToString(ACCESS_TOKEN_FROM_CACHE.getBytes(StandardCharsets.UTF_8)));
    CredentialManager.fillCachedOAuthAccessToken(loginInputSnowflakeOAuth);

    String oauthRefreshKey =
        SecureStorageManager.buildCacheKey(
            new CacheKeyInput(
                CachedCredentialType.OAUTH_REFRESH_TOKEN.getValue(),
                SNOWFLAKE_SERVER_URL,
                SNOWFLAKE_HOST,
                SOME_USER,
                ""));
    when(mockSecureStorageManager.getCredential(oauthRefreshKey))
        .thenReturn(
            encoder.encodeToString(REFRESH_TOKEN_FROM_CACHE.getBytes(StandardCharsets.UTF_8)));
    CredentialManager.fillCachedOAuthRefreshToken(loginInputSnowflakeOAuth);

    assertEquals(ACCESS_TOKEN_FROM_CACHE, loginInputSnowflakeOAuth.getOauthAccessToken());
    assertEquals(REFRESH_TOKEN_FROM_CACHE, loginInputSnowflakeOAuth.getOauthRefreshToken());

    SFLoginInput loginInputExternalOAuth = createLoginInputWithExternalOAuth();

    String extOauthAccessKey =
        SecureStorageManager.buildCacheKey(
            new CacheKeyInput(
                CachedCredentialType.OAUTH_ACCESS_TOKEN.getValue(),
                EXTERNAL_OAUTH_TOKEN_URL,
                SNOWFLAKE_HOST,
                SOME_USER,
                ""));
    when(mockSecureStorageManager.getCredential(extOauthAccessKey))
        .thenReturn(
            encoder.encodeToString(
                EXTERNAL_ACCESS_TOKEN_FROM_CACHE.getBytes(StandardCharsets.UTF_8)));
    CredentialManager.fillCachedOAuthAccessToken(loginInputExternalOAuth);

    String extOauthRefreshKey =
        SecureStorageManager.buildCacheKey(
            new CacheKeyInput(
                CachedCredentialType.OAUTH_REFRESH_TOKEN.getValue(),
                EXTERNAL_OAUTH_TOKEN_URL,
                SNOWFLAKE_HOST,
                SOME_USER,
                ""));
    when(mockSecureStorageManager.getCredential(extOauthRefreshKey))
        .thenReturn(
            encoder.encodeToString(
                EXTERNAL_REFRESH_TOKEN_FROM_CACHE.getBytes(StandardCharsets.UTF_8)));
    CredentialManager.fillCachedOAuthRefreshToken(loginInputExternalOAuth);

    assertEquals(EXTERNAL_ACCESS_TOKEN_FROM_CACHE, loginInputExternalOAuth.getOauthAccessToken());
    assertEquals(
        EXTERNAL_REFRESH_TOKEN_FROM_CACHE, loginInputExternalOAuth.getOauthRefreshToken());

    SFLoginInput loginInputDPoP = createLoginInputWithDPoPPublicKey();
    String dpopKey =
        SecureStorageManager.buildCacheKey(
            new CacheKeyInput(
                CachedCredentialType.DPOP_BUNDLED_ACCESS_TOKEN.getValue(),
                SNOWFLAKE_SERVER_URL,
                SNOWFLAKE_HOST,
                SOME_USER,
                ""));
    String dpopBundledToken =
        encoder.encodeToString(SOME_ACCESS_TOKEN.getBytes(StandardCharsets.UTF_8))
            + "."
            + encoder.encodeToString(SOME_DPOP_PUBLIC_KEY.getBytes(StandardCharsets.UTF_8));
    when(mockSecureStorageManager.getCredential(dpopKey)).thenReturn(dpopBundledToken);
    CredentialManager.fillCachedDPoPBundledAccessToken(loginInputDPoP);

    assertEquals(SOME_ACCESS_TOKEN, loginInputDPoP.getOauthAccessToken());
    assertEquals(SOME_DPOP_PUBLIC_KEY, loginInputDPoP.getDPoPPublicKey());
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  /** Builds the expected v2 cache key for a given token type, using host as both idp and snowflake. */
  private static String cacheKey(
      CachedCredentialType type, String idp, String snowflake, String role) {
    return SecureStorageManager.buildCacheKey(
        new CacheKeyInput(type.getValue(), idp, snowflake, SOME_USER, role));
  }

  private SFLoginInput createLoginInputWithExternalOAuth() {
    SFLoginInput loginInput = createGenericLoginInput();
    loginInput.setOauthLoginInput(
        new SFOauthLoginInput(null, null, null, null, EXTERNAL_OAUTH_TOKEN_URL, null));
    loginInput.setServerUrl(SNOWFLAKE_SERVER_URL);
    return loginInput;
  }

  private SFLoginInput createLoginInputWithSnowflakeServer() {
    SFLoginInput loginInput = createGenericLoginInput();
    loginInput.setOauthLoginInput(new SFOauthLoginInput(null, null, null, null, null, null));
    loginInput.setServerUrl(SNOWFLAKE_SERVER_URL);
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
