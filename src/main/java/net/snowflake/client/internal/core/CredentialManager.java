package net.snowflake.client.internal.core;

import static net.snowflake.client.internal.jdbc.SnowflakeUtil.isNullOrEmpty;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import net.snowflake.client.api.exception.ErrorCode;
import net.snowflake.client.internal.log.SFLogger;
import net.snowflake.client.internal.log.SFLoggerFactory;

public class CredentialManager {
  private static final SFLogger logger = SFLoggerFactory.getLogger(CredentialManager.class);

  private SecureStorageManager secureStorageManager;

  private CredentialManager() {
    initSecureStorageManager();
  }

  private void initSecureStorageManager() {
    try {
      if (Constants.getOS() == Constants.OS.MAC) {
        secureStorageManager = SecureStorageAppleManager.builder();
      } else if (Constants.getOS() == Constants.OS.WINDOWS) {
        secureStorageManager = SecureStorageWindowsManager.builder();
      } else if (Constants.getOS() == Constants.OS.LINUX) {
        secureStorageManager = SecureStorageLinuxManager.getInstance();
      } else {
        logger.error("Unsupported Operating System. Expected: OSX, Windows, Linux", false);
      }
    } catch (NoClassDefFoundError error) {
      logMissingJnaJarForSecureLocalStorage();
    }
  }

  /** Helper function for tests to go back to normal settings. */
  static void resetSecureStorageManager() {
    logger.debug("Resetting the secure storage manager");
    getInstance().initSecureStorageManager();
  }

  /**
   * Testing purpose. Inject a mock manager.
   *
   * @param manager SecureStorageManager
   */
  static void injectSecureStorageManager(SecureStorageManager manager) {
    logger.debug("Injecting secure storage manager");
    getInstance().secureStorageManager = manager;
  }

  private static class CredentialManagerHolder {
    private static final CredentialManager INSTANCE = new CredentialManager();
  }

  public static CredentialManager getInstance() {
    return CredentialManagerHolder.INSTANCE;
  }

  /**
   * Reuse the cached id token stored locally
   *
   * @param loginInput login input to attach id token
   */
  static void fillCachedIdToken(SFLoginInput loginInput) throws SFException {
    if (isNullOrEmpty(loginInput.getUserName())) {
      logger.debug("Missing username; Cannot read from credential cache");
      return;
    }
    String host = loginInput.getHostFromServerUrl();
    String cacheKey =
        SecureStorageManager.buildCacheKey(
            new CacheKeyInput(
                CachedCredentialType.ID_TOKEN.getValue(),
                host,
                host,
                loginInput.getUserName(),
                emptyIfNull(loginInput.getRole())));
    logger.debug(
        "Looking for cached id token for user: {}, host: {}",
        loginInput.getUserName(),
        loginInput.getHostFromServerUrl());
    getInstance().fillCachedCredential(loginInput, cacheKey, CachedCredentialType.ID_TOKEN);
  }

  /**
   * Reuse the cached mfa token stored locally
   *
   * @param loginInput login input to attach mfa token
   */
  static void fillCachedMfaToken(SFLoginInput loginInput) throws SFException {
    if (isNullOrEmpty(loginInput.getUserName())) {
      logger.debug("Missing username; Cannot read from credential cache");
      return;
    }
    String host = loginInput.getHostFromServerUrl();
    String cacheKey =
        SecureStorageManager.buildCacheKey(
            new CacheKeyInput(
                CachedCredentialType.MFA_TOKEN.getValue(),
                host,
                host,
                loginInput.getUserName(),
                ""));
    logger.debug(
        "Looking for cached mfa token for user: {}, host: {}",
        loginInput.getUserName(),
        loginInput.getHostFromServerUrl());
    getInstance().fillCachedCredential(loginInput, cacheKey, CachedCredentialType.MFA_TOKEN);
  }

  /**
   * Reuse the cached OAuth access token stored locally
   *
   * @param loginInput login input to attach access token
   */
  static void fillCachedOAuthAccessToken(SFLoginInput loginInput) throws SFException {
    String idpUrl = getIdpUrlForOAuthCacheKey(loginInput);
    String cacheKey =
        SecureStorageManager.buildCacheKey(
            new CacheKeyInput(
                CachedCredentialType.OAUTH_ACCESS_TOKEN.getValue(),
                idpUrl,
                loginInput.getHostFromServerUrl(),
                loginInput.getUserName(),
                emptyIfNull(loginInput.getRole())));
    logger.debug(
        "Looking for cached OAuth access token for user: {}, idp: {}",
        loginInput.getUserName(),
        idpUrl);
    getInstance()
        .fillCachedCredential(loginInput, cacheKey, CachedCredentialType.OAUTH_ACCESS_TOKEN);
  }

  /**
   * Reuse the cached OAuth refresh token stored locally
   *
   * @param loginInput login input to attach refresh token
   */
  static void fillCachedOAuthRefreshToken(SFLoginInput loginInput) throws SFException {
    String idpUrl = getIdpUrlForOAuthCacheKey(loginInput);
    String cacheKey =
        SecureStorageManager.buildCacheKey(
            new CacheKeyInput(
                CachedCredentialType.OAUTH_REFRESH_TOKEN.getValue(),
                idpUrl,
                loginInput.getHostFromServerUrl(),
                loginInput.getUserName(),
                emptyIfNull(loginInput.getRole())));
    logger.debug(
        "Looking for cached OAuth refresh token for user: {}, idp: {}",
        loginInput.getUserName(),
        idpUrl);
    getInstance()
        .fillCachedCredential(loginInput, cacheKey, CachedCredentialType.OAUTH_REFRESH_TOKEN);
  }

  /**
   * Reuse the cached OAuth access token &amp; DPoP public key tied to it
   *
   * @param loginInput login input to attach refresh token
   */
  static void fillCachedDPoPBundledAccessToken(SFLoginInput loginInput) throws SFException {
    String idpUrl = getIdpUrlForOAuthCacheKey(loginInput);
    String cacheKey =
        SecureStorageManager.buildCacheKey(
            new CacheKeyInput(
                CachedCredentialType.DPOP_BUNDLED_ACCESS_TOKEN.getValue(),
                idpUrl,
                loginInput.getHostFromServerUrl(),
                loginInput.getUserName(),
                emptyIfNull(loginInput.getRole())));
    logger.debug(
        "Looking for cached DPoP public key for user: {}, idp: {}",
        loginInput.getUserName(),
        idpUrl);
    getInstance()
        .fillCachedCredential(
            loginInput, cacheKey, CachedCredentialType.DPOP_BUNDLED_ACCESS_TOKEN);
  }

  /** Reuse the cached token stored locally */
  synchronized void fillCachedCredential(
      SFLoginInput loginInput, String cacheKey, CachedCredentialType credType) throws SFException {
    if (secureStorageManager == null) {
      logMissingJnaJarForSecureLocalStorage();
      return;
    }

    String base64EncodedCred, cred = null;
    try {
      base64EncodedCred = secureStorageManager.getCredential(cacheKey);
    } catch (NoClassDefFoundError error) {
      logMissingJnaJarForSecureLocalStorage();
      return;
    }

    if (base64EncodedCred == null) {
      logger.debug("Retrieved {} is null", credType);
    }

    logger.debug(
        "Setting {}{} token for user: {}",
        base64EncodedCred == null ? "null " : "",
        credType.getValue(),
        loginInput.getUserName());

    if (base64EncodedCred != null && credType != CachedCredentialType.DPOP_BUNDLED_ACCESS_TOKEN) {
      try {
        cred = new String(Base64.getDecoder().decode(base64EncodedCred));
      } catch (Exception e) {
        // handle legacy non-base64 encoded cache values
        deleteTemporaryCredential(cacheKey, credType);
        return;
      }
    }
    switch (credType) {
      case ID_TOKEN:
        loginInput.setIdToken(cred);
        break;
      case MFA_TOKEN:
        loginInput.setMfaToken(cred);
        break;
      case OAUTH_ACCESS_TOKEN:
        loginInput.setOauthAccessToken(cred);
        break;
      case OAUTH_REFRESH_TOKEN:
        loginInput.setOauthRefreshToken(cred);
        break;
      case DPOP_BUNDLED_ACCESS_TOKEN:
        updateInputWithTokenAndPublicKey(base64EncodedCred, loginInput);
        break;
      default:
        throw new SFException(
            ErrorCode.INTERNAL_ERROR, "Unrecognized type {} for local cached credential", credType);
    }
  }

  private void updateInputWithTokenAndPublicKey(String cred, SFLoginInput loginInput)
      throws SFException {
    if (!isNullOrEmpty(cred)) {
      String[] values = cred.split("\\.");
      if (values.length != 2) {
        throw new SFException(
            ErrorCode.INTERNAL_ERROR, "Invalid DPoP bundled access token credential format");
      }
      Base64.Decoder decoder = Base64.getDecoder();
      loginInput.setOauthAccessToken(new String(decoder.decode(values[0])));
      loginInput.setDPoPPublicKey(new String(decoder.decode(values[1])));
    }
  }

  static void writeIdToken(SFLoginInput loginInput, String idToken) throws SFException {
    String host = loginInput.getHostFromServerUrl();
    String cacheKey =
        SecureStorageManager.buildCacheKey(
            new CacheKeyInput(
                CachedCredentialType.ID_TOKEN.getValue(),
                host,
                host,
                loginInput.getUserName(),
                emptyIfNull(loginInput.getRole())));
    logger.debug(
        "Caching id token in a secure storage for user: {}, host: {}",
        loginInput.getUserName(),
        host);
    getInstance().writeTemporaryCredential(cacheKey, idToken, CachedCredentialType.ID_TOKEN);
  }

  static void writeMfaToken(SFLoginInput loginInput, String mfaToken) throws SFException {
    String host = loginInput.getHostFromServerUrl();
    String cacheKey =
        SecureStorageManager.buildCacheKey(
            new CacheKeyInput(
                CachedCredentialType.MFA_TOKEN.getValue(),
                host,
                host,
                loginInput.getUserName(),
                ""));
    logger.debug(
        "Caching mfa token in a secure storage for user: {}, host: {}",
        loginInput.getUserName(),
        host);
    getInstance().writeTemporaryCredential(cacheKey, mfaToken, CachedCredentialType.MFA_TOKEN);
  }

  /**
   * Store OAuth Access Token
   *
   * @param loginInput loginInput to denote to the cache
   */
  static void writeOAuthAccessToken(SFLoginInput loginInput) throws SFException {
    String idpUrl = getIdpUrlForOAuthCacheKey(loginInput);
    String cacheKey =
        SecureStorageManager.buildCacheKey(
            new CacheKeyInput(
                CachedCredentialType.OAUTH_ACCESS_TOKEN.getValue(),
                idpUrl,
                loginInput.getHostFromServerUrl(),
                loginInput.getUserName(),
                emptyIfNull(loginInput.getRole())));
    logger.debug(
        "Caching OAuth access token in a secure storage for user: {}, idp: {}",
        loginInput.getUserName(),
        idpUrl);
    getInstance()
        .writeTemporaryCredential(
            cacheKey, loginInput.getOauthAccessToken(), CachedCredentialType.OAUTH_ACCESS_TOKEN);
  }

  /**
   * Store OAuth Refresh Token
   *
   * @param loginInput loginInput to denote to the cache
   */
  static void writeOAuthRefreshToken(SFLoginInput loginInput) throws SFException {
    String idpUrl = getIdpUrlForOAuthCacheKey(loginInput);
    String cacheKey =
        SecureStorageManager.buildCacheKey(
            new CacheKeyInput(
                CachedCredentialType.OAUTH_REFRESH_TOKEN.getValue(),
                idpUrl,
                loginInput.getHostFromServerUrl(),
                loginInput.getUserName(),
                emptyIfNull(loginInput.getRole())));
    logger.debug(
        "Caching OAuth refresh token in a secure storage for user: {}, idp: {}",
        loginInput.getUserName(),
        idpUrl);
    getInstance()
        .writeTemporaryCredential(
            cacheKey, loginInput.getOauthRefreshToken(), CachedCredentialType.OAUTH_REFRESH_TOKEN);
  }

  /**
   * Store OAuth DPoP Public Key With Token
   *
   * @param loginInput loginInput to denote to the cache
   */
  static void writeDPoPBundledAccessToken(SFLoginInput loginInput) throws SFException {
    String idpUrl = getIdpUrlForOAuthCacheKey(loginInput);
    String cacheKey =
        SecureStorageManager.buildCacheKey(
            new CacheKeyInput(
                CachedCredentialType.DPOP_BUNDLED_ACCESS_TOKEN.getValue(),
                idpUrl,
                loginInput.getHostFromServerUrl(),
                loginInput.getUserName(),
                emptyIfNull(loginInput.getRole())));
    logger.debug(
        "Caching DPoP public key in a secure storage for user: {}, idp: {}",
        loginInput.getUserName(),
        idpUrl);
    Base64.Encoder encoder = Base64.getEncoder();
    String tokenBase64 =
        encoder.encodeToString(loginInput.getOauthAccessToken().getBytes(StandardCharsets.UTF_8));
    String publicKeyBase64 =
        encoder.encodeToString(loginInput.getDPoPPublicKey().getBytes(StandardCharsets.UTF_8));
    getInstance()
        .writeTemporaryCredential(
            cacheKey,
            tokenBase64 + "." + publicKeyBase64,
            CachedCredentialType.DPOP_BUNDLED_ACCESS_TOKEN);
  }

  /** Store the temporary credential */
  synchronized void writeTemporaryCredential(
      String cacheKey, String cred, CachedCredentialType credType) {
    if (isNullOrEmpty(cacheKey)) {
      logger.debug("Missing cache key; Cannot write to credential cache");
      return;
    }
    if (isNullOrEmpty(cred)) {
      logger.debug("No {} is given.", credType);
      return;
    }

    if (secureStorageManager == null) {
      logMissingJnaJarForSecureLocalStorage();
      return;
    }

    try {
      if (credType == CachedCredentialType.DPOP_BUNDLED_ACCESS_TOKEN) {
        secureStorageManager.setCredential(cacheKey, cred);
      } else {
        String base64EncodedCred =
            Base64.getEncoder().encodeToString(cred.getBytes(StandardCharsets.UTF_8));
        secureStorageManager.setCredential(cacheKey, base64EncodedCred);
      }
    } catch (NoClassDefFoundError error) {
      logMissingJnaJarForSecureLocalStorage();
    }
  }

  /** Delete the id token cache */
  static void deleteIdTokenCacheEntry(String host, String user) {
    String cacheKey =
        SecureStorageManager.buildCacheKey(
            new CacheKeyInput(CachedCredentialType.ID_TOKEN.getValue(), host, host, user, ""));
    logger.debug(
        "Removing cached id token from a secure storage for user: {}, host: {}", user, host);
    getInstance().deleteTemporaryCredential(cacheKey, CachedCredentialType.ID_TOKEN);
  }

  /** Delete the mfa token cache */
  static void deleteMfaTokenCacheEntry(String host, String user) {
    String cacheKey =
        SecureStorageManager.buildCacheKey(
            new CacheKeyInput(CachedCredentialType.MFA_TOKEN.getValue(), host, host, user, ""));
    logger.debug(
        "Removing cached mfa token from a secure storage for user: {}, host: {}", user, host);
    getInstance().deleteTemporaryCredential(cacheKey, CachedCredentialType.MFA_TOKEN);
  }

  /** Delete the Oauth access token cache */
  static void deleteOAuthAccessTokenCacheEntry(String host, String user) {
    String cacheKey =
        SecureStorageManager.buildCacheKey(
            new CacheKeyInput(
                CachedCredentialType.OAUTH_ACCESS_TOKEN.getValue(), host, host, user, ""));
    logger.debug(
        "Removing cached oauth access token from a secure storage for user: {}, host: {}",
        user,
        host);
    getInstance().deleteTemporaryCredential(cacheKey, CachedCredentialType.OAUTH_ACCESS_TOKEN);
  }

  /** Delete the Oauth refresh token cache */
  static void deleteOAuthRefreshTokenCacheEntry(String host, String user) {
    String cacheKey =
        SecureStorageManager.buildCacheKey(
            new CacheKeyInput(
                CachedCredentialType.OAUTH_REFRESH_TOKEN.getValue(), host, host, user, ""));
    logger.debug(
        "Removing cached OAuth refresh token from a secure storage for user: {}, host: {}",
        user,
        host);
    getInstance().deleteTemporaryCredential(cacheKey, CachedCredentialType.OAUTH_REFRESH_TOKEN);
  }

  /** Delete the DPoP bundled access token cache */
  static void deleteDPoPBundledAccessTokenCacheEntry(String host, String user) {
    String cacheKey =
        SecureStorageManager.buildCacheKey(
            new CacheKeyInput(
                CachedCredentialType.DPOP_BUNDLED_ACCESS_TOKEN.getValue(), host, host, user, ""));
    logger.debug(
        "Removing cached DPoP public key from a secure storage for user: {}, host: {}", user, host);
    getInstance()
        .deleteTemporaryCredential(cacheKey, CachedCredentialType.DPOP_BUNDLED_ACCESS_TOKEN);
  }

  /** Delete the OAuth access token cache */
  static void deleteOAuthAccessTokenCacheEntry(SFLoginInput loginInput) throws SFException {
    String idpUrl = getIdpUrlForOAuthCacheKey(loginInput);
    String cacheKey =
        SecureStorageManager.buildCacheKey(
            new CacheKeyInput(
                CachedCredentialType.OAUTH_ACCESS_TOKEN.getValue(),
                idpUrl,
                loginInput.getHostFromServerUrl(),
                loginInput.getUserName(),
                emptyIfNull(loginInput.getRole())));
    getInstance().deleteTemporaryCredential(cacheKey, CachedCredentialType.OAUTH_ACCESS_TOKEN);
  }

  /** Delete the OAuth refresh token cache */
  static void deleteOAuthRefreshTokenCacheEntry(SFLoginInput loginInput) throws SFException {
    String idpUrl = getIdpUrlForOAuthCacheKey(loginInput);
    String cacheKey =
        SecureStorageManager.buildCacheKey(
            new CacheKeyInput(
                CachedCredentialType.OAUTH_REFRESH_TOKEN.getValue(),
                idpUrl,
                loginInput.getHostFromServerUrl(),
                loginInput.getUserName(),
                emptyIfNull(loginInput.getRole())));
    getInstance().deleteTemporaryCredential(cacheKey, CachedCredentialType.OAUTH_REFRESH_TOKEN);
  }

  /** Delete the DPoP bundled access token cache */
  static void deleteDPoPBundledAccessTokenCacheEntry(SFLoginInput loginInput) throws SFException {
    String idpUrl = getIdpUrlForOAuthCacheKey(loginInput);
    String cacheKey =
        SecureStorageManager.buildCacheKey(
            new CacheKeyInput(
                CachedCredentialType.DPOP_BUNDLED_ACCESS_TOKEN.getValue(),
                idpUrl,
                loginInput.getHostFromServerUrl(),
                loginInput.getUserName(),
                emptyIfNull(loginInput.getRole())));
    getInstance()
        .deleteTemporaryCredential(cacheKey, CachedCredentialType.DPOP_BUNDLED_ACCESS_TOKEN);
  }

  /**
   * Returns the IdP token-endpoint URL for OAuth cache key construction. Returns the full
   * token-request URL when available, otherwise falls back to the Snowflake server URL.
   */
  static String getIdpUrlForOAuthCacheKey(SFLoginInput loginInput) throws SFException {
    String url = loginInput.getOauthLoginInput().getTokenRequestUrl();
    return (url != null) ? url : loginInput.getServerUrl();
  }

  /**
   * Delete the temporary credential
   *
   * @param cacheKey pre-built v2 cache key
   * @param credType type of the credential
   */
  synchronized void deleteTemporaryCredential(String cacheKey, CachedCredentialType credType) {
    if (secureStorageManager == null) {
      logMissingJnaJarForSecureLocalStorage();
      return;
    }

    try {
      secureStorageManager.deleteCredential(cacheKey);
    } catch (NoClassDefFoundError error) {
      logMissingJnaJarForSecureLocalStorage();
    }
  }

  private static String emptyIfNull(String s) {
    return s != null ? s : "";
  }

  private static void logMissingJnaJarForSecureLocalStorage() {
    logger.warn(
        "JNA jar files are needed for Secure Local Storage service. Please follow the Snowflake JDBC instruction for Secure Local Storage feature. Fall back to normal process.",
        false);
  }
}
