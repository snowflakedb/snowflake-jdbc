/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import com.google.common.base.Strings;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

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
  void resetSecureStorageManager() {
    logger.debug("Resetting the secure storage manager");
    initSecureStorageManager();
  }

  /**
   * Testing purpose. Inject a mock manager.
   *
   * @param manager SecureStorageManager
   */
  void injectSecureStorageManager(SecureStorageManager manager) {
    logger.debug("Injecting secure storage manager");
    secureStorageManager = manager;
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
    logger.debug(
        "Looking for cached id token for user: {}, host: {}",
        loginInput.getUserName(),
        loginInput.getHostFromServerUrl());
    getInstance().fillCachedCredential(loginInput, CachedCredentialType.ID_TOKEN);
  }

  /**
   * Reuse the cached mfa token stored locally
   *
   * @param loginInput login input to attach mfa token
   */
  static void fillCachedMfaToken(SFLoginInput loginInput) throws SFException {
    logger.debug(
        "Looking for cached mfa token for user: {}, host: {}",
        loginInput.getUserName(),
        loginInput.getHostFromServerUrl());
    getInstance().fillCachedCredential(loginInput, CachedCredentialType.MFA_TOKEN);
  }

  /**
   * Reuse the cached OAuth access token stored locally
   *
   * @param loginInput login input to attach access token
   */
  static void fillCachedOAuthAccessToken(SFLoginInput loginInput) throws SFException {
    logger.debug(
        "Looking for cached OAuth access token for user: {}, host: {}",
        loginInput.getUserName(),
        loginInput.getHostFromServerUrl());
    getInstance().fillCachedCredential(loginInput, CachedCredentialType.OAUTH_ACCESS_TOKEN);
  }

  /**
   * Reuse the cached OAuth refresh token stored locally
   *
   * @param loginInput login input to attach refresh token
   */
  static void fillCachedOAuthRefreshToken(SFLoginInput loginInput) throws SFException {
    logger.debug(
        "Looking for cached OAuth refresh token for user: {}, host: {}",
        loginInput.getUserName(),
        loginInput.getHostFromServerUrl());
    getInstance().fillCachedCredential(loginInput, CachedCredentialType.OAUTH_REFRESH_TOKEN);
  }

  /**
   * Reuse the cached token stored locally
   *
   * @param loginInput login input to attach token
   * @param credType credential type to retrieve
   */
  synchronized void fillCachedCredential(SFLoginInput loginInput, CachedCredentialType credType)
      throws SFException {
    if (secureStorageManager == null) {
      logMissingJnaJarForSecureLocalStorage();
      return;
    }

    String cred;
    try {
      cred =
          secureStorageManager.getCredential(
              loginInput.getHostFromServerUrl(), loginInput.getUserName(), credType.getValue());
    } catch (NoClassDefFoundError error) {
      logMissingJnaJarForSecureLocalStorage();
      return;
    }

    if (cred == null) {
      logger.debug("Retrieved {} is null", credType);
    }

    logger.debug(
        "Setting {}{} token for user: {}, host: {}",
        cred == null ? "null " : "",
        credType.getValue(),
        loginInput.getUserName(),
        loginInput.getHostFromServerUrl());
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
      default:
        logger.debug("Unrecognized type {} for local cached credential", credType);
        break;
    }
  }

  /**
   * Store ID Token
   *
   * @param loginInput loginInput to denote to the cache
   * @param loginOutput loginOutput to denote to the cache
   */
  static void writeIdToken(SFLoginInput loginInput, SFLoginOutput loginOutput) throws SFException {
    logger.debug(
        "Caching id token in a secure storage for user: {}, host: {}",
        loginInput.getUserName(),
        loginInput.getHostFromServerUrl());
    getInstance()
        .writeTemporaryCredential(
            loginInput, loginOutput.getIdToken(), CachedCredentialType.ID_TOKEN);
  }

  /**
   * Store MFA Token
   *
   * @param loginInput loginInput to denote to the cache
   * @param loginOutput loginOutput to denote to the cache
   */
  static void writeMfaToken(SFLoginInput loginInput, SFLoginOutput loginOutput) throws SFException {
    logger.debug(
        "Caching mfa token in a secure storage for user: {}, host: {}",
        loginInput.getUserName(),
        loginInput.getHostFromServerUrl());
    getInstance()
        .writeTemporaryCredential(
            loginInput, loginOutput.getMfaToken(), CachedCredentialType.MFA_TOKEN);
  }

  /**
   * Store OAuth Access Token
   *
   * @param loginInput loginInput to denote to the cache
   */
  static void writeOAuthAccessToken(SFLoginInput loginInput) throws SFException {
    logger.debug(
        "Caching OAuth access token in a secure storage for user: {}, host: {}",
        loginInput.getUserName(),
        loginInput.getHostFromServerUrl());
    getInstance()
        .writeTemporaryCredential(
            loginInput, loginInput.getOauthAccessToken(), CachedCredentialType.OAUTH_ACCESS_TOKEN);
  }

  /**
   * Store OAuth Refresh Token
   *
   * @param loginInput loginInput to denote to the cache
   */
  static void writeOAuthRefreshToken(SFLoginInput loginInput) throws SFException {
    logger.debug(
        "Caching OAuth refresh token in a secure storage for user: {}, host: {}",
        loginInput.getUserName(),
        loginInput.getHostFromServerUrl());
    getInstance()
        .writeTemporaryCredential(
            loginInput,
            loginInput.getOauthRefreshToken(),
            CachedCredentialType.OAUTH_REFRESH_TOKEN);
  }

  /**
   * Store the temporary credential
   *
   * @param loginInput loginInput to denote to the cache
   * @param cred the credential
   * @param credType type of the credential
   */
  synchronized void writeTemporaryCredential(
      SFLoginInput loginInput, String cred, CachedCredentialType credType) throws SFException {
    if (Strings.isNullOrEmpty(cred)) {
      logger.debug("No {} is given.", credType);
      return; // no credential
    }

    if (secureStorageManager == null) {
      logMissingJnaJarForSecureLocalStorage();
      return;
    }

    try {
      secureStorageManager.setCredential(
          loginInput.getHostFromServerUrl(), loginInput.getUserName(), credType.getValue(), cred);
    } catch (NoClassDefFoundError error) {
      logMissingJnaJarForSecureLocalStorage();
    }
  }

  /** Delete the id token cache */
  static void deleteIdTokenCache(String host, String user) {
    logger.debug(
        "Removing cached id token from a secure storage for user: {}, host: {}", user, host);
    getInstance().deleteTemporaryCredential(host, user, CachedCredentialType.ID_TOKEN);
  }

  /** Delete the mfa token cache */
  static void deleteMfaTokenCache(String host, String user) {
    logger.debug(
        "Removing cached mfa token from a secure storage for user: {}, host: {}", user, host);
    getInstance().deleteTemporaryCredential(host, user, CachedCredentialType.MFA_TOKEN);
  }

  /** Delete the OAuth access token cache */
  static void deleteOAuthAccessTokenCache(String host, String user) {
    logger.debug(
        "Removing cached OAuth access token from a secure storage for user: {}, host: {}",
        user,
        host);
    getInstance().deleteTemporaryCredential(host, user, CachedCredentialType.OAUTH_ACCESS_TOKEN);
  }

  /** Delete the OAuth refresh token cache */
  static void deleteOAuthRefreshTokenCache(String host, String user) {
    logger.debug(
        "Removing cached OAuth refresh token from a secure storage for user: {}, host: {}",
        user,
        host);
    getInstance().deleteTemporaryCredential(host, user, CachedCredentialType.OAUTH_REFRESH_TOKEN);
  }

  /**
   * Delete the temporary credential
   *
   * @param host host name
   * @param user user name
   * @param credType type of the credential
   */
  synchronized void deleteTemporaryCredential(
      String host, String user, CachedCredentialType credType) {
    if (secureStorageManager == null) {
      logMissingJnaJarForSecureLocalStorage();
      return;
    }

    try {
      secureStorageManager.deleteCredential(host, user, credType.getValue());
    } catch (NoClassDefFoundError error) {
      logMissingJnaJarForSecureLocalStorage();
    }
  }

  private static void logMissingJnaJarForSecureLocalStorage() {
    logger.warn(
        "JNA jar files are needed for Secure Local Storage service. Please follow the Snowflake JDBC instruction for Secure Local Storage feature. Fall back to normal process.",
        false);
  }
}
