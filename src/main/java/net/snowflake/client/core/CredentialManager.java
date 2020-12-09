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

  private static final String ID_TOKEN = "ID_TOKEN";
  private static final String MFA_TOKEN = "MFATOKEN";

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
        logger.error("Unsupported Operating System. Expected: OSX, Windows, Linux");
      }
    } catch (NoClassDefFoundError error) {
      logger.info(
          "JNA jar files are needed for Secure Local Storage service. Please follow the Snowflake JDBC instruction for Secure Local Storage feature. Fall back to normal process.");
    }
  }

  /** Helper function for tests to go back to normal settings. */
  void resetSecureStorageManager() {
    initSecureStorageManager();
  }

  /**
   * Testing purpose. Inject a mock manager.
   *
   * @param manager
   */
  void injectSecureStorageManager(SecureStorageManager manager) {
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
  void fillCachedIdToken(SFLoginInput loginInput) throws SFException {
    fillCachedCredential(loginInput, ID_TOKEN);
  }

  /**
   * Reuse the cached mfa token stored locally
   *
   * @param loginInput login input to attach mfa token
   */
  void fillCachedMfaToken(SFLoginInput loginInput) throws SFException {
    fillCachedCredential(loginInput, MFA_TOKEN);
  }

  /**
   * Reuse the cached token stored locally
   *
   * @param loginInput login input to attach token
   * @param credType credential type to retrieve
   */
  synchronized void fillCachedCredential(SFLoginInput loginInput, String credType)
      throws SFException {
    if (secureStorageManager == null) {
      logger.info(
          "JNA jar files are needed for Secure Local Storage service. Please follow the Snowflake JDBC instruction for Secure Local Storage feature. Fall back to normal process.");
      return;
    }

    String cred = null;
    try {
      cred =
          secureStorageManager.getCredential(
              loginInput.getHostFromServerUrl(), loginInput.getUserName(), credType);
    } catch (NoClassDefFoundError error) {
      logger.info(
          "JNA jar files are needed for Secure Local Storage service. Please follow the Snowflake JDBC instruction for Secure Local Storage feature. Fall back to normal process.");
      return;
    }

    if (cred == null) {
      logger.debug("retrieved %s is null", credType);
    }

    // cred can be null
    if (credType == ID_TOKEN) {
      loginInput.setIdToken(cred);
    } else if (credType == MFA_TOKEN) {
      loginInput.setMfaToken(cred);
    } else {
      logger.debug("unrecognized type %s for local cached credential", credType);
    }
    return;
  }

  /**
   * Store ID Token
   *
   * @param loginInput loginInput to denote to the cache
   * @param loginOutput loginOutput to denote to the cache
   */
  void writeIdToken(SFLoginInput loginInput, SFLoginOutput loginOutput) throws SFException {
    writeTemporaryCredential(loginInput, loginOutput.getIdToken(), ID_TOKEN);
  }

  /**
   * Store MFA Token
   *
   * @param loginInput loginInput to denote to the cache
   * @param loginOutput loginOutput to denote to the cache
   */
  void writeMfaToken(SFLoginInput loginInput, SFLoginOutput loginOutput) throws SFException {
    writeTemporaryCredential(loginInput, loginOutput.getMfaToken(), MFA_TOKEN);
  }

  /**
   * Store the temporary credential
   *
   * @param loginInput loginInput to denote to the cache
   * @param cred the credential
   * @param credType type of the credential
   */
  synchronized void writeTemporaryCredential(SFLoginInput loginInput, String cred, String credType)
      throws SFException {
    if (Strings.isNullOrEmpty(cred)) {
      logger.debug("no %s is given.", credType);
      return; // no credential
    }

    if (secureStorageManager == null) {
      logger.info(
          "JNA jar files are needed for Secure Local Storage service. Please follow the Snowflake JDBC instruction for Secure Local Storage feature. Fall back to normal process.");
      return;
    }

    try {
      secureStorageManager.setCredential(
          loginInput.getHostFromServerUrl(), loginInput.getUserName(), credType, cred);
    } catch (NoClassDefFoundError error) {
      logger.info(
          "JNA jar files are needed for Secure Local Storage service. Please follow the Snowflake JDBC instruction for Secure Local Storage feature. Fall back to normal process.");
    }
  }

  /** Delete the id token cache */
  void deleteIdTokenCache(String host, String user) {
    deleteTemporaryCredential(host, user, ID_TOKEN);
  }

  /** Delete the mfa token cache */
  void deleteMfaTokenCache(String host, String user) {
    deleteTemporaryCredential(host, user, MFA_TOKEN);
  }

  /**
   * Delete the temporary credential
   *
   * @param host host name
   * @param user user name
   * @param credType type of the credential
   */
  synchronized void deleteTemporaryCredential(String host, String user, String credType) {
    if (secureStorageManager == null) {
      logger.info(
          "JNA jar files are needed for Secure Local Storage service. Please follow the Snowflake JDBC instruction for Secure Local Storage feature. Fall back to normal process.");
      return;
    }

    try {
      secureStorageManager.deleteCredential(host, user, credType);
    } catch (NoClassDefFoundError error) {
      logger.info(
          "JNA jar files are needed for Secure Local Storage service. Please follow the Snowflake JDBC instruction for Secure Local Storage feature. Fall back to normal process.");
    }
  }
}
