/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import com.google.common.base.Strings;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;

public class CredentialManager {
  private static final SFLogger logger = SFLoggerFactory.getLogger(CredentialManager.class);

  private SecureStorageManager secureStorageManager;

  private static final String ID_TOKEN = "ID_TOKEN";
  private static final String MFA_TOKEN = "MFATOKEN";

  private CredentialManager() {
    if (Constants.getOS() == Constants.OS.MAC) {
      secureStorageManager = SecureStorageAppleManager.builder();
    } else if (Constants.getOS() == Constants.OS.WINDOWS) {
      secureStorageManager = SecureStorageWindowsManager.builder();
    } else if (Constants.getOS() == Constants.OS.LINUX) {
      secureStorageManager = SecureStorageLinuxManager.builder();
    } else {
      logger.error("Unsupported Operating System. Expected: OSX, Windows, Linux");
    }
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
  synchronized void fillCachedIdToken(SFLoginInput loginInput) throws SFException {
    String idToken =
        secureStorageManager.getCredential(
            extractHostFromServerUrl(loginInput.getServerUrl()), loginInput.getUserName(), ID_TOKEN);

    if (idToken == null) {
      logger.debug("retrieved idToken is null");
    }
    loginInput.setIdToken(idToken); // idToken can be null
    return;
  }

  /**
   * Reuse the cached mfa token stored locally
   *
   * @param loginInput login input to attach mfa token
   */
  synchronized void fillCachedMfaToken(SFLoginInput loginInput) throws SFException {
    String mfaToken =
        secureStorageManager.getCredential(
            extractHostFromServerUrl(loginInput.getServerUrl()), loginInput.getUserName(), MFA_TOKEN);

    if (mfaToken == null) {
      logger.debug("retrieved mfaToken is null");
    }
    loginInput.setMfaToken(mfaToken); // mfaToken can be null
    return;
  }

  /**
   * Store the temporary credential
   *
   * @param loginInput loginInput to denote to the cache
   * @param loginOutput loginOutput to denote to the cache
   */
  synchronized void writeTemporaryCredential(SFLoginInput loginInput, SFLoginOutput loginOutput)
      throws SFException {
    String idToken = loginOutput.getIdToken();
    if (Strings.isNullOrEmpty(idToken)) {
      logger.debug("no idToken is given.");
      return; // no idToken
    }

    secureStorageManager.setCredential(
        extractHostFromServerUrl(loginInput.getServerUrl()), loginInput.getUserName(), ID_TOKEN, idToken);
  }

  synchronized void writeMfaToken(SFLoginInput loginInput, SFLoginOutput loginOutput)
      throws SFException {
      // WUFAN TODO:
      String mfaToken = loginOutput.getMfaToken();
      if (Strings.isNullOrEmpty(mfaToken)) {
        logger.debug("no username_pwd_mfa token is given.");
        return; // no mfa token
      }

    secureStorageManager.setCredential(
        extractHostFromServerUrl(loginInput.getServerUrl()), loginInput.getUserName(), MFA_TOKEN, mfaToken);
  }

  /** Delete the id token cache */
  void deleteIdTokenCache(String host, String user) {
    secureStorageManager.deleteCredential(host, user, ID_TOKEN);
  }

  /** Delete the mfa token cache */
  void deleteMfaTokenCache(String host, String user) {
    secureStorageManager.deleteCredential(host, user, MFA_TOKEN);
  }

  /**
   * Used to extract host name from a well formated internal serverUrl, e.g., serverUrl in
   * SFLoginInput.
   */
  private String extractHostFromServerUrl(String serverUrl) throws SFException {
    URL url = null;
    try {
      url = new URL(serverUrl);
    } catch (MalformedURLException e) {
      logger.error("Invalid serverUrl for retrieving host name");
      throw new SFException(ErrorCode.INTERNAL_ERROR, "Invalid serverUrl for retrieving host name");
    }
    return url.getHost();
  }
}
