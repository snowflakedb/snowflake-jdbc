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
import java.util.HashMap;
import java.util.Map;

public class CredentialManager
{
  private static final
  SFLogger logger = SFLoggerFactory.getLogger(CredentialManager.class);
  private SecureStorageManager secureStorageManager;

  private static CredentialManager instance;

  private CredentialManager()
  {
    if (Constants.getOS() == Constants.OS.MAC)
    {
      secureStorageManager = SecureStorageAppleManager.builder();
    }
    else if (Constants.getOS() == Constants.OS.WINDOWS)
    {
      secureStorageManager = SecureStorageWindowsManager.builder();
    }
    else if (Constants.getOS() == Constants.OS.LINUX)
    {
      secureStorageManager = SecureStorageLinuxManager.builder();
    }
    else
    {
      logger.error("Unsupported Operating System. Expected: OSX, Windows, Linux");
    }
  }

  public static CredentialManager getInstance()
  {
    if (instance == null)
    {
      synchronized (CredentialManager.class)
      {
        if (instance == null)
        {
          instance = new CredentialManager();
        }
      }
    }
    return instance;
  }

  /**
   * Reuse the cached id token stored locally
   *
   * @param loginInput login input to attach id token
   */
  synchronized void fillCachedIdToken(SFLoginInput loginInput) throws SFException
  {
    String idToken =
        secureStorageManager.getCredential(extractHostFromServerUrl(loginInput.getServerUrl()), loginInput.getUserName());

    if (idToken == null)
    {
      logger.debug("retrieved idToken is null");
    }
    loginInput.setIdToken(idToken); // idToken can be null
    return;
  }

  /**
   * Store the temporary credential
   *
   * @param loginInput  loginInput to denote to the cache
   * @param loginOutput loginOutput to denote to the cache
   */
  synchronized void writeTemporaryCredential(
      SFLoginInput loginInput, SFLoginOutput loginOutput) throws SFException
  {
    String idToken = loginOutput.getIdToken();
    if (Strings.isNullOrEmpty(idToken))
    {
      logger.debug("no idToken is given.");
      return; // no idToken
    }

    secureStorageManager.setCredential(extractHostFromServerUrl(loginInput.getServerUrl()), loginInput.getUserName(), idToken);
  }

  /**
   * Delete the id token cache
   */
  void deleteIdTokenCache(String host, String user)
  {
    secureStorageManager.deleteCredential(host, user);
  }

  /**
   * Used to extract host name from a well formated internal serverUrl, e.g., serverUrl in SFLoginInput.
   */
  private String extractHostFromServerUrl(String serverUrl) throws SFException
  {
    URL url = null;
    try
    {
      url = new URL(serverUrl);
    }
    catch (MalformedURLException e)
    {
      logger.error("Invalid serverUrl for retrieving host name");
      throw new SFException(ErrorCode.INTERNAL_ERROR, "Invalid serverUrl for retrieving host name");
    }
    return url.getHost();
  }
}
