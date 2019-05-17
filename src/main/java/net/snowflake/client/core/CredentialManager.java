/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Strings;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static net.snowflake.client.core.StmtUtil.mapper;

public class CredentialManager
{
  private static final
  SFLogger logger = SFLoggerFactory.getLogger(CredentialManager.class);
  private static final String CACHE_FILE_NAME = "temporary_credential.json";
  private static final String CACHE_DIR_PROP = "net.snowflake.jdbc.temporaryCredentialCacheDir";
  private static final String CACHE_DIR_ENV = "SF_TEMPORARY_CREDENTIAL_CACHE_DIR";
  private static final long CACHE_EXPIRATION_IN_SECONDS = 86400L;
  private static final long CACHE_FILE_LOCK_EXPIRATION_IN_SECONDS = 60L;
  private FileCacheManager fileCacheManager;

  private static CredentialManager instance;

  private final Map<String, Map<String, String>> idTokenCache = new HashMap<>();

  private CredentialManager()
  {
    fileCacheManager = FileCacheManager
        .builder()
        .setCacheDirectorySystemProperty(CACHE_DIR_PROP)
        .setCacheDirectoryEnvironmentVariable(CACHE_DIR_ENV)
        .setBaseCacheFileName(CACHE_FILE_NAME)
        .setCacheExpirationInSeconds(CACHE_EXPIRATION_IN_SECONDS)
        .setCacheFileLockExpirationInSeconds(CACHE_FILE_LOCK_EXPIRATION_IN_SECONDS).build();

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
   * @return true if the id token is found and attached
   */
  synchronized boolean fillCachedIdToken(SFLoginInput loginInput)
  {
    String idToken;
    JsonNode res = fileCacheManager.readCacheFile();
    readJsonStoreCache(res);

    Map<String, String> userMap = idTokenCache.get(
        loginInput.getAccountName().toUpperCase());

    if (userMap == null)
    {
      return false;
    }
    idToken = userMap.get(loginInput.getUserName().toUpperCase());

    if (idToken == null)
    {
      return false;
    }
    loginInput.setIdToken(idToken);
    return true;
  }

  /**
   * Store the temporary credential
   *
   * @param loginInput  loginInput to denote to the cache
   * @param loginOutput loginOutput to denote to the cache
   */
  synchronized void writeTemporaryCredential(
      SFLoginInput loginInput, SFLoginOutput loginOutput)
  {
    if (Strings.isNullOrEmpty(loginOutput.getIdToken()))
    {
      return; // no idToken
    }
    String currentAccount = loginInput.getAccountName().toUpperCase();

    idTokenCache.computeIfAbsent(currentAccount, newMap -> new HashMap<>());

    Map<String, String> currentUserMap = idTokenCache.get(currentAccount);
    currentUserMap.put(loginInput.getUserName().toUpperCase(), loginOutput.getIdToken());

    ObjectNode out = mapper.createObjectNode();
    for (Map.Entry<String, Map<String, String>> elem : idTokenCache.entrySet())
    {
      String account = elem.getKey();
      Map<String, String> userMap = elem.getValue();
      ObjectNode userNode = mapper.createObjectNode();
      for (Map.Entry<String, String> elem0 : userMap.entrySet())
      {
        userNode.put(elem0.getKey(), elem0.getValue());
      }
      out.set(account, userNode);
    }
    fileCacheManager.writeCacheFile(out);
  }

  /**
   * Delete the id token cache
   */
  void deleteIdTokenCache()
  {
    fileCacheManager.deleteCacheFile();
  }

  private void readJsonStoreCache(JsonNode m)
  {
    if (m == null || !m.getNodeType().equals(JsonNodeType.OBJECT))
    {
      logger.debug("Invalid cache file format.");
      return;
    }
    for (Iterator<Map.Entry<String, JsonNode>> itr = m.fields(); itr.hasNext(); )
    {
      Map.Entry<String, JsonNode> accountMap = itr.next();
      String account = accountMap.getKey();
      if (!idTokenCache.containsKey(account))
      {
        idTokenCache.put(account, new HashMap<>());
      }
      JsonNode userJsonNode = accountMap.getValue();
      for (Iterator<Map.Entry<String, JsonNode>> itr0 = userJsonNode.fields(); itr0.hasNext(); )
      {
        Map.Entry<String, JsonNode> userMap = itr0.next();
        idTokenCache.get(account).put(
            userMap.getKey(), userMap.getValue().asText());
      }
    }
  }

}
