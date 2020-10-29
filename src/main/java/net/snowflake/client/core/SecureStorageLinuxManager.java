/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All rights reserved.
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

/**
 * Linux currently doesn't have a local secure storage like Keychain/Credential Manager in
 * Mac/Windows. This class just wraps the local file cache logic to keep Linux platform api consistent
 * Mac/Windows platform.
 */
public class SecureStorageLinuxManager implements SecureStorageManager
{
  private static final
  SFLogger logger = SFLoggerFactory.getLogger(SecureStorageLinuxManager.class);
  private static final String CACHE_FILE_NAME = "temporary_credential.json";
  private static final String CACHE_DIR_PROP = "net.snowflake.jdbc.temporaryCredentialCacheDir";
  private static final String CACHE_DIR_ENV = "SF_TEMPORARY_CREDENTIAL_CACHE_DIR";
  private static final long CACHE_EXPIRATION_IN_SECONDS = 86400L;
  private static final long CACHE_FILE_LOCK_EXPIRATION_IN_SECONDS = 60L;
  private FileCacheManager fileCacheManager;

  private final Map<String, Map<String, String>> idTokenCache = new HashMap<>();

  private SecureStorageLinuxManager()
  {
    fileCacheManager = FileCacheManager
        .builder()
        .setCacheDirectorySystemProperty(CACHE_DIR_PROP)
        .setCacheDirectoryEnvironmentVariable(CACHE_DIR_ENV)
        .setBaseCacheFileName(CACHE_FILE_NAME)
        .setCacheExpirationInSeconds(CACHE_EXPIRATION_IN_SECONDS)
        .setCacheFileLockExpirationInSeconds(CACHE_FILE_LOCK_EXPIRATION_IN_SECONDS).build();
  }

  public static SecureStorageLinuxManager builder()
  {
    return new SecureStorageLinuxManager();
  }

  public SecureStorageStatus setCredential(String host, String user, String token)
  {
    if (Strings.isNullOrEmpty(token))
    {
      logger.info("No token provided");
      return SecureStorageStatus.SUCCESS;
    }

    idTokenCache.computeIfAbsent(host.toUpperCase(), newMap -> new HashMap<>());

    Map<String, String> currentUserMap = idTokenCache.get(host.toUpperCase());
    currentUserMap.put(user.toUpperCase(), token);

    ObjectNode out = mapper.createObjectNode();
    for (Map.Entry<String, Map<String, String>> elem : idTokenCache.entrySet())
    {
      String elemHost = elem.getKey();
      Map<String, String> userMap = elem.getValue();
      ObjectNode userNode = mapper.createObjectNode();
      for (Map.Entry<String, String> elem0 : userMap.entrySet())
      {
        userNode.put(elem0.getKey(), elem0.getValue());
      }
      out.set(elemHost, userNode);
    }
    fileCacheManager.writeCacheFile(out);

    return SecureStorageStatus.SUCCESS;
  }

  public String getCredential(String host, String user)
  {
    JsonNode res = fileCacheManager.readCacheFile();
    readJsonStoreCache(res);

    Map<String, String> userMap = idTokenCache.get(host.toUpperCase());

    if (userMap == null)
    {
      return null;
    }

    return userMap.get(user.toUpperCase());
  }

  /**
   * Since Linux doesn't have secure local storage right now, this function's input
   * parameters are only here to be consistent with Mac/Win platform. We don't really
   * use these parameters. If in the future, deletion for a specific credential is
   * needed, we can change this function to satisfy it."
   */
  public SecureStorageStatus deleteCredential(String host, String user)
  {
    fileCacheManager.deleteCacheFile();
    idTokenCache.clear();
    return SecureStorageStatus.SUCCESS;
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
      Map.Entry<String, JsonNode> hostMap = itr.next();
      String host = hostMap.getKey();
      if (!idTokenCache.containsKey(host))
      {
        idTokenCache.put(host, new HashMap<>());
      }
      JsonNode userJsonNode = hostMap.getValue();
      for (Iterator<Map.Entry<String, JsonNode>> itr0 = userJsonNode.fields(); itr0.hasNext(); )
      {
        Map.Entry<String, JsonNode> userMap = itr0.next();
        idTokenCache.get(host).put(
            userMap.getKey(), userMap.getValue().asText());
      }
    }
  }
}
