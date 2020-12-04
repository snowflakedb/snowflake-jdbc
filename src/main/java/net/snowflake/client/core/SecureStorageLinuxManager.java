/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import static net.snowflake.client.core.StmtUtil.mapper;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Strings;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

/**
 * Linux currently doesn't have a local secure storage like Keychain/Credential Manager in
 * Mac/Windows. This class just wraps the local file cache logic to keep Linux platform api
 * consistent Mac/Windows platform.
 */
public class SecureStorageLinuxManager implements SecureStorageManager {
  private static final SFLogger logger = SFLoggerFactory.getLogger(SecureStorageLinuxManager.class);
  private static final String CACHE_FILE_NAME = "temporary_credential.json";
  private static final String CACHE_DIR_PROP = "net.snowflake.jdbc.temporaryCredentialCacheDir";
  private static final String CACHE_DIR_ENV = "SF_TEMPORARY_CREDENTIAL_CACHE_DIR";
  private static final long CACHE_EXPIRATION_IN_SECONDS = 86400L;
  private static final long CACHE_FILE_LOCK_EXPIRATION_IN_SECONDS = 60L;
  private FileCacheManager fileCacheManager;

  private final Map<String, Map<String, String>> localCredCache = new HashMap<>();

  private SecureStorageLinuxManager() {
    fileCacheManager =
        FileCacheManager.builder()
            .setCacheDirectorySystemProperty(CACHE_DIR_PROP)
            .setCacheDirectoryEnvironmentVariable(CACHE_DIR_ENV)
            .setBaseCacheFileName(CACHE_FILE_NAME)
            .setCacheExpirationInSeconds(CACHE_EXPIRATION_IN_SECONDS)
            .setCacheFileLockExpirationInSeconds(CACHE_FILE_LOCK_EXPIRATION_IN_SECONDS)
            .build();
  }

  private static class SecureStorageLinuxManagerHolder {
    private static final SecureStorageLinuxManager INSTANCE = new SecureStorageLinuxManager();
  }

  public static SecureStorageLinuxManager getInstance() {
    return SecureStorageLinuxManagerHolder.INSTANCE;
  }

  private ObjectNode localCacheToJson() {
    ObjectNode res = mapper.createObjectNode();
    for (Map.Entry<String, Map<String, String>> elem : localCredCache.entrySet()) {
      String elemHost = elem.getKey();
      Map<String, String> hostMap = elem.getValue();
      ObjectNode hostNode = mapper.createObjectNode();
      for (Map.Entry<String, String> elem0 : hostMap.entrySet()) {
        hostNode.put(elem0.getKey(), elem0.getValue());
      }
      res.set(elemHost, hostNode);
    }
    return res;
  }

  public synchronized SecureStorageStatus setCredential(
      String host, String user, String type, String token) {
    if (Strings.isNullOrEmpty(token)) {
      logger.info("No token provided");
      return SecureStorageStatus.SUCCESS;
    }

    localCredCache.computeIfAbsent(host.toUpperCase(), newMap -> new HashMap<>());

    Map<String, String> hostMap = localCredCache.get(host.toUpperCase());
    hostMap.put(SecureStorageManager.convertTarget(host, user, type), token);

    fileCacheManager.writeCacheFile(localCacheToJson());
    return SecureStorageStatus.SUCCESS;
  }

  public synchronized String getCredential(String host, String user, String type) {
    JsonNode res = fileCacheManager.readCacheFile();
    readJsonStoreCache(res);

    Map<String, String> hostMap = localCredCache.get(host.toUpperCase());

    if (hostMap == null) {
      return null;
    }

    return hostMap.get(SecureStorageManager.convertTarget(host, user, type));
  }

  /** May delete credentials which doesn't belong to this process */
  public synchronized SecureStorageStatus deleteCredential(String host, String user, String type) {
    Map<String, String> hostMap = localCredCache.get(host.toUpperCase());
    if (hostMap != null) {
      hostMap.remove(SecureStorageManager.convertTarget(host, user, type));
      if (hostMap.isEmpty()) {
        localCredCache.remove(host.toUpperCase());
      }
    }
    fileCacheManager.writeCacheFile(localCacheToJson());
    return SecureStorageStatus.SUCCESS;
  }

  private void readJsonStoreCache(JsonNode m) {
    if (m == null || !m.getNodeType().equals(JsonNodeType.OBJECT)) {
      logger.debug("Invalid cache file format.");
      return;
    }
    for (Iterator<Map.Entry<String, JsonNode>> itr = m.fields(); itr.hasNext(); ) {
      Map.Entry<String, JsonNode> hostMap = itr.next();
      String host = hostMap.getKey();
      if (!localCredCache.containsKey(host)) {
        localCredCache.put(host, new HashMap<>());
      }
      JsonNode userJsonNode = hostMap.getValue();
      for (Iterator<Map.Entry<String, JsonNode>> itr0 = userJsonNode.fields(); itr0.hasNext(); ) {
        Map.Entry<String, JsonNode> userMap = itr0.next();
        localCredCache.get(host).put(userMap.getKey(), userMap.getValue().asText());
      }
    }
  }
}
