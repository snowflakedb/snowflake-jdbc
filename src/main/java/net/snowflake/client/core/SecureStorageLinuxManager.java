package net.snowflake.client.core;

import static net.snowflake.client.core.StmtUtil.mapper;
import static net.snowflake.client.jdbc.SnowflakeUtil.isNullOrEmpty;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
  private static final String CACHE_FILE_NAME = "credential_cache_v1.json";
  private static final String CACHE_DIR_PROP = "net.snowflake.jdbc.temporaryCredentialCacheDir";
  private static final String CACHE_DIR_ENV = "SF_TEMPORARY_CREDENTIAL_CACHE_DIR";
  private static final String CACHE_FILE_TOKENS_OBJECT_NAME = "tokens";
  private static final long CACHE_FILE_LOCK_EXPIRATION_IN_SECONDS = 60L;
  private final FileCacheManager fileCacheManager;

  private SecureStorageLinuxManager() {
    fileCacheManager =
        FileCacheManager.builder()
            .setCacheDirectorySystemProperty(CACHE_DIR_PROP)
            .setCacheDirectoryEnvironmentVariable(CACHE_DIR_ENV)
            .setBaseCacheFileName(CACHE_FILE_NAME)
            .setCacheFileLockExpirationInSeconds(CACHE_FILE_LOCK_EXPIRATION_IN_SECONDS)
            .build();
    logger.debug(
        "Using temporary file: {} as a token cache storage", fileCacheManager.getCacheFilePath());
  }

  private static class SecureStorageLinuxManagerHolder {
    private static final SecureStorageLinuxManager INSTANCE = new SecureStorageLinuxManager();
  }

  public static SecureStorageLinuxManager getInstance() {
    return SecureStorageLinuxManagerHolder.INSTANCE;
  }

  @Override
  public synchronized SecureStorageStatus setCredential(
      String host, String user, String type, String token) {
    if (isNullOrEmpty(token)) {
      logger.warn("No token provided", false);
      return SecureStorageStatus.SUCCESS;
    }
    fileCacheManager.withLock(
        () -> {
          Map<String, Map<String, String>> cachedCredentials =
              readJsonStoreCache(fileCacheManager.readCacheFile());
          cachedCredentials.computeIfAbsent(
              CACHE_FILE_TOKENS_OBJECT_NAME, tokensMap -> new HashMap<>());
          Map<String, String> credentialsMap = cachedCredentials.get(CACHE_FILE_TOKENS_OBJECT_NAME);
          credentialsMap.put(SecureStorageManager.buildCredentialsKey(host, user, type), token);
          fileCacheManager.writeCacheFile(
              SecureStorageLinuxManager.this.localCacheToJson(cachedCredentials));
          return null;
        });
    return SecureStorageStatus.SUCCESS;
  }

  @Override
  public synchronized String getCredential(String host, String user, String type) {
    return fileCacheManager.withLock(
        () -> {
          JsonNode res = fileCacheManager.readCacheFile();
          Map<String, Map<String, String>> cache = readJsonStoreCache(res);
          Map<String, String> credentialsMap = cache.get(CACHE_FILE_TOKENS_OBJECT_NAME);
          if (credentialsMap == null) {
            return null;
          }
          return credentialsMap.get(SecureStorageManager.buildCredentialsKey(host, user, type));
        });
  }

  @Override
  public synchronized SecureStorageStatus deleteCredential(String host, String user, String type) {
    fileCacheManager.withLock(
        () -> {
          JsonNode res = fileCacheManager.readCacheFile();
          Map<String, Map<String, String>> cache = readJsonStoreCache(res);
          Map<String, String> credentialsMap = cache.get(CACHE_FILE_TOKENS_OBJECT_NAME);
          if (credentialsMap != null) {
            credentialsMap.remove(SecureStorageManager.buildCredentialsKey(host, user, type));
            if (credentialsMap.isEmpty()) {
              cache.remove(CACHE_FILE_TOKENS_OBJECT_NAME);
            }
          }
          fileCacheManager.writeCacheFile(localCacheToJson(cache));
          return null;
        });
    return SecureStorageStatus.SUCCESS;
  }

  private ObjectNode localCacheToJson(Map<String, Map<String, String>> cache) {
    ObjectNode jsonNode = mapper.createObjectNode();
    Map<String, String> tokensMap = cache.get(CACHE_FILE_TOKENS_OBJECT_NAME);
    if (tokensMap != null) {
      ObjectNode tokensNode = mapper.createObjectNode();
      for (Map.Entry<String, String> credential : tokensMap.entrySet()) {
        tokensNode.put(credential.getKey(), credential.getValue());
      }
      jsonNode.set(CACHE_FILE_TOKENS_OBJECT_NAME, tokensNode);
    }
    return jsonNode;
  }

  private Map<String, Map<String, String>> readJsonStoreCache(JsonNode node) {
    Map<String, Map<String, String>> cache = new HashMap<>();
    if (node == null || !node.getNodeType().equals(JsonNodeType.OBJECT)) {
      logger.debug("Invalid cache file format.");
      return cache;
    }
    cache.put(CACHE_FILE_TOKENS_OBJECT_NAME, new HashMap<>());
    JsonNode credentialsNode = node.get(CACHE_FILE_TOKENS_OBJECT_NAME);
    Map<String, String> credentialsCache = cache.get(CACHE_FILE_TOKENS_OBJECT_NAME);
    if (credentialsNode != null && node.getNodeType().equals(JsonNodeType.OBJECT)) {
      for (Iterator<Map.Entry<String, JsonNode>> itr = credentialsNode.fields(); itr.hasNext(); ) {
        Map.Entry<String, JsonNode> credential = itr.next();
        credentialsCache.put(credential.getKey(), credential.getValue().asText());
      }
    }
    return cache;
  }
}
