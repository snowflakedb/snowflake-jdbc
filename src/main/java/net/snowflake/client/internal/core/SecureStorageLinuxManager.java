package net.snowflake.client.internal.core;

import static net.snowflake.client.internal.config.SFConnectionConfigParser.SKIP_TOKEN_FILE_PERMISSIONS_VERIFICATION;
import static net.snowflake.client.internal.core.StmtUtil.mapper;
import static net.snowflake.client.internal.jdbc.SnowflakeUtil.convertSystemGetEnvToBooleanValue;
import static net.snowflake.client.internal.jdbc.SnowflakeUtil.isNullOrEmpty;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import net.snowflake.client.internal.log.SFLogger;
import net.snowflake.client.internal.log.SFLoggerFactory;

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
    boolean shouldSkipTokenFilePermissionsVerification =
        convertSystemGetEnvToBooleanValue(SKIP_TOKEN_FILE_PERMISSIONS_VERIFICATION, false);
    if (shouldSkipTokenFilePermissionsVerification) {
      logger.debug(
          "Skip credential cache file permissions verification because {} is enabled",
          SKIP_TOKEN_FILE_PERMISSIONS_VERIFICATION);
    }
    fileCacheManager =
        new FileCacheManagerBuilder()
            .setCacheDirectorySystemProperty(CACHE_DIR_PROP)
            .setCacheDirectoryEnvironmentVariable(CACHE_DIR_ENV)
            .setBaseCacheFileName(CACHE_FILE_NAME)
            .setOnlyOwnerPermissions(!shouldSkipTokenFilePermissionsVerification)
            .setCacheFileLockExpirationInSeconds(CACHE_FILE_LOCK_EXPIRATION_IN_SECONDS)
            .build();
  }

  private static class SecureStorageLinuxManagerHolder {
    private static final SecureStorageLinuxManager INSTANCE = new SecureStorageLinuxManager();
  }

  public static SecureStorageLinuxManager getInstance() {
    return SecureStorageLinuxManagerHolder.INSTANCE;
  }

  @Override
  public synchronized SecureStorageStatus setCredential(String cacheKey, String token) {
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
          credentialsMap.put(cacheKey, token);
          fileCacheManager.writeCacheFile(
              SecureStorageLinuxManager.this.localCacheToJson(cachedCredentials));
          return null;
        });
    return SecureStorageStatus.SUCCESS;
  }

  @Override
  public synchronized String getCredential(String cacheKey) {
    return fileCacheManager.withLock(
        () -> {
          JsonNode res = fileCacheManager.readCacheFile();
          Map<String, Map<String, String>> cache = readJsonStoreCache(res);
          Map<String, String> credentialsMap = cache.get(CACHE_FILE_TOKENS_OBJECT_NAME);
          if (credentialsMap == null) {
            return null;
          }
          return credentialsMap.get(cacheKey);
        });
  }

  @Override
  public synchronized SecureStorageStatus deleteCredential(String cacheKey) {
    fileCacheManager.withLock(
        () -> {
          JsonNode res = fileCacheManager.readCacheFile();
          Map<String, Map<String, String>> cache = readJsonStoreCache(res);
          Map<String, String> credentialsMap = cache.get(CACHE_FILE_TOKENS_OBJECT_NAME);
          if (credentialsMap != null) {
            credentialsMap.remove(cacheKey);
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
