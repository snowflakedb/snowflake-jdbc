package net.snowflake.client.internal.core;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.util.function.Supplier;
import net.snowflake.client.internal.log.SFLogger;
import net.snowflake.client.internal.log.SFLoggerFactory;

class NoOpFileCacheManager implements FileCacheManager {
  private static final SFLogger logger = SFLoggerFactory.getLogger(NoOpFileCacheManager.class);

  NoOpFileCacheManager() {
    logger.warn(
        "Cache is not available. Caching will be disabled. "
            + "Set the HOME, SF_TEMPORARY_CREDENTIAL_CACHE_DIR, "
            + "or net.snowflake.jdbc.temporaryCredentialCacheDir to enable caching.");
  }

  @Override
  public String getCacheFilePath() {
    return null;
  }

  @Override
  public void overrideCacheFile(File newCacheFile) {
    logger.debug("Cache is not enabled; ignoring override", false);
  }

  @Override
  public <T> T withLock(Supplier<T> supplier) {
    return null;
  }

  @Override
  public JsonNode readCacheFile() {
    return null;
  }

  @Override
  public void writeCacheFile(JsonNode input) {}

  @Override
  public void deleteCacheFile() {}
}
