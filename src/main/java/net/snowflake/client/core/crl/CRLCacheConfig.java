package net.snowflake.client.core.crl;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import net.snowflake.client.core.FileCacheUtil;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.jdbc.SnowflakeUtil;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

@SnowflakeJdbcInternalApi
public class CRLCacheConfig {
  private static final SFLogger logger = SFLoggerFactory.getLogger(CRLCacheConfig.class);
  public static final String ENABLE_CRL_IN_MEMORY_CACHING = "ENABLE_CRL_IN_MEMORY_CACHING";
  public static final String ENABLE_CRL_DISK_CACHING = "ENABLE_CRL_DISK_CACHING";
  public static final String CRL_CACHE_VALIDITY_TIME = "CRL_CACHE_VALIDITY_TIME";
  public static final String CRL_RESPONSE_CACHE_DIR = "CRL_RESPONSE_CACHE_DIR";
  public static final String CRL_ON_DISK_CACHE_REMOVAL_DELAY = "CRL_ON_DISK_CACHE_REMOVAL_DELAY";

  public static boolean getInMemoryCacheEnabled() {
    return SnowflakeUtil.convertSystemPropertyToBooleanValue(ENABLE_CRL_IN_MEMORY_CACHING, true);
  }

  public static boolean getOnDiskCacheEnabled() {
    return SnowflakeUtil.convertSystemPropertyToBooleanValue(ENABLE_CRL_DISK_CACHING, true);
  }

  public static Duration getCacheValidityTime() {
    String validityTime = SnowflakeUtil.systemGetProperty(CRL_CACHE_VALIDITY_TIME);
    if (validityTime != null && !validityTime.isEmpty()) {
      try {
        long seconds = Long.parseLong(validityTime);
        if (seconds <= 0) {
          throw new IllegalArgumentException("Cache validity time must be positive");
        }
        return Duration.ofSeconds(seconds);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Invalid cache validity time: " + validityTime, e);
      }
    } else {
      return Duration.ofDays(1);
    }
  }

  public static Path getOnDiskCacheDir() {
    String cacheDir = SnowflakeUtil.systemGetProperty(CRL_RESPONSE_CACHE_DIR);
    if (cacheDir == null || cacheDir.isEmpty()) {
      File defaultCacheDir = FileCacheUtil.getDefaultCacheDir();
      if (defaultCacheDir != null) {
        return Paths.get(defaultCacheDir.getAbsolutePath(), "crls");
      } else {
        throw new IllegalStateException(
            "Default cache dir not set but CRL file cache is enabled. Either fix the environment so that a cache directory can be determined, or disable the CRL file cache in the configuration.");
      }
    } else {
      return Paths.get(cacheDir);
    }
  }

  public static Duration getCrlOnDiskCacheRemovalDelay() {
    String removalDelay = SnowflakeUtil.systemGetProperty(CRL_ON_DISK_CACHE_REMOVAL_DELAY);
    if (removalDelay != null && !removalDelay.isEmpty()) {
      try {
        long seconds = Long.parseLong(removalDelay);
        if (seconds <= 0) {
          throw new IllegalArgumentException("Cache removal delay time must be positive");
        }
        return Duration.ofSeconds(seconds);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Invalid cache removal delay: " + removalDelay, e);
      }
    } else {
      logger.debug("Using default on-disk cache removal delay of 7 days");
      return Duration.ofDays(7);
    }
  }
}
