package net.snowflake.client.core.crl;

import java.security.cert.X509CRL;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import net.snowflake.client.jdbc.SnowflakeSQLLoggedException;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

/**
 * Cache manager that coordinates between in-memory and file-based CRL caches. Provides automatic
 * cleanup of expired entries and proper lifecycle management.
 */
class CRLCacheManager {

  private static final SFLogger logger = SFLoggerFactory.getLogger(CRLCacheManager.class);

  private final CRLInMemoryCache memoryCache;
  private final CRLFileCache fileCache;
  private final ScheduledExecutorService cleanupScheduler;
  private final long cleanupIntervalInMs;

  CRLCacheManager(CRLInMemoryCache memoryCache, CRLFileCache fileCache, Duration cleanupInterval) {
    this.memoryCache = memoryCache;
    this.fileCache = fileCache;
    this.cleanupIntervalInMs = cleanupInterval.toMillis();

    ThreadFactory threadFactory =
        r -> {
          Thread t = new Thread(r, "crl-cache-cleanup");
          t.setDaemon(true); // Don't prevent JVM shutdown
          return t;
        };
    this.cleanupScheduler = Executors.newSingleThreadScheduledExecutor(threadFactory);
  }

  static CRLCacheManager fromConfig(CRLValidationConfig config) throws SnowflakeSQLLoggedException {
    CRLInMemoryCache memoryCache =
        new CRLInMemoryCache(config.getCacheValidityTime(), config.isInMemoryCacheEnabled());

    CRLFileCache fileCache =
        new CRLFileCache(
            config.getOnDiskCacheDir(),
            config.getOnDiskCacheRemovalDelay(),
            config.isOnDiskCacheEnabled());

    CRLCacheManager manager =
        new CRLCacheManager(memoryCache, fileCache, config.getOnDiskCacheRemovalDelay());
    if (config.isInMemoryCacheEnabled() || config.isOnDiskCacheEnabled()) {
      manager.startPeriodicCleanup();
    }
    return manager;
  }

  CRLCacheEntry get(String crlUrl) {
    CRLCacheEntry entry = memoryCache.get(crlUrl);
    if (entry != null) {
      return entry;
    }

    entry = fileCache.get(crlUrl);
    if (entry != null) {
      // Promote to memory cache
      memoryCache.put(crlUrl, entry);
      return entry;
    }

    logger.debug("CRL not found in cache for {}", crlUrl);
    return null;
  }

  void put(String crlUrl, X509CRL crl, Instant downloadTime) {
    CRLCacheEntry entry = new CRLCacheEntry(crl, downloadTime);
    memoryCache.put(crlUrl, entry);
    fileCache.put(crlUrl, crl, downloadTime);
  }

  private void startPeriodicCleanup() {
    Runnable cleanupTask =
        () -> {
          try {
            logger.debug(
                "Running periodic CRL cache cleanup with interval {} seconds", cleanupIntervalInMs);
            memoryCache.cleanup();
            fileCache.cleanup();
          } catch (Exception e) {
            logger.error("An error occurred during scheduled CRL cache cleanup.", e);
          }
        };

    cleanupScheduler.scheduleAtFixedRate(
        cleanupTask, cleanupIntervalInMs, cleanupIntervalInMs, TimeUnit.MILLISECONDS);

    logger.debug("Scheduled CRL cache cleanup task to run every {} seconds.", cleanupIntervalInMs);
  }
}
