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

  private final CRLCache memoryCache;
  private final CRLCache fileCache;
  private final ScheduledExecutorService cleanupScheduler;
  private final Runnable cleanupTask;
  private final long cleanupIntervalInMs;

  CRLCacheManager(CRLCache memoryCache, CRLCache fileCache, Duration cleanupInterval) {
    this.memoryCache = memoryCache;
    this.fileCache = fileCache;
    this.cleanupIntervalInMs = cleanupInterval.toMillis();

    this.cleanupTask =
        () -> {
          try {
            logger.debug(
                "Running periodic CRL cache cleanup with interval {} seconds",
                cleanupIntervalInMs / 1000.0);
            memoryCache.cleanup();
            fileCache.cleanup();
          } catch (Exception e) {
            logger.error("An error occurred during scheduled CRL cache cleanup.", e);
          }
        };
    ThreadFactory threadFactory =
        r -> {
          Thread t = new Thread(r, "crl-cache-cleanup");
          t.setDaemon(true); // Don't prevent JVM shutdown
          return t;
        };
    this.cleanupScheduler = Executors.newSingleThreadScheduledExecutor(threadFactory);
  }

  static CRLCacheManager fromConfig(CRLValidationConfig config) throws SnowflakeSQLLoggedException {
    CRLCache memoryCache;
    if (config.isInMemoryCacheEnabled()) {
      logger.debug("Enabling in-memory CRL cache");
      memoryCache = new CRLInMemoryCache(config.getCacheValidityTime());
    } else {
      logger.debug("In-memory CRL cache disabled");
      memoryCache = NoopCRLCache.INSTANCE;
    }

    CRLCache fileCache;
    if (config.isOnDiskCacheEnabled()) {
      logger.debug("Enabling file based CRL cache");
      fileCache = new CRLFileCache(config.getOnDiskCacheDir(), config.getOnDiskCacheRemovalDelay());
    } else {
      logger.debug("File based CRL cache disabled");
      fileCache = NoopCRLCache.INSTANCE;
    }

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
    fileCache.put(crlUrl, entry);
  }

  private void startPeriodicCleanup() {
    cleanupScheduler.scheduleAtFixedRate(
        cleanupTask, cleanupIntervalInMs, cleanupIntervalInMs, TimeUnit.MILLISECONDS);

    logger.debug(
        "Scheduled CRL cache cleanup task to run every {} seconds.", cleanupIntervalInMs / 1000.0);
  }
}
