package net.snowflake.client.core.crl;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

class CRLInMemoryCache {

  private static final SFLogger logger = SFLoggerFactory.getLogger(CRLInMemoryCache.class);
  private final ConcurrentHashMap<String, CRLCacheEntry> cache = new ConcurrentHashMap<>();
  private final Duration cacheValidityTime;
  private final boolean enabled;

  CRLInMemoryCache(Duration cacheValidityTime, boolean enabled) {
    this.cacheValidityTime = cacheValidityTime;
    this.enabled = enabled;
  }

  CRLCacheEntry get(String crlUrl) {
    if (!enabled) {
      return null;
    }

    CRLCacheEntry entry = cache.get(crlUrl);
    if (entry != null) {
      logger.debug("Found CRL in memory cache for {}", crlUrl);
    }
    return entry;
  }

  void put(String crlUrl, CRLCacheEntry entry) {
    if (!enabled) {
      return;
    }
    cache.put(crlUrl, entry);
  }

  void cleanup() {
    if (!enabled) {
      return;
    }

    Instant now = Instant.now();
    logger.debug("Cleaning up in-memory CRL cache at {}", now);

    int initialSize = cache.size();
    cache
        .entrySet()
        .removeIf(
            entry -> {
              CRLCacheEntry cacheEntry = entry.getValue();
              boolean expired = cacheEntry.isCrlExpired(now);
              boolean evicted = cacheEntry.isEvicted(now, cacheValidityTime);
              return expired || evicted;
            });

    int removedCount = initialSize - cache.size();
    if (removedCount > 0) {
      logger.debug("Removed {} expired/evicted entries from in-memory CRL cache", removedCount);
    }
  }
}
