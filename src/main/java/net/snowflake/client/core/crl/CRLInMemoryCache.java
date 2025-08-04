package net.snowflake.client.core.crl;

import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

class CRLInMemoryCache {

  private static final SFLogger logger = SFLoggerFactory.getLogger(CRLInMemoryCache.class);
  private static final ConcurrentHashMap<String, CRLCacheEntry> cache = new ConcurrentHashMap<>();

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

    int removedCount = 0;
    for (Iterator<Entry<String, CRLCacheEntry>> iterator = cache.entrySet().iterator();
        iterator.hasNext(); ) {
      Entry<String, CRLCacheEntry> entry = iterator.next();
      CRLCacheEntry cacheEntry = entry.getValue();

      boolean expired = cacheEntry.isCrlExpired(now);
      boolean evicted = cacheEntry.shouldEvict(now, cacheValidityTime);

      if (expired || evicted) {
        iterator.remove();
        removedCount++;
      }
    }

    if (removedCount > 0) {
      logger.debug("Removed {} expired/evicted entries from in-memory CRL cache", removedCount);
    }
  }
}
