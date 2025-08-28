package net.snowflake.client.core.crl;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

class CRLInMemoryCache implements CRLCache {

  private static final SFLogger logger = SFLoggerFactory.getLogger(CRLInMemoryCache.class);
  private final ConcurrentHashMap<String, CRLCacheEntry> cache = new ConcurrentHashMap<>();
  private final Duration cacheValidityTime;

  CRLInMemoryCache(Duration cacheValidityTime) {
    this.cacheValidityTime = cacheValidityTime;
  }

  public CRLCacheEntry get(String crlUrl) {
    CRLCacheEntry entry = cache.get(crlUrl);
    if (entry != null) {
      logger.debug("Found CRL in memory cache for {}", crlUrl);
    }
    return entry;
  }

  public void put(String crlUrl, CRLCacheEntry entry) {
    cache.put(crlUrl, entry);
  }

  public void cleanup() {
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
              logger.debug(
                  "Removing in-memory CRL cache entry for {}: expired={}, evicted={}",
                  entry.getKey(),
                  expired,
                  evicted);
              return expired || evicted;
            });

    int removedCount = initialSize - cache.size();
    if (removedCount > 0) {
      logger.debug("Removed {} expired/evicted entries from in-memory CRL cache", removedCount);
    }
  }
}
