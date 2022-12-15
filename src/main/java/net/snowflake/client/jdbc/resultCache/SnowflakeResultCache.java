/*
 * Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc.resultCache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.concurrent.TimeUnit;

/** Client side result cache */
public class SnowflakeResultCache implements IResultCache {

  private final Cache<String, ResultCacheElement> cache;
  private static final int EXPIRATION_TIME_IN_MINUTES = 60;
  private static final int CAPACITY = 256;

  public SnowflakeResultCache() {
    cache =
        Caffeine.newBuilder()
            .expireAfterAccess(EXPIRATION_TIME_IN_MINUTES, TimeUnit.MINUTES)
            .maximumSize(CAPACITY)
            .build();
  }

  @Override
  public void putResult(String queryText, String queryId, String sessionId, Object result) {
    cache.put(queryText, new ResultCacheElement(queryId, sessionId, result));
  }

  @Override
  public Object getResult(String queryText) {
    ResultCacheElement elem = cache.getIfPresent(queryText);
    return (elem != null) ? elem.result : null;
  }

  @Override
  public void clean(String queryText) {
    if (queryText != null) {
      cache.invalidate(queryText);
    } else {
      cache.invalidateAll();
    }

    cache.cleanUp();
  }

  private static class ResultCacheElement {
    private final String queryId;
    private final String sessionId;
    private final Object result;

    public ResultCacheElement(String queryId, String sessionId, Object result) {
      this.queryId = queryId;
      this.sessionId = sessionId;
      this.result = result;
    }
  }
}
