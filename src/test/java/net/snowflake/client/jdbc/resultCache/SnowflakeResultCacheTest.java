package net.snowflake.client.jdbc.resultCache;

import static org.hamcrest.MatcherAssert.assertThat;

import com.github.benmanes.caffeine.cache.Cache;
import org.junit.Test;

public class SnowflakeResultCacheTest {

  private SnowflakeResultCache resultCache = null;
  private Cache<String, ?> cache = null;

  private void initCache() {
    resultCache = new SnowflakeResultCache();
    cache = resultCache.getCache();
  }

  private void initCacheData() {
    initCache();

    // Put dummy data in the cache
    for (int i = 1; i <= 10; i++) {
      resultCache.putResult(
          "SELECT " + i, "my-qyery-id-" + i, "my-session-id-" + i, new String("" + i));
    }
  }

  private void assertCacheData() {
    assertThat("Non empty cache", cache.estimatedSize() == 10);
    // Put dummy data in the cache
    for (int i = 1; i <= 10; i++) {
      String resultObj = (String) resultCache.getResult("SELECT " + i);
      resultObj.equals(new String("" + i));
    }
  }

  /** Test for empty cache */
  @Test
  public void testIsEmpty() throws Exception {
    initCache();
    assertThat("Empty cache", cache.estimatedSize() == 0);
  }

  @Test
  public void testWithSomeData() throws Exception {
    initCacheData();
    // Compare elements
    assertCacheData();
  }
}
