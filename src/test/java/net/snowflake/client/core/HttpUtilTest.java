package net.snowflake.client.core;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.lang.reflect.Field;
import java.util.AbstractMap;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.Configurable;
import org.apache.http.impl.client.CloseableHttpClient;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matcher;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Test;

public class HttpUtilTest {

  /**
   * Test based on <a href="https://github.com/snowflakedb/snowflake-jdbc/issues/2047">reported
   * issue in SNOW-1898533</a>
   */
  @Test
  public void buildHttpClientRace() throws InterruptedException {
    HttpUtil.httpClient.clear();
    // start two threads but only need one to fail
    CountDownLatch latch = new CountDownLatch(1);
    final Queue<AbstractMap.SimpleEntry<Thread, Throwable>> failures =
        new ConcurrentLinkedQueue<>();
    final HttpClientSettingsKey noProxyKey = new HttpClientSettingsKey(null);
    final HttpClientSettingsKey proxyKey =
        new HttpClientSettingsKey(
            null, "some.proxy.host", 8080, null, null, null, "http", null, false);

    Thread noProxyThread =
        new Thread(() -> verifyProxyUsage(noProxyKey, failures, latch), "noProxyThread");
    noProxyThread.start();

    Thread withProxyThread =
        new Thread(() -> verifyProxyUsage(proxyKey, failures, latch), "withProxyThread");
    withProxyThread.start();

    // if latch goes to zero, then one of the threads failed
    // if await times out (returns false), then neither thread has failed (both still running)
    boolean failed = latch.await(1, TimeUnit.SECONDS);
    noProxyThread.interrupt();
    withProxyThread.interrupt();
    if (failed) {
      AbstractMap.SimpleEntry<Thread, Throwable> failure = failures.remove();
      fail(failure.getKey().getName() + " failed", failure.getValue());
    }
  }

  private static void verifyProxyUsage(
      HttpClientSettingsKey key,
      Queue<AbstractMap.SimpleEntry<Thread, Throwable>> failures,
      CountDownLatch latch) {
    while (!Thread.currentThread().isInterrupted()) {
      try (CloseableHttpClient client = HttpUtil.buildHttpClient(key, null, false)) {
        assertHttpClientUsesProxy(client, key.usesProxy());
      } catch (Throwable e) {
        failures.add(new AbstractMap.SimpleEntry<>(Thread.currentThread(), e));
        latch.countDown();
        break;
      }
    }
  }

  private static void assertHttpClientUsesProxy(CloseableHttpClient client, boolean proxyUsed) {
    assertRequestConfigWithoutProxyConfig(client);
    assertRoutePlannerOverridden(client, proxyUsed);
  }

  private static void assertRequestConfigWithoutProxyConfig(CloseableHttpClient client) {
    MatcherAssert.assertThat(client, CoreMatchers.instanceOf(Configurable.class));
    Configurable c = (Configurable) client;
    RequestConfig config = c.getConfig();
    assertNull(config.getProxy(), "request config has configured proxy");
  }

  private static void assertRoutePlannerOverridden(CloseableHttpClient client, boolean proxyUsed) {
    try {
      // HTTP client does not provide information about proxy settings so to detect that we are
      // using proxy we have to look inside via reflection and if the route planner is overridden to
      // our proxy class
      Field routePlannerField = client.getClass().getDeclaredField("routePlanner");
      routePlannerField.setAccessible(true);
      Matcher<Object> snowflakeProxyPlannerClassMatcher =
          CoreMatchers.instanceOf(SnowflakeMutableProxyRoutePlanner.class);
      MatcherAssert.assertThat(
          routePlannerField.get(client),
          proxyUsed
              ? snowflakeProxyPlannerClassMatcher
              : CoreMatchers.not(snowflakeProxyPlannerClassMatcher));
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }
}
