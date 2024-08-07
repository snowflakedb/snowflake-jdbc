/*
 * Copyright (c) 2024 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.time.Duration;
import net.snowflake.client.category.TestCategoryCore;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(TestCategoryCore.class)
public class HttpUtilLatestIT {

  private static final String HANG_WEBSERVER_ADDRESS = "http://localhost:12345/hang";

  /** Added in > 3.14.5 */
  @Test(timeout = 1000L)
  public void shouldOverrideConnectionAndSocketTimeouts() {
    // it's hard to test connection timeout so there is only a test for socket timeout
    HttpUtil.setSocketTimeout(200);
    HttpUtil.setConnectionTimeout(100);

    CloseableHttpClient httpClient =
        HttpUtil.getHttpClient(new HttpClientSettingsKey(OCSPMode.INSECURE));
    try {
      httpClient.execute(new HttpGet(HANG_WEBSERVER_ADDRESS));
      fail("Request should fail with exception");
    } catch (IOException e) {
      MatcherAssert.assertThat(e, CoreMatchers.instanceOf(SocketTimeoutException.class));
    }
  }

  /** Added in > 3.14.5 */
  @Test
  public void shouldGetDefaultConnectionAndSocketTimeouts() {
    assertEquals(Duration.ofMillis(60_000), HttpUtil.getConnectionTimeout());
    assertEquals(Duration.ofMillis(300_000), HttpUtil.getSocketTimeout());
  }
}
