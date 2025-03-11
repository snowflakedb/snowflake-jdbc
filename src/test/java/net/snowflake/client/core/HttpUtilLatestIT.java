package net.snowflake.client.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.time.Duration;
import net.snowflake.client.category.TestTags;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Tag(TestTags.CORE)
public class HttpUtilLatestIT {

  private static final String HANG_WEBSERVER_ADDRESS = "http://localhost:12345/hang";

  @BeforeEach
  public void resetHttpClientsCache() {
    HttpUtil.httpClient.clear();
  }

  @AfterEach
  public void resetHttpTimeouts() {
    HttpUtil.setConnectionTimeout(60000);
    HttpUtil.setSocketTimeout(300000);
  }

  /** Added in > 3.14.5 */
  @Test
  public void shouldGetDefaultConnectionAndSocketTimeouts() {
    assertEquals(Duration.ofMillis(60_000), HttpUtil.getConnectionTimeout());
    assertEquals(Duration.ofMillis(300_000), HttpUtil.getSocketTimeout());
  }

  /** Added in > 3.14.5 */
  @Test
  @Timeout(1)
  public void shouldOverrideConnectionAndSocketTimeouts() {
    // it's hard to test connection timeout so there is only a test for socket timeout
    HttpUtil.setConnectionTimeout(100);
    HttpUtil.setSocketTimeout(200);

    CloseableHttpClient httpClient =
        HttpUtil.getHttpClient(new HttpClientSettingsKey(OCSPMode.INSECURE));
    IOException e =
        assertThrows(
            IOException.class,
            () -> {
              httpClient.execute(new HttpGet(HANG_WEBSERVER_ADDRESS));
            });
    MatcherAssert.assertThat(e, CoreMatchers.instanceOf(SocketTimeoutException.class));
  }
}
