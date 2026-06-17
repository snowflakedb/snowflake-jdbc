package net.snowflake.client.internal.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.concurrent.atomic.AtomicBoolean;
import net.snowflake.client.api.exception.SnowflakeSQLException;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.internal.jdbc.telemetry.ExecTimeTelemetryData;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.pool.PoolStats;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Verifies that {@link RestRequest#executeWithRetries} releases the underlying HTTP connection
 * back to the pool on the failure-side exhaustion break, while NOT releasing it on a successful
 * 200 response (where the caller still owns the response stream).
 *
 * <p>See PR https://github.com/snowflakedb/snowflake-jdbc/pull/2643.
 */
@Tag(TestTags.OTHERS)
public class RestRequestConnectionReleaseWiremockLatestIT extends BaseWiremockTest {

  private static final String ALWAYS_503_SCENARIO =
      "{\n"
          + "    \"mappings\": [\n"
          + "        {\n"
          + "            \"request\": {\n"
          + "                \"method\": \"GET\",\n"
          + "                \"url\": \"/endpoint\"\n"
          + "            },\n"
          + "            \"response\": {\n"
          + "                \"status\": 503,\n"
          + "                \"body\": \"Service Unavailable\"\n"
          + "            }\n"
          + "        }\n"
          + "    ],\n"
          + "    \"importOptions\": {\n"
          + "        \"duplicatePolicy\": \"IGNORE\",\n"
          + "        \"deleteAllNotInImport\": true\n"
          + "    }\n"
          + "}";

  @Test
  public void releasesConnectionWhenExhaustionEndsInNon200Response() throws Exception {
    importMapping(ALWAYS_503_SCENARIO);

    PoolingHttpClientConnectionManager connMgr = new PoolingHttpClientConnectionManager();
    connMgr.setMaxTotal(2);
    connMgr.setDefaultMaxPerRoute(2);

    try (CloseableHttpClient httpClient =
        HttpClientBuilder.create()
            .setConnectionManager(connMgr)
            .disableAutomaticRetries()
            .build()) {
      HttpGet request =
          new HttpGet(String.format("http://%s:%d/endpoint", WIREMOCK_HOST, wiremockHttpPort));

      try {
        RestRequest.executeWithRetries(
            httpClient,
            request,
            0,
            0,
            0,
            1,
            0,
            new AtomicBoolean(false),
            false,
            false,
            false,
            false,
            false,
            new ExecTimeTelemetryData(),
            null,
            null,
            null,
            false);
        fail("Expected SnowflakeSQLException from non-200 exhaustion");
      } catch (SnowflakeSQLException expected) {
        // expected — retry budget exhausted, executeWithRetries surfaces NETWORK_ERROR
      }

      PoolStats stats = connMgr.getTotalStats();
      assertEquals(
          0,
          stats.getLeased(),
          "Connection must be released back to the pool after non-200 exhaustion. "
              + "PoolStats=" + stats);
    } finally {
      connMgr.close();
    }
  }

  @Test
  public void releasesConnectionWhenExhaustionEndsInNullResponse() throws Exception {
    // implemented in Task 3
  }

  @Test
  public void doesNotReleaseConnectionImmediatelyOnSuccessfulResponse() throws Exception {
    // implemented in Task 4
  }
}
