package net.snowflake.client.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.jdbc.BaseWiremockTest;
import net.snowflake.client.jdbc.HttpHeadersCustomizer;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Tag(TestTags.CORE)
public class HttpUtilLatestIT extends BaseWiremockTest {

  private static final String HANG_WEBSERVER_ADDRESS = "http://localhost:12345/hang";
  private static final String SUCCESS_AT_THIRD_RETRY_ECHOING_HEADERS_SENT =
      "{\n"
          + "  \"mappings\": [\n"
          + "    {\n"
          + "      \"scenarioName\": \"Retry Scenario Example\",\n"
          + "      \"requiredScenarioState\": \"Started\",\n"
          + "      \"newScenarioState\": \"First Attempt Failed\",\n"
          + "      \"request\": {\n"
          + "        \"method\": \"GET\",\n"
          + "        \"urlPath\": \"/echo-headers\"\n"
          + "      },\n"
          + "      \"response\": {\n"
          + "        \"fault\": \"EMPTY_RESPONSE\"\n"
          + "      }\n"
          + "    },\n"
          + "    {\n"
          + "      \"scenarioName\": \"Retry Scenario Example\",\n"
          + "      \"requiredScenarioState\": \"First Attempt Failed\",\n"
          + "      \"newScenarioState\": \"Second Attempt Failed\",\n"
          + "      \"request\": {\n"
          + "        \"method\": \"GET\",\n"
          + "        \"urlPath\": \"/echo-headers\"\n"
          + "      },\n"
          + "      \"response\": {\n"
          + "        \"fault\": \"EMPTY_RESPONSE\"\n"
          + "      }\n"
          + "    },\n"
          + "    {\n"
          + "      \"scenarioName\": \"Retry Scenario Example\",\n"
          + "      \"requiredScenarioState\": \"Second Attempt Failed\",\n"
          + "      \"request\": {\n"
          + "        \"method\": \"GET\",\n"
          + "        \"urlPath\": \"/echo-headers\"\n"
          + "      },\n"
          + "      \"response\": {\n"
          + "        \"status\": 200,\n"
          + "        \"headers\": {\n"
          + "          \"Content-Type\": \"application/json\"\n"
          + "        },\n"
          + "        \"body\": \"{{request.headers}}\",\n"
          + "        \"transformers\": [\n"
          + "          \"response-template\"\n"
          + "        ]\n"
          + "      }\n"
          + "    }\n"
          + "  ]\n"
          + "}\n";

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
        HttpUtil.getHttpClient(new HttpClientSettingsKey(OCSPMode.INSECURE), null);
    IOException e =
        assertThrows(
            IOException.class,
            () -> {
              httpClient.execute(new HttpGet(HANG_WEBSERVER_ADDRESS));
            });
    MatcherAssert.assertThat(e, CoreMatchers.instanceOf(SocketTimeoutException.class));
  }

  @Test
  public void testAddHttpInterceptorsIfPresent() throws IOException {
    importMapping(SUCCESS_AT_THIRD_RETRY_ECHOING_HEADERS_SENT);
    AtomicInteger invocations = new AtomicInteger();

    HttpHeadersCustomizer customizer =
        new HttpHeadersCustomizer() {
          @Override
          public boolean applies(
              String method, String uri, Map<String, List<String>> currentHeaders) {
            return true;
          }

          @Override
          public Map<String, List<String>> newHeaders() {
            invocations.incrementAndGet();
            Map<String, List<String>> stringListMap = new java.util.HashMap<>();
            stringListMap.put("test-header", Collections.singletonList("test-header-value"));
            return stringListMap;
          }

          @Override
          public boolean invokeOnce() {
            return false;
          }
        };

    try (CloseableHttpClient httpClient =
        HttpUtil.buildHttpClient(null, null, false, Collections.singletonList(customizer))) {
      CloseableHttpResponse response =
          httpClient.execute(
              new HttpGet(
                  String.format("http://%s:%d/echo-headers", WIREMOCK_HOST, wiremockHttpPort)));
      String content = EntityUtils.toString(response.getEntity());

      assertHttpHeadersAdded(content);
      assertCustomizerInvokedForEachRetry(3, invocations.get());
    }
  }

  private void assertHttpHeadersAdded(String responseBody) {
    assertTrue(responseBody.contains("test-header-value"));
  }

  private void assertCustomizerInvokedForEachRetry(int expectedInvocations, int actualInvocations) {
    assertEquals(expectedInvocations, actualInvocations);
  }
}
