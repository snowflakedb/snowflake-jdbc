package net.snowflake.client.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.CORE)
public class HttpUtilWiremockLatestIT extends BaseWiremockTest {
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

  @AfterEach
  public void resetHttpClients() {
    HttpUtil.httpClient.clear();
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
