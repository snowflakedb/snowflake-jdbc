package net.snowflake.client.jdbc;

import java.util.concurrent.atomic.AtomicBoolean;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.core.ExecTimeTelemetryData;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.OTHERS)
public class RestRequestWiremockLatestIT extends BaseWiremockTest {

  String connectionResetByPeerScenario =
      "{\n"
          + "    \"mappings\": [\n"
          + "        {\n"
          + "            \"scenarioName\": \"Connection reset by peer\",\n"
          + "            \"requiredScenarioState\": \"Started\",\n"
          + "            \"newScenarioState\": \"Connection is stable\",\n"
          + "            \"request\": {\n"
          + "                \"method\": \"GET\",\n"
          + "                \"url\": \"/endpoint\"\n"
          + "            },\n"
          + "            \"response\": {\n"
          + "                \"fault\": \"CONNECTION_RESET_BY_PEER\"\n"
          + "            }\n"
          + "        },\n"
          + "        {\n"
          + "            \"scenarioName\": \"Connection reset by peer\",\n"
          + "            \"requiredScenarioState\": \"Connection is stable\",\n"
          + "            \"request\": {\n"
          + "                \"method\": \"GET\",\n"
          + "                \"url\": \"/endpoint\"\n"
          + "            },\n"
          + "            \"response\": {\n"
          + "                \"status\": 200\n"
          + "            }\n"
          + "        }\n"
          + "    ],\n"
          + "    \"importOptions\": {\n"
          + "        \"duplicatePolicy\": \"IGNORE\",\n"
          + "        \"deleteAllNotInImport\": true\n"
          + "    }"
          + "}";

  @Test
  public void testProxyIsUsedWhenSetInProperties() throws Exception {
    importMapping(connectionResetByPeerScenario);
    HttpClientBuilder httpClientBuilder = HttpClientBuilder.create().disableAutomaticRetries();
    try (CloseableHttpClient httpClient = httpClientBuilder.build()) {
      HttpGet request =
          new HttpGet(String.format("http://%s:%d/endpoint", WIREMOCK_HOST, wiremockHttpPort));
      RestRequest.execute(
          httpClient,
          request,
          0,
          0,
          0,
          0,
          0,
          new AtomicBoolean(false),
          false,
          false,
          false,
          false,
          new ExecTimeTelemetryData());

      CloseableHttpResponse response = httpClient.execute(request);
      assert (response.getStatusLine().getStatusCode() == 200);
    }
  }
}
