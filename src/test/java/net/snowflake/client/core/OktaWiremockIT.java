package net.snowflake.client.core;

import net.snowflake.client.category.TestTags;
import net.snowflake.client.jdbc.BaseWiremockTest;
import net.snowflake.client.jdbc.RestRequest;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

@Tag(TestTags.OTHERS)
public class OktaWiremockIT extends BaseWiremockTest {

//  TODO: move to testutil
  private SFLoginInput createOktaLoginInput() {
    SFLoginInput input = new SFLoginInput();
    input.setServerUrl("https://testauth.okta.com");
    input.setUserName("MOCK_USERNAME");
    input.setPassword("MOCK_PASSWORD");
    input.setAccountName("MOCK_ACCOUNT_NAME");
    input.setAppId("MOCK_APP_ID");
    input.setOCSPMode(OCSPMode.FAIL_OPEN);
    input.setHttpClientSettingsKey(new HttpClientSettingsKey(OCSPMode.FAIL_OPEN));
    input.setLoginTimeout(1000);
    input.setSessionParameters(new HashMap<>());
    input.setAuthenticator("https://testauth.okta.com");
    return input;
  }

  private String getProxyProtocol(Properties props) {
    return props.get("ssl").toString().equals("on") ? "https" : "http";
  }

  private int getProxyPort(String proxyProtocol) {
    if (Objects.equals(proxyProtocol, "http")) {
      return wiremockHttpPort;
    } else {
      return wiremockHttpsPort;
    }
  }

  private void setJvmProperties(Properties props) {
    String proxyProtocol = getProxyProtocol(props);
    System.setProperty("http.useProxy", "true");
    System.setProperty("http.proxyProtocol", proxyProtocol);
    if (Objects.equals(proxyProtocol, "http")) {
      System.setProperty("http.proxyHost", WIREMOCK_HOST);
      System.setProperty("http.proxyPort", String.valueOf(getProxyPort(proxyProtocol)));
    } else {
      System.setProperty("https.proxyHost", WIREMOCK_HOST);
      System.setProperty("https.proxyPort", String.valueOf(getProxyPort(proxyProtocol)));
    }
  }

  private Map<SFSessionProperty, Object> initConnectionPropertiesMap() {
    Map<SFSessionProperty, Object> connectionPropertiesMap = new HashMap<>();
    connectionPropertiesMap.put(SFSessionProperty.TRACING, "ALL");
    return connectionPropertiesMap;
  }
  String connectionResetByPeerScenario =
      "{\n"
          + "    \"mappings\": [\n"
          + "        {\n"
          + "            \"scenarioName\": \"Too many okta connections\",\n"
          + "            \"requiredScenarioState\": \"Started\",\n"
          + "            \"newScenarioState\": \"Too many okta connections - 1\",\n"
          + "            \"request\": {\n"
          + "                \"method\": \"POST\",\n"
          + "                \"url\": \"/api/v1/authn/\"\n"
          + "            },\n"
          + "            \"response\": {\n"
          + "                \"status\": 429\n"
          + "            }\n"
          + "        },\n"
          + "        {\n"
          + "            \"scenarioName\": \"Too many okta connections\",\n"
          + "            \"requiredScenarioState\": \"Too many okta connections - 1\",\n"
          + "            \"newScenarioState\": \"Too many okta connections - 2\",\n"
          + "            \"newScenarioState\": \"Connection is stable\",\n"
          + "            \"request\": {\n"
          + "                \"method\": \"POST\",\n"
          + "                \"url\": \"/api/v1/authn/\"\n"
          + "            },\n"
          + "            \"response\": {\n"
          + "                \"status\": 429\n"
          + "            }\n"
          + "        },\n"
          + "        {\n"
          + "            \"scenarioName\": \"Too many okta connections\",\n"
          + "            \"requiredScenarioState\": \"Too many okta connections - 2\",\n"
          + "            \"newScenarioState\": \"Connection is stable\",\n"
          + "            \"request\": {\n"
          + "                \"method\": \"POST\",\n"
          + "                \"url\": \"/api/v1/authn/\"\n"
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
  public void testOktaRetryWaitsUsingDefaultRetryStrategy() throws Throwable {
    importMapping(connectionResetByPeerScenario);
//    setCustomTrustStorePropertyPath();
    Properties props = getProperties();
    setJvmProperties(props);
    SFLoginInput loginInput = createOktaLoginInput();
    Map<SFSessionProperty, Object> connectionPropertiesMap = initConnectionPropertiesMap();
    SessionUtil.openSession(loginInput, connectionPropertiesMap, "ALL");

//    TODO: WIREMOCK_HOST, wiremockHttpPort

//    HttpClientBuilder httpClientBuilder = HttpClientBuilder.create().disableAutomaticRetries();
//    try (CloseableHttpClient httpClient = httpClientBuilder.build()) {
//      HttpPost request =
//          new HttpPost(String.format("http://%s:%d/api/v1/authn/", WIREMOCK_HOST, wiremockHttpPort));
//      RestRequest.execute(
//          httpClient,
//          request,
//          0,
//          0,
//          0,
//          0,
//          0,
//          new AtomicBoolean(false),
//          false,
//          false,
//          false,
//          false,
//          new ExecTimeTelemetryData());

//      CloseableHttpResponse response = httpClient.execute(request);
//      assert (response.getStatusLine().getStatusCode() == 200);
    }
  }
//}

//
//@Test
//public void testOktaRetryWaitsUsingRetryAfterResponseHeader() throws Exception {
//}

//
//@Test
//public void testOktaRetryUsesNewOneTimeTokenForBasicOktaURL() throws Exception {
//}

//@Test
//public void testOktaRetryUsesNewOneTimeTokenForVanityOktaURL() throws Exception {
//}