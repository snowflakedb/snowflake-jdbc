package net.snowflake.client.core;

import net.snowflake.client.category.TestTags;
import net.snowflake.client.jdbc.BaseWiremockTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

@Tag(TestTags.OTHERS)
public class OktaWiremockIT extends BaseWiremockTest {
  private final String WIREMOCK_HOST_WITH_HTTPS = "https" + "://" + WIREMOCK_HOST;
  private final String WIREMOCK_HOST_WITH_HTTPS_AND_PORT = WIREMOCK_HOST_WITH_HTTPS + ":" + wiremockHttpsPort;
//  TODO: move to testutil
  private SFLoginInput createOktaLoginInput() {
    SFLoginInput input = new SFLoginInput();
//    TODO: change here to wiremock url
    input.setServerUrl("https://testauth.okta.com");
    input.setServerUrl(WIREMOCK_HOST_WITH_HTTPS_AND_PORT);
    input.setUserName("MOCK_USERNAME");
    input.setPassword("MOCK_PASSWORD");
    input.setAccountName("MOCK_ACCOUNT_NAME");
    input.setAppId("MOCK_APP_ID");
    input.setOCSPMode(OCSPMode.FAIL_OPEN);
    input.setHttpClientSettingsKey(new HttpClientSettingsKey(OCSPMode.FAIL_OPEN));
    input.setLoginTimeout(1000);
    input.setSessionParameters(new HashMap<>());
    input.setAuthenticator(WIREMOCK_HOST_WITH_HTTPS_AND_PORT + "/okta-stub/vanity-url/");
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
          + "            \"request\": {\n"
          + "                \"method\": \"GET\",\n"
          + "                \"url\": \"/ocsp_response_cache.json\"\n"
          + "            },\n"
          + "            \"response\": {\n"
          + "                \"status\": 429\n"
          + "            }\n"
          + "        },\n"
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

  String wireMockMapping =
          "{\n"
                  + "    \"mappings\": [\n"
                  + "        {\n"
                  + "            \"scenarioName\": \"Mock Okta Authenticator Request\",\n"
                  + "            \"requiredScenarioState\": \"Started\",\n"
                  + "            \"newScenarioState\": \"Authenticator Requested\",\n"
                  + "            \"request\": {\n"
                  + "                \"method\": \"POST\",\n"
                  + "                \"urlPath\": \"/session/authenticator-request\",\n"
                  + "                \"queryParameters\": {\n"
                  + "                    \"request_guid\": {\n"
                  + "                        \"matches\": \".*\"\n"
                  + "                    }\n"
                  + "                }\n"
                  + "            },\n"
                  + "            \"response\": {\n"
                  + "                \"status\": 200,\n"
                  + "                \"headers\": {\n"
                  + "                    \"Content-Type\": \"application/json\"\n"
                  + "                },\n"
                  + "                \"jsonBody\": {\n"
                  + "                    \"data\": {\n"
                  + "                        \"tokenUrl\": \"" + WIREMOCK_HOST_WITH_HTTPS_AND_PORT + "/okta-stub/vanity-url/api/v1/authn\",\n"
                  // pragma: allowlist nextline secret
                  + "                        \"ssoUrl\": \"" + WIREMOCK_HOST_WITH_HTTPS_AND_PORT + "/okta-stub/vanity-url/app/snowflake/abcdefghijklmnopqrstuvwxyz/sso/saml\",\n"
                  + "                        \"proofKey\": null\n"
                  + "                    },\n"
                  + "                    \"code\": null,\n"
                  + "                    \"message\": null,\n"
                  + "                    \"success\": true\n"
                  + "                }\n"
                  + "            }\n"
                  + "        },\n"
                  + "        {\n"
                  + "            \"scenarioName\": \"Mock Okta Authn Response\",\n"
                  + "            \"request\": {\n"
                  + "                \"method\": \"POST\",\n"
                  + "                \"urlPath\": \"/okta-stub/vanity-url/api/v1/authn\"\n"
                  + "            },\n"
                  + "            \"response\": {\n"
                  + "                \"status\": 200,\n"
                  + "                \"headers\": {\n"
                  + "                    \"Content-Type\": \"application/json\"\n"
                  + "                },\n"
                  + "                \"jsonBody\": {\n"
                  + "                    \"expiresAt\": \"2023-10-13T19:18:09.000Z\",\n"
                  + "                    \"status\": \"SUCCESS\",\n"
                  + "                    \"sessionToken\": \"testsessiontoken\"\n"
                  + "                }\n"
                  + "            }\n"
                  + "        },\n"
                  + "        {\n"
                  + "            \"scenarioName\": \"Mock Okta SAML Response\",\n"
                  + "            \"request\": {\n"
                  + "                \"method\": \"GET\",\n"
                  // pragma: allowlist nextline secret
                  + "                \"urlPath\": \"/okta-stub/vanity-url/app/snowflake/abcdefghijklmnopqrstuvwxyz/sso/saml\",\n"
                  + "                \"queryParameters\": {\n"
                  + "                    \"RelayState\": { \"matches\": \".*\" },\n"
                  + "                    \"onetimetoken\": { \"matches\": \".*\" },\n"
                  + "                    \"request_guid\": { \"matches\": \".*\" }\n"
                  + "                }\n"
                  + "            },\n"
                  + "            \"response\": {\n"
                  + "                \"status\": 200,\n"
                  + "                \"headers\": {\n"
                  + "                    \"Content-Type\": \"text/html\"\n"
                  + "                },\n"
                  + "                \"body\": \"<body><form action=\\\"" + WIREMOCK_HOST_WITH_HTTPS_AND_PORT + "/okta-stub/vanity-url/\\\"></form></body>\"\n"
                  + "            }\n"
                  + "        },\n"
                  + "        {\n"
                  + "            \"scenarioName\": \"Mock Okta Login Request\",\n"
                  + "            \"request\": {\n"
                  + "                \"method\": \"POST\",\n"
                  + "                \"urlPath\": \"/session/v1/login-request\",\n"
                  + "                \"queryParameters\": {\n"
                  + "                    \"requestId\": { \"matches\": \".*\" },\n"
                  + "                    \"request_guid\": { \"matches\": \".*\" }\n"
                  + "                }\n"
                  + "            },\n"
                  + "            \"response\": {\n"
                  + "                \"status\": 200,\n"
                  + "                \"headers\": {\n"
                  + "                    \"Content-Type\": \"application/json\"\n"
                  + "                },\n"
                  + "                \"jsonBody\": {\n"
                  + "                    \"data\": {\n"
                  + "                        \"tokenUrl\": \"https://testauth.okta.com/api/v1/authn\",\n"
                  + "                        \"ssoUrl\": \"https://testauth.okta.com/app/snowflake/abcdefghijklmnopqrstuvwxyz/sso/saml\",\n"
                  + "                        \"proofKey\": null\n"
                  + "                    },\n"
                  + "                    \"code\": null,\n"
                  + "                    \"message\": null,\n"
                  + "                    \"success\": true\n"
                  + "                }\n"
                  + "            }\n"
                  + "        }\n"
                  + "    ],\n"
                  + "    \"importOptions\": {\n"
                  + "        \"duplicatePolicy\": \"IGNORE\",\n"
                  + "        \"deleteAllNotInImport\": true\n"
                  + "    }\n"
                  + "}";

  @Test
  public void testOktaRetryWaitsUsingDefaultRetryStrategy() throws Throwable {
    importMapping(wireMockMapping);
    setCustomTrustStorePropertyPath();
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