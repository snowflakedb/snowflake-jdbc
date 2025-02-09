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
public class SessionUtilWiremockIT extends BaseWiremockTest {
  private final String WIREMOCK_HOST_WITH_HTTPS = "https" + "://" + WIREMOCK_HOST;
  private final String WIREMOCK_HOST_WITH_HTTPS_AND_PORT = WIREMOCK_HOST_WITH_HTTPS + ":" + wiremockHttpsPort;

  private SFLoginInput createOktaLoginInput() {
    SFLoginInput input = new SFLoginInput();
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


  @Test
  public void testOktaRetryWaitsUsingDefaultRetryStrategy() throws Throwable {
    // GIVEN
    String wireMockMappingPathFromResources = "/net/snowflake/client/jdbc/wiremock-mappings/session-util-wiremock-it-always-429-response.json";
    Map<String, Object> placeholders = new HashMap<>();
    placeholders.put("{{WIREMOCK_HOST_WITH_HTTPS_AND_PORT}}", WIREMOCK_HOST_WITH_HTTPS_AND_PORT);
    String wireMockMapping = getWireMockMappingFromFile(wireMockMappingPathFromResources, placeholders);
    importMapping(wireMockMapping);
    setCustomTrustStorePropertyPath();
    Properties props = getProperties();
    setJvmProperties(props);
    SFLoginInput loginInput = createOktaLoginInput();
    Map<SFSessionProperty, Object> connectionPropertiesMap = initConnectionPropertiesMap();

    // WHEN
    SessionUtil.openSession(loginInput, connectionPropertiesMap, "ALL");
  }
}

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