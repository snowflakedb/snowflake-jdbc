package net.snowflake.client.core;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.jdbc.BaseWiremockTest;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Disabled
@Tag(TestTags.CORE)
public class SessionUtilWiremockIT extends BaseWiremockTest {
  private static final String OKTA_VANITY_PATH = "/okta-stub/vanity-url";
  private static final String OKTA_AUTH_API_ENDPOINT = OKTA_VANITY_PATH + "/api/v1";
  private static final String ALWAYS_429_MAPPING =
      "/net/snowflake/client/jdbc/wiremock-mappings/session-util-wiremock-it-always-429-response.json";

  /**
   * Minimum spacing we expect between consecutive requests, in milliseconds - associated with
   * RestRequest minBackoff.
   */
  private static final long EXPECTED_MIN_RETRY_DELAY_MS = 1000;

  private final String WIREMOCK_HOST_WITH_HTTPS = "https://" + WIREMOCK_HOST;
  private final String WIREMOCK_HOST_WITH_HTTPS_AND_PORT =
      WIREMOCK_HOST_WITH_HTTPS + ":" + wiremockHttpsPort;

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
    input.setAuthenticator(WIREMOCK_HOST_WITH_HTTPS_AND_PORT + OKTA_VANITY_PATH);
    return input;
  }

  private String getProxyProtocol(Properties props) {
    return props.get("ssl").toString().equals("on") ? "https" : "http";
  }

  private int getProxyPort(String proxyProtocol) {
    return Objects.equals(proxyProtocol, "http") ? wiremockHttpPort : wiremockHttpsPort;
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
    Map<String, Object> placeholders = new HashMap<>();
    placeholders.put("{{WIREMOCK_HOST_WITH_HTTPS_AND_PORT}}", WIREMOCK_HOST_WITH_HTTPS_AND_PORT);

    String wireMockMapping = getWireMockMappingFromFile(ALWAYS_429_MAPPING, placeholders);
    importMapping(wireMockMapping);

    // Basic setup for trust store, proxy, etc.
    setCustomTrustStorePropertyPath();
    Properties props = getProperties();
    setJvmProperties(props);

    SFLoginInput loginInput = createOktaLoginInput();
    Map<SFSessionProperty, Object> connectionPropertiesMap = initConnectionPropertiesMap();

    // WHEN
    try {
      SessionUtil.openSession(loginInput, connectionPropertiesMap, "ALL");
    } catch (SnowflakeSQLException ex) {
      assertTrue(ex.getMessage().contains("429"));
    }

    // THEN
    List<MinimalServeEvent> allEvents = getAllServeEvents();

    // Filter only events that hit "/okta-stub/vanity-url/".
    List<MinimalServeEvent> vanityUrlCalls =
        allEvents.stream()
            .filter(e -> e.getRequest().getUrl().contains(OKTA_AUTH_API_ENDPOINT))
            .sorted(Comparator.comparing(e -> e.getRequest().getLoggedDate()))
            .collect(Collectors.toList());

    assertThat(
        "Expected multiple calls to " + OKTA_AUTH_API_ENDPOINT + ", got " + vanityUrlCalls.size(),
        vanityUrlCalls.size(),
        greaterThan(2));

    // Ensure each consecutive pair of calls has at least 1-second gap (1000 ms).
    for (int i = 1; i < vanityUrlCalls.size(); i++) {
      long t1 = vanityUrlCalls.get(i - 1).getRequest().getLoggedDate().getTime();
      long t2 = vanityUrlCalls.get(i).getRequest().getLoggedDate().getTime();
      long deltaMillis = t2 - t1;
      assertThat(
          String.format(
              "Consecutive calls to %s were only %d ms apart (index %d -> %d).",
              OKTA_AUTH_API_ENDPOINT, deltaMillis, i - 1, i),
          deltaMillis,
          greaterThan(EXPECTED_MIN_RETRY_DELAY_MS));
    }
  }
}
