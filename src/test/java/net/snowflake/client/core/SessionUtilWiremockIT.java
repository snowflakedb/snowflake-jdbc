package net.snowflake.client.core;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.jdbc.BaseWiremockTest;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.OTHERS)
public class SessionUtilWiremockIT extends BaseWiremockTest {
  final int DECREASED_LOGIN_TIMEOUT = 5;
  private static final String OKTA_VANITY_PATH = "/okta-stub/vanity-url";
  private static final String OKTA_AUTH_API_ENDPOINT = OKTA_VANITY_PATH + "/api/v1";
  private static final String OKTA_SAML_RESPONSE_SUBPATH = "/sso/saml";
  private static final String ALWAYS_429_IN_FEDERATED_STEP_3 =
          "/net/snowflake/client/jdbc/wiremock-mappings/session-util-wiremock-it-always-429-in-federated-step-3.json";
  private static final String ALWAYS_429_IN_FEDERATED_STEP_4 =
          "/net/snowflake/client/jdbc/wiremock-mappings/session-util-wiremock-it-always-429-in-federated-step-4.json";
  private static final String MULTIPLE_429_IN_FEDERATED_STEP_3 =
          "/net/snowflake/client/jdbc/wiremock-mappings/session-util-wiremock-it-multiple-429-in-federated-step-3.json";
  private static final String MULTIPLE_429_IN_FEDERATED_STEP_4 =
          "/net/snowflake/client/jdbc/wiremock-mappings/session-util-wiremock-it-multiple-429-in-federated-step-4.json";
  private static final String MULTIPLE_429_FROM_OKTA_WHEN_LOGIN_TO_SF =
          "/net/snowflake/client/jdbc/wiremock-mappings/session-util-wiremock-it-multiple-429-from-okta-in-login-request-to-sf.json";

  /**
   * Minimum spacing we expect between consecutive requests, in milliseconds - associated with
   * RestRequest minBackoff.
   */
  private static final long EXPECTED_MIN_RETRY_DELAY_MS = 1000;

  private final String WIREMOCK_HOST_WITH_HTTPS = "https://" + WIREMOCK_HOST;
  private final String WIREMOCK_HOST_WITH_HTTPS_AND_PORT =
          WIREMOCK_HOST_WITH_HTTPS + ":" + wiremockHttpsPort;

  private SFLoginInput createOktaLoginInputBase() {
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
  public void testOktaRetryWaitsUsingDefaultRetryStrategyWhen429InFederatedStep3()
          throws Throwable {
    // GIVEN
    Map<String, Object> placeholders = new HashMap<>();
    placeholders.put("{{WIREMOCK_HOST_WITH_HTTPS_AND_PORT}}", WIREMOCK_HOST_WITH_HTTPS_AND_PORT);

    String wireMockMapping =
            getWireMockMappingFromFile(MULTIPLE_429_IN_FEDERATED_STEP_3, placeholders);
    importMapping(wireMockMapping);

    setCustomTrustStorePropertyPath();
    Properties props = getProperties();
    setJvmProperties(props);

    SFLoginInput loginInput = createOktaLoginInputBase();
    Map<SFSessionProperty, Object> connectionPropertiesMap = initConnectionPropertiesMap();

    // WHEN
    try {
      SessionUtil.openSession(loginInput, connectionPropertiesMap, "ALL");
    } catch (SnowflakeSQLException ex) {
      fail(ex.getMessage());
    }

    // THEN
    List<ServeEvent> allEvents = getAllServeEvents();

    // Filter only events that hit "/okta-stub/vanity-url/".
    List<ServeEvent> vanityUrlCalls =
            allEvents.stream()
                    .filter(e -> e.getRequest().getUrl().contains(OKTA_AUTH_API_ENDPOINT))
                    .sorted(Comparator.comparing(e -> e.getRequest().getLoggedDate()))
                    .collect(Collectors.toList());

    assertThat(
            "Expected multiple calls to " + OKTA_AUTH_API_ENDPOINT + ", got " + vanityUrlCalls.size(),
            vanityUrlCalls.size(),
            greaterThan(2));

    // Ensure each consecutive pair of calls has at least EXPECTED_MIN_RETRY_DELAY_MS gap
    // (determined by RestRequest.minBackoffInMilli (=1000 ms)).
    assertRequestsToWiremockHaveDelay(vanityUrlCalls, EXPECTED_MIN_RETRY_DELAY_MS);
  }

  @Test
  public void testOktaRetryWaitsUsingDefaultRetryStrategyWhen429InFederatedStep4()
          throws Throwable {
    // GIVEN
    Map<String, Object> placeholders = new HashMap<>();
    placeholders.put("{{WIREMOCK_HOST_WITH_HTTPS_AND_PORT}}", WIREMOCK_HOST_WITH_HTTPS_AND_PORT);

    String wireMockMapping =
            getWireMockMappingFromFile(MULTIPLE_429_IN_FEDERATED_STEP_4, placeholders);
    importMapping(wireMockMapping);

    setCustomTrustStorePropertyPath();
    Properties props = getProperties();
    setJvmProperties(props);

    SFLoginInput loginInput = createOktaLoginInputBase();
    Map<SFSessionProperty, Object> connectionPropertiesMap = initConnectionPropertiesMap();

    // WHEN
    try {
      SessionUtil.openSession(loginInput, connectionPropertiesMap, "ALL");
    } catch (SnowflakeSQLException ex) {
      fail(ex.getMessage());
    }

    // THEN
    List<ServeEvent> allEvents = getAllServeEvents();

    // Filter only events that hit the final endpoint (federated step 4) - using the retrieved token.
    List<ServeEvent> vanityUrlCalls =
            allEvents.stream()
                    .filter(e -> e.getRequest().getUrl().contains(OKTA_SAML_RESPONSE_SUBPATH))
                    .sorted(Comparator.comparing(e -> e.getRequest().getLoggedDate()))
                    .collect(Collectors.toList());

    assertThat(
            "Expected multiple calls to " + OKTA_SAML_RESPONSE_SUBPATH + ", got " + vanityUrlCalls.size(),
            vanityUrlCalls.size(),
            greaterThan(2));

    assertRequestsToWiremockHaveDelay(vanityUrlCalls, EXPECTED_MIN_RETRY_DELAY_MS);
    assertRequestsToWiremockHaveDifferentValuesOfParameter(vanityUrlCalls, "onetimetoken");
  }

  @Test
  public void testOktaRetriesUntilTimeoutThenRaisesAuthTimeoutExceptionWhen429InFederatedStep3()
          throws Throwable {
    // GIVEN
    Map<String, Object> placeholders = new HashMap<>();
    placeholders.put("{{WIREMOCK_HOST_WITH_HTTPS_AND_PORT}}", WIREMOCK_HOST_WITH_HTTPS_AND_PORT);

    String wireMockMapping =
            getWireMockMappingFromFile(ALWAYS_429_IN_FEDERATED_STEP_3, placeholders);
    importMapping(wireMockMapping);

    setCustomTrustStorePropertyPath();
    Properties props = getProperties();
    setJvmProperties(props);

    SFLoginInput loginInput = createOktaLoginInputBase();
    loginInput.setLoginTimeout(5); // decreased timeout for test purposes
    Map<SFSessionProperty, Object> connectionPropertiesMap = initConnectionPropertiesMap();

    // WHEN
    try {
      SessionUtil.openSession(loginInput, connectionPropertiesMap, "ALL");
    } catch (SnowflakeSQLException ex) {
      assertThat(
              "When timeout for login in retrieving OKTA auth response is reached NETWORK_ERROR should be raised",
              ex.getErrorCode(),
              equalTo(ErrorCode.NETWORK_ERROR.getMessageCode()));
    }
  }

  @Test
  public void testOktaRetriesUntilTimeoutThenRaisesAuthTimeoutExceptionWhen429InFederatedStep4()
          throws Throwable {
    // GIVEN
    Map<String, Object> placeholders = new HashMap<>();
    placeholders.put("{{WIREMOCK_HOST_WITH_HTTPS_AND_PORT}}", WIREMOCK_HOST_WITH_HTTPS_AND_PORT);
    String wireMockMapping =
            getWireMockMappingFromFile(ALWAYS_429_IN_FEDERATED_STEP_4, placeholders);
    importMapping(wireMockMapping);

    setCustomTrustStorePropertyPath();
    Properties props = getProperties();
    setJvmProperties(props);

    SFLoginInput loginInput = createOktaLoginInputBase();
    loginInput.setLoginTimeout(DECREASED_LOGIN_TIMEOUT); // decreased timeout for test purposes
    Map<SFSessionProperty, Object> connectionPropertiesMap = initConnectionPropertiesMap();

    // WHEN
    try {
      SessionUtil.openSession(loginInput, connectionPropertiesMap, "ALL");
    } catch (SnowflakeSQLException ex) {
      assertThat(
              "When timeout for login in retrieving OKTA auth response is reached NETWORK_ERROR should be raised",
              ex.getErrorCode(),
              equalTo(ErrorCode.NETWORK_ERROR.getMessageCode()));
    }
  }

  @Test
  public void testTotalLoginTimeoutIsKeptWhenOktaRetriesUntilTimeoutThenRaisesAuthTimeoutExceptionWhen429InFederatedStep4()
          throws Throwable {
    // GIVEN
    Map<String, Object> placeholders = new HashMap<>();
    placeholders.put("{{WIREMOCK_HOST_WITH_HTTPS_AND_PORT}}", WIREMOCK_HOST_WITH_HTTPS_AND_PORT);

    String wireMockMapping =
            getWireMockMappingFromFile(ALWAYS_429_IN_FEDERATED_STEP_4, placeholders);
    importMapping(wireMockMapping);

    setCustomTrustStorePropertyPath();
    Properties props = getProperties();
    setJvmProperties(props);

    SFLoginInput loginInput = createOktaLoginInputBase();
    loginInput.setLoginTimeout(DECREASED_LOGIN_TIMEOUT); // decreased timeout for test purposes
    Map<SFSessionProperty, Object> connectionPropertiesMap = initConnectionPropertiesMap();

    // WHEN
    try {
      SessionUtil.openSession(loginInput, connectionPropertiesMap, "ALL");
    } catch (SnowflakeSQLException ex) {
      assertThat(
              "When timeout for login in retrieving OKTA auth response is reached NETWORK_ERROR should be raised",
              ex.getErrorCode(),
              equalTo(ErrorCode.NETWORK_ERROR.getMessageCode()));
    }
  }
}
