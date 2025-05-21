package net.snowflake.client.core;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.jdbc.BaseWiremockTest;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.CORE)
public class SessionUtilWiremockIT extends BaseWiremockTest {
  private static final int DECREASED_LOGIN_TIMEOUT = 5;
  private static final String OKTA_VANITY_PATH = "/okta-stub/vanity-url";
  private static final String OKTA_AUTH_API_ENDPOINT = OKTA_VANITY_PATH + "/api/v1";
  private static final String OKTA_SAML_RESPONSE_SUBPATH = "/sso/saml";
  private static final String ALWAYS_429_IN_FEDERATED_STEP_3 =
      "/wiremock/mappings/session/session-util-wiremock-it-always-429-in-federated-step-3.json";
  private static final String ALWAYS_429_IN_FEDERATED_STEP_4 =
      "/wiremock/mappings/session/session-util-wiremock-it-always-429-in-federated-step-4.json";
  private static final String MULTIPLE_429_IN_FEDERATED_STEP_3 =
      "/wiremock/mappings/session/session-util-wiremock-it-multiple-429-in-federated-step-3.json";
  private static final String MULTIPLE_429_IN_FEDERATED_STEP_4 =
      "/wiremock/mappings/session/session-util-wiremock-it-multiple-429-in-federated-step-4.json";

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

    SFLoginInput loginInput = createOktaLoginInputBase();
    Map<SFSessionProperty, Object> connectionPropertiesMap = initConnectionPropertiesMap();

    // WHEN
    try {
      SessionUtil.openSession(loginInput, connectionPropertiesMap, "ALL");
    } catch (SnowflakeSQLException ex) {
      fail("SessionUtil test failed with error: " + ex.getMessage());
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

    SFLoginInput loginInput = createOktaLoginInputBase();
    Map<SFSessionProperty, Object> connectionPropertiesMap = initConnectionPropertiesMap();

    // WHEN
    try {
      SessionUtil.openSession(loginInput, connectionPropertiesMap, "ALL");
    } catch (SnowflakeSQLException ex) {
      fail(ex.getMessage());
    }

    // THEN
    List<MinimalServeEvent> allEvents = getAllServeEvents();

    // Filter only events that hit the final endpoint (federated step 4) - using the retrieved
    // token.
    List<MinimalServeEvent> vanityUrlCalls =
        allEvents.stream()
            .filter(e -> e.getRequest().getUrl().contains(OKTA_SAML_RESPONSE_SUBPATH))
            .sorted(Comparator.comparing(e -> e.getRequest().getLoggedDate()))
            .collect(Collectors.toList());

    assertThat(
        "Expected multiple calls to "
            + OKTA_SAML_RESPONSE_SUBPATH
            + ", got "
            + vanityUrlCalls.size(),
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
  // This test can be flake due to approximated value of duration of retries execution
  public void testOktaRetriesUntilTimeoutThenRaisesAuthTimeoutExceptionWhen429InFederatedStep4()
      throws Throwable {
    // GIVEN
    final int ALLOWED_DIFFERENCE_BETWEEN_LOGIN_TIMEOUT_AND_ACTUAL_DURATION_IN_MS = 500;
    Map<String, Object> placeholders = new HashMap<>();
    placeholders.put("{{WIREMOCK_HOST_WITH_HTTPS_AND_PORT}}", WIREMOCK_HOST_WITH_HTTPS_AND_PORT);
    String wireMockMapping =
        getWireMockMappingFromFile(ALWAYS_429_IN_FEDERATED_STEP_4, placeholders);
    importMapping(wireMockMapping);

    setCustomTrustStorePropertyPath();

    SFLoginInput loginInput = createOktaLoginInputBase();
    loginInput.setLoginTimeout(DECREASED_LOGIN_TIMEOUT); // decreased timeout for test purposes
    Map<SFSessionProperty, Object> connectionPropertiesMap = initConnectionPropertiesMap();

    try {
      // WHEN
      SessionUtil.openSession(loginInput, connectionPropertiesMap, "ALL");
      // THEN
    } catch (SnowflakeSQLException ex) {
      assertThat(
          "When timeout for login in retrieving OKTA auth response is reached NETWORK_ERROR should be raised",
          ex.getErrorCode(),
          equalTo(ErrorCode.NETWORK_ERROR.getMessageCode()));
    }

    List<MinimalServeEvent> allEvents = getAllServeEvents();

    // Filter only events that hit the final endpoint (federated step 4) - using the retrieved
    // token.
    List<MinimalServeEvent> vanityUrlCalls =
        allEvents.stream()
            .filter(e -> e.getRequest().getUrl().contains(OKTA_VANITY_PATH))
            .sorted(Comparator.comparing(e -> e.getRequest().getLoggedDate()))
            .collect(Collectors.toList());

    // This can cause test to be flaky - if for some reason execution of steps before sending the
    // first request takes too long (time is approximated based on time of arrival of the first and
    // the last request to wiremock)
    // Most important for this check is to make sure that we honor the login timeout even when
    // retryContext is injected (which issues requests as well inside)
    assertThatTotalLoginTimeoutIsKeptWhenRetrying(
        vanityUrlCalls,
        loginInput.getLoginTimeout(),
        ALLOWED_DIFFERENCE_BETWEEN_LOGIN_TIMEOUT_AND_ACTUAL_DURATION_IN_MS);
  }

  private void assertThatTotalLoginTimeoutIsKeptWhenRetrying(
      List<MinimalServeEvent> requestEvents, long loginTimeout, long allowedDifferenceInMs) {
    final int SECONDS_TO_MS_FACTOR = 1000;
    long firstRequestTime = requestEvents.get(0).getRequest().getLoggedDate().getTime();
    long lastRequestTime =
        requestEvents.get(requestEvents.size() - 1).getRequest().getLoggedDate().getTime();
    long approximatedDurationOfOktaRetriesBasedOnLogsInMs = lastRequestTime - firstRequestTime;
    long differenceBetweenTimeoutAndCalculatedDurationInMs =
        Math.abs(
            loginTimeout * SECONDS_TO_MS_FACTOR - approximatedDurationOfOktaRetriesBasedOnLogsInMs);
    assertThat(
        String.format(
            "Retrying calls to okta lasted %d ms, while login timeout was set to %d ms.",
            approximatedDurationOfOktaRetriesBasedOnLogsInMs, loginTimeout),
        allowedDifferenceInMs,
        greaterThanOrEqualTo(differenceBetweenTimeoutAndCalculatedDurationInMs));
  }

  private void assertRequestsToWiremockHaveDelay(
      List<MinimalServeEvent> requestEvents, long minExpectedDelayBetweenCalls) {
    for (int i = 1; i < requestEvents.size(); i++) {
      long t1 = requestEvents.get(i - 1).getRequest().getLoggedDate().getTime();
      long t2 = requestEvents.get(i).getRequest().getLoggedDate().getTime();
      long deltaMillis = t2 - t1;
      assertThat(
          String.format(
              "Consecutive calls were only %d ms apart (index %d -> %d).", deltaMillis, i - 1, i),
          deltaMillis,
          greaterThanOrEqualTo(minExpectedDelayBetweenCalls));
    }
  }

  /**
   * Ensures that each request *with* the given parameter uses a unique value. Requests that do not
   * have the parameter are ignored. Fails if any duplicate parameter values are detected.
   */
  private void assertRequestsToWiremockHaveDifferentValuesOfParameter(
      List<MinimalServeEvent> requestEvents, String parameterName) {
    // Extract all parameter values from requests that have this parameter
    List<String> paramValues =
        requestEvents.stream()
            .filter(e -> e.getRequest().getQueryParams().containsKey(parameterName))
            .map(e -> e.getRequest().getQueryParams().get(parameterName).firstValue())
            .collect(Collectors.toList());

    long distinctCount = paramValues.stream().distinct().count();
    assertThat(
        "Found duplicate value(s) for parameter '" + parameterName + "'. Values: " + paramValues,
        distinctCount,
        equalTo((long) paramValues.size()));
  }
}
