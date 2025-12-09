package net.snowflake.client.core;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.BooleanNode;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.InvalidPathException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.snowflake.client.core.auth.AuthenticatorType;
import net.snowflake.client.internal.core.minicore.MinicoreLoadResult;
import net.snowflake.client.internal.core.minicore.MinicoreTelemetry;
import net.snowflake.client.jdbc.MockConnectionTest;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class SessionUtilTest {
  private static String originalUrlValue;
  private static String originalRetryUrlPattern;

  @BeforeAll
  public static void saveStaticValues() {
    originalUrlValue = SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_URL_VALUE;
    originalRetryUrlPattern = SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_RETRY_URL_PATTERN;
  }

  @AfterAll
  public static void restoreStaticValues() {
    SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_URL_VALUE = originalUrlValue;
    SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_RETRY_URL_PATTERN = originalRetryUrlPattern;
  }

  /** Test isPrefixEqual */
  @Test
  public void testIsPrefixEqual() throws Exception {
    assertThat(
        "no port number",
        SessionUtil.isPrefixEqual(
            "https://testaccount.snowflakecomputing.com/blah",
            "https://testaccount.snowflakecomputing.com/"));
    assertThat(
        "no port number with a slash",
        SessionUtil.isPrefixEqual(
            "https://testaccount.snowflakecomputing.com/blah",
            "https://testaccount.snowflakecomputing.com"));
    assertThat(
        "including a port number on one of them",
        SessionUtil.isPrefixEqual(
            "https://testaccount.snowflakecomputing.com/blah",
            "https://testaccount.snowflakecomputing.com:443/"));

    // negative
    assertThat(
        "different hostnames",
        !SessionUtil.isPrefixEqual(
            "https://testaccount1.snowflakecomputing.com/blah",
            "https://testaccount2.snowflakecomputing.com/"));
    assertThat(
        "different port numbers",
        !SessionUtil.isPrefixEqual(
            "https://testaccount.snowflakecomputing.com:123/blah",
            "https://testaccount.snowflakecomputing.com:443/"));
    assertThat(
        "different protocols",
        !SessionUtil.isPrefixEqual(
            "http://testaccount.snowflakecomputing.com/blah",
            "https://testaccount.snowflakecomputing.com/"));
  }

  @Test
  public void testParameterParsing() {
    Map<String, Object> parameterMap = new HashMap<>();
    parameterMap.put("other_parameter", BooleanNode.getTrue());
    SFBaseSession session = new MockConnectionTest.MockSnowflakeConnectionImpl().getSFSession();
    SessionUtil.updateSfDriverParamValues(parameterMap, session);
    assertTrue(((BooleanNode) session.getOtherParameter("other_parameter")).asBoolean());
  }

  @Test
  public void testConvertSystemPropertyToIntValue() {
    try {
      // Test that setting real value works
      System.setProperty("net.snowflake.jdbc.max_connections", "500");
      assertEquals(
          500,
          SystemUtil.convertSystemPropertyToIntValue(
              HttpUtil.JDBC_MAX_CONNECTIONS_PROPERTY, HttpUtil.DEFAULT_MAX_CONNECTIONS));
      // Test that entering a non-int sets the value to the default
      System.setProperty("net.snowflake.jdbc.max_connections", "notAnInteger");
      assertEquals(
          HttpUtil.DEFAULT_MAX_CONNECTIONS,
          SystemUtil.convertSystemPropertyToIntValue(
              HttpUtil.JDBC_MAX_CONNECTIONS_PROPERTY, HttpUtil.DEFAULT_MAX_CONNECTIONS));
      // Test another system property
      System.setProperty("net.snowflake.jdbc.max_connections_per_route", "30");
      assertEquals(
          30,
          SystemUtil.convertSystemPropertyToIntValue(
              HttpUtil.JDBC_MAX_CONNECTIONS_PER_ROUTE_PROPERTY,
              HttpUtil.DEFAULT_MAX_CONNECTIONS_PER_ROUTE));
    } finally {
      System.clearProperty("net.snowflake.jdbc.max_connections");
      System.clearProperty("net.snowflake.jdbc.max_connections_per_route");
    }
  }

  @Test
  public void testIsLoginRequest() {
    List<String> testCases = new ArrayList<String>();
    testCases.add("/session/v1/login-request");
    testCases.add("/session/token-request");
    testCases.add("/session/authenticator-request");

    for (String testCase : testCases) {
      try {
        URIBuilder uriBuilder = new URIBuilder("https://test.snowflakecomputing.com");
        uriBuilder.setPath(testCase);
        URI uri = uriBuilder.build();
        HttpPost postRequest = new HttpPost(uri);
        assertTrue(SessionUtil.isNewRetryStrategyRequest(postRequest));
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Test
  public void testIsLoginRequestInvalidURIPath() {
    List<String> testCases = new ArrayList<String>();
    testCases.add("/session/not-a-real-path");

    for (String testCase : testCases) {
      try {
        URIBuilder uriBuilder = new URIBuilder("https://test.snowflakecomputing.com");
        uriBuilder.setPath(testCase);
        URI uri = uriBuilder.build();
        HttpPost postRequest = new HttpPost(uri);
        assertFalse(SessionUtil.isNewRetryStrategyRequest(postRequest));
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Test
  public void shouldDerivePrivateLinkOcspCacheServerUrlBasedOnHost() throws IOException {
    resetOcspConfiguration();

    SessionUtil.resetOCSPUrlIfNecessary("https://test.privatelink.snowflakecomputing.com");
    assertEquals(
        "http://ocsp.test.privatelink.snowflakecomputing.com/ocsp_response_cache.json",
        SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_URL_VALUE);
    assertEquals(
        "http://ocsp.test.privatelink.snowflakecomputing.com/retry/%s/%s",
        SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_RETRY_URL_PATTERN);

    resetOcspConfiguration();

    SessionUtil.resetOCSPUrlIfNecessary("https://test.privatelink.snowflakecomputing.cn");
    assertEquals(
        "http://ocsp.test.privatelink.snowflakecomputing.cn/ocsp_response_cache.json",
        SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_URL_VALUE);
    assertEquals(
        "http://ocsp.test.privatelink.snowflakecomputing.cn/retry/%s/%s",
        SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_RETRY_URL_PATTERN);

    resetOcspConfiguration();

    SessionUtil.resetOCSPUrlIfNecessary("https://test.privatelink.snowflakecomputing.xyz");
    assertEquals(
        "http://ocsp.test.privatelink.snowflakecomputing.xyz/ocsp_response_cache.json",
        SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_URL_VALUE);
    assertEquals(
        "http://ocsp.test.privatelink.snowflakecomputing.xyz/retry/%s/%s",
        SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_RETRY_URL_PATTERN);
  }

  @Test
  public void testGetCommonParams() throws Exception {
    ObjectMapper mapper = ObjectMapperFactory.getObjectMapper();

    // Test unknown param name
    Map<String, Object> result =
        SessionUtil.getCommonParams(
            mapper.readTree("[{\"name\": \"testParam\", \"value\": true}]"));
    assertTrue((boolean) result.get("testParam"));
    result =
        SessionUtil.getCommonParams(
            mapper.readTree("[{\"name\": \"testParam\", \"value\": false}]"));
    assertFalse((boolean) result.get("testParam"));

    result =
        SessionUtil.getCommonParams(mapper.readTree("[{\"name\": \"testParam\", \"value\": 0}]"));
    assertEquals(0, (int) result.get("testParam"));
    result =
        SessionUtil.getCommonParams(
            mapper.readTree("[{\"name\": \"testParam\", \"value\": 1000}]"));
    assertEquals(1000, (int) result.get("testParam"));

    result =
        SessionUtil.getCommonParams(
            mapper.readTree("[{\"name\": \"testParam\", \"value\": \"\"}]"));
    assertEquals("", result.get("testParam"));
    result =
        SessionUtil.getCommonParams(
            mapper.readTree("[{\"name\": \"testParam\", \"value\": \"value\"}]"));
    assertEquals("value", result.get("testParam"));

    // Test known param name
    result =
        SessionUtil.getCommonParams(
            mapper.readTree("[{\"name\": \"CLIENT_DISABLE_INCIDENTS\", \"value\": true}]"));
    assertTrue((boolean) result.get("CLIENT_DISABLE_INCIDENTS"));
    result =
        SessionUtil.getCommonParams(
            mapper.readTree("[{\"name\": \"CLIENT_DISABLE_INCIDENTS\", \"value\": false}]"));
    assertFalse((boolean) result.get("CLIENT_DISABLE_INCIDENTS"));

    result =
        SessionUtil.getCommonParams(
            mapper.readTree(
                "[{\"name\": \"CLIENT_STAGE_ARRAY_BINDING_THRESHOLD\", \"value\": 0}]"));
    assertEquals(0, (int) result.get("CLIENT_STAGE_ARRAY_BINDING_THRESHOLD"));
    result =
        SessionUtil.getCommonParams(
            mapper.readTree(
                "[{\"name\": \"CLIENT_STAGE_ARRAY_BINDING_THRESHOLD\", \"value\": 1000}]"));
    assertEquals(1000, (int) result.get("CLIENT_STAGE_ARRAY_BINDING_THRESHOLD"));

    result =
        SessionUtil.getCommonParams(mapper.readTree("[{\"name\": \"TIMEZONE\", \"value\": \"\"}]"));
    assertEquals("", result.get("TIMEZONE"));
    result =
        SessionUtil.getCommonParams(
            mapper.readTree("[{\"name\": \"TIMEZONE\", \"value\": \"value\"}]"));
    assertEquals("value", result.get("TIMEZONE"));
  }

  private void resetOcspConfiguration() {
    SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_URL_VALUE = null;
    SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_RETRY_URL_PATTERN = null;
  }

  @Test
  public void testOktaAuthRequestsAreRetriedUsingLoginRetryStrategy() {
    final String oktaSSOAuthPath = "api/v1/authn";
    final String oktaTokenAuthPath = "app/snowflake/tokenlikepartofurl/sso/saml";
    List<String> oktaAuthURLs = new ArrayList<String>();
    oktaAuthURLs.add("https://anytestpath.okta.com"); // default *.okta.com URL
    oktaAuthURLs.add("https://vanity-url.somecompany.com"); // some custom Vanity OKTA URL

    for (String oktaAuthURL : oktaAuthURLs) {
      try {
        // Check that SSO path is recognized as the new retry strategy
        assertThatPathIsRecognizedAsNewRetryStrategy(oktaAuthURL, oktaSSOAuthPath);
        // Check that Token path is recognized as the new retry strategy
        assertThatPathIsRecognizedAsNewRetryStrategy(oktaAuthURL, oktaTokenAuthPath);
      } catch (URISyntaxException e) {
        fail(
            "Test case data cannot be treated as a valid URL and path. Check test input data. Error: "
                + e.getMessage());
      }
    }
  }

  private void assertThatPathIsRecognizedAsNewRetryStrategy(String uriToTest, String pathToTest)
      throws URISyntaxException {
    URIBuilder uriBuilder = new URIBuilder(uriToTest);
    uriBuilder.setPath(pathToTest);
    URI uri = uriBuilder.build();
    HttpPost postRequest = new HttpPost(uri);
    assertThat(
        "New retry strategy (designed to serve login-like requests) should be used for okta authn endpoint authentication.",
        SessionUtil.isNewRetryStrategyRequest(postRequest));
  }

  @Test
  public void testCreateClientEnvironmentInfo() {
    // GIVEN
    SFLoginInput loginInput = new SFLoginInput();
    loginInput.setOCSPMode(OCSPMode.FAIL_OPEN);
    loginInput.setApplication("TestApp");

    Map<SFSessionProperty, Object> connectionPropertiesMap = new HashMap<>();
    connectionPropertiesMap.put(SFSessionProperty.USER, "testuser");
    connectionPropertiesMap.put(SFSessionProperty.PASSWORD, "testpass");
    connectionPropertiesMap.put(
        SFSessionProperty.SERVER_URL, "https://test.snowflakecomputing.com");
    connectionPropertiesMap.put(SFSessionProperty.HTTP_CLIENT_SOCKET_TIMEOUT, 30000);
    connectionPropertiesMap.put(SFSessionProperty.HTTP_CLIENT_CONNECTION_TIMEOUT, 10000);

    String tracingLevel = "INFO";
    AuthenticatorType authenticatorType = AuthenticatorType.SNOWFLAKE;

    // WHEN
    Map<String, Object> clientEnv =
        SessionUtil.createClientEnvironmentInfo(
            loginInput, connectionPropertiesMap, tracingLevel, authenticatorType);

    // THEN
    // Verify basic environment properties
    assertThat("OS should be set", clientEnv.containsKey("OS"));
    assertThat("OS_VERSION should be set", clientEnv.containsKey("OS_VERSION"));
    assertThat("JAVA_VERSION should be set", clientEnv.containsKey("JAVA_VERSION"));
    assertThat("JAVA_RUNTIME should be set", clientEnv.containsKey("JAVA_RUNTIME"));
    assertThat("JAVA_VM should be set", clientEnv.containsKey("JAVA_VM"));
    assertThat("OCSP_MODE should be set", clientEnv.containsKey("OCSP_MODE"));
    assertThat("JDBC_JAR_NAME should be set", clientEnv.containsKey("JDBC_JAR_NAME"));

    // Verify application path is set
    assertThat("APPLICATION_PATH should be set", clientEnv.containsKey("APPLICATION_PATH"));
    assertThat("APPLICATION_PATH should not be null", clientEnv.get("APPLICATION_PATH") != null);
    assertThat(
        "APPLICATION_PATH should be a string", clientEnv.get("APPLICATION_PATH") instanceof String);

    // Verify application name is set from loginInput
    assertThat(
        "APPLICATION should be set to TestApp", "TestApp".equals(clientEnv.get("APPLICATION")));

    // Verify OCSP mode is set correctly
    assertThat("OCSP_MODE should be FAIL_OPEN", "FAIL_OPEN".equals(clientEnv.get("OCSP_MODE")));

    // Verify connection parameters are included (with masked values)
    assertThat("User parameter should be included", clientEnv.containsKey("user"));
    assertThat("User has correct value", clientEnv.get("user").toString().contains("testuser"));
    assertThat("Server URL should be included", clientEnv.containsKey("serverURL"));
    assertThat(
        "Socket timeout should be included", clientEnv.containsKey("HTTP_CLIENT_SOCKET_TIMEOUT"));
    assertThat(
        "Connection timeout should be included",
        clientEnv.containsKey("HTTP_CLIENT_CONNECTION_TIMEOUT"));

    // Verify tracing level is set
    assertThat("Tracing should be set to INFO", "INFO".equals(clientEnv.get("tracing")));

    // Verify APPLICATION_PATH is a valid file path
    String applicationPath = (String) clientEnv.get("APPLICATION_PATH");
    assertThat("APPLICATION_PATH should not be empty", !applicationPath.isEmpty());
    assertThat("APPLICATION_PATH should contain file path", isValidPath(applicationPath));
  }

  @Test
  public void testMinicoreTelemetryWithSuccessfulLoad() {
    // Create a mock successful load result
    List<String> logs = new ArrayList<>();
    logs.add("Starting minicore loading");
    logs.add("Platform supported");
    logs.add("Minicore library loaded successfully");

    MinicoreLoadResult successResult =
        MinicoreLoadResult.success(
            "libsf_mini_core.so",
            null, // library instance not needed for this test
            "1.0.0",
            logs);

    MinicoreTelemetry telemetry = MinicoreTelemetry.fromLoadResult(successResult);
    Map<String, Object> telemetryMap = telemetry.toClientEnvironmentTelemetryMap();

    // THEN - Telemetry should contain success information
    assertTrue(telemetryMap.containsKey("ISA"), "ISA should be set");
    assertNotNull(telemetryMap.get("ISA"), "ISA should not be null");

    assertTrue(telemetryMap.containsKey("CORE_FILE_NAME"), "CORE_FILE_NAME should be set");
    assertEquals("libsf_mini_core.so", telemetryMap.get("CORE_FILE_NAME"));

    assertTrue(telemetryMap.containsKey("CORE_VERSION"), "CORE_VERSION should be set on success");
    assertEquals("1.0.0", telemetryMap.get("CORE_VERSION"));
    assertFalse(
        telemetryMap.containsKey("CORE_LOAD_ERROR"),
        "CORE_LOAD_ERROR should not be set on success");
  }

  @Test
  public void testMinicoreTelemetryWithFailedLoad() {
    // Create a mock failed load result
    List<String> logs = new ArrayList<>();
    logs.add("Starting minicore loading");
    logs.add("Failed to load library");

    MinicoreLoadResult failedResult =
        MinicoreLoadResult.failure(
            "Failed to load library: UnsatisfiedLinkError",
            "libsf_mini_core.so",
            new UnsatisfiedLinkError("Cannot load library"),
            logs);

    MinicoreTelemetry telemetry = MinicoreTelemetry.fromLoadResult(failedResult);
    Map<String, Object> telemetryMap = telemetry.toClientEnvironmentTelemetryMap();

    // THEN - Telemetry should contain error information
    assertTrue(telemetryMap.containsKey("ISA"), "ISA should be set");
    assertNotNull(telemetryMap.get("ISA"), "ISA should not be null");

    assertTrue(telemetryMap.containsKey("CORE_FILE_NAME"), "CORE_FILE_NAME should be set");
    assertEquals("libsf_mini_core.so", telemetryMap.get("CORE_FILE_NAME"));

    assertFalse(
        telemetryMap.containsKey("CORE_VERSION"), "CORE_VERSION should not be set on failure");
  }

  private static boolean isValidPath(String path) {
    try {
      Paths.get(path);
    } catch (InvalidPathException | NullPointerException ex) {
      return false;
    }
    return true;
  }
}
