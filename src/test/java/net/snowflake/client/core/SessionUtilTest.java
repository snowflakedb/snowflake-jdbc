package net.snowflake.client.core;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mockStatic;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.BooleanNode;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.snowflake.client.core.auth.AuthenticatorType;
import net.snowflake.client.jdbc.MockConnectionTest;
import net.snowflake.client.jdbc.SnowflakeUtil;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

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

  @Test
  public void shouldProperlyCheckIfExperimentalAuthEnabled() {
    try (MockedStatic<SnowflakeUtil> snowflakeUtilMockedStatic = mockStatic(SnowflakeUtil.class)) {
      snowflakeUtilMockedStatic
          .when(() -> SnowflakeUtil.systemGetEnv("SF_ENABLE_EXPERIMENTAL_AUTHENTICATION"))
          .thenReturn(null);
      assertDoesNotThrow(
          () ->
              SessionUtil.checkIfExperimentalAuthnEnabled(
                  AuthenticatorType.OAUTH_AUTHORIZATION_CODE));
      assertDoesNotThrow(
          () ->
              SessionUtil.checkIfExperimentalAuthnEnabled(
                  AuthenticatorType.OAUTH_CLIENT_CREDENTIALS));
      assertThrows(
          SFException.class,
          () ->
              SessionUtil.checkIfExperimentalAuthnEnabled(
                  AuthenticatorType.PROGRAMMATIC_ACCESS_TOKEN));
      assertThrows(
          SFException.class,
          () -> SessionUtil.checkIfExperimentalAuthnEnabled(AuthenticatorType.WORKLOAD_IDENTITY));

      snowflakeUtilMockedStatic
          .when(() -> SnowflakeUtil.systemGetEnv("SF_ENABLE_EXPERIMENTAL_AUTHENTICATION"))
          .thenReturn("true");
      assertDoesNotThrow(
          () ->
              SessionUtil.checkIfExperimentalAuthnEnabled(
                  AuthenticatorType.OAUTH_AUTHORIZATION_CODE));
      assertDoesNotThrow(
          () ->
              SessionUtil.checkIfExperimentalAuthnEnabled(
                  AuthenticatorType.OAUTH_CLIENT_CREDENTIALS));
      assertDoesNotThrow(
          () ->
              SessionUtil.checkIfExperimentalAuthnEnabled(
                  AuthenticatorType.PROGRAMMATIC_ACCESS_TOKEN));
      assertDoesNotThrow(
          () -> SessionUtil.checkIfExperimentalAuthnEnabled(AuthenticatorType.WORKLOAD_IDENTITY));
    }
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
}
