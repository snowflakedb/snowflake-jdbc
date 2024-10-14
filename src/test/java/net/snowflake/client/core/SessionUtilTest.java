/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.node.BooleanNode;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.snowflake.client.jdbc.MockConnectionTest;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.junit.jupiter.api.Test;

public class SessionUtilTest {

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

  private void resetOcspConfiguration() {
    SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_URL_VALUE = null;
    SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_RETRY_URL_PATTERN = null;
  }
}
