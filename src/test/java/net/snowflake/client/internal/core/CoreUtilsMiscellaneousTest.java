package net.snowflake.client.internal.core;

import static net.snowflake.client.internal.jdbc.SnowflakeUtil.systemGetProperty;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.azure.core.http.ProxyOptions;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import net.snowflake.client.annotations.DontRunOnGithubActions;
import net.snowflake.client.api.exception.ErrorCode;
import net.snowflake.client.api.exception.SnowflakeSQLException;
import net.snowflake.client.internal.jdbc.SnowflakeUtil;
import net.snowflake.client.internal.jdbc.cloud.storage.S3HttpUtil;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.http.nio.netty.ProxyConfiguration;

public class CoreUtilsMiscellaneousTest {

  /**
   * Assert that the AssertUtil.AssertTrue statement issues the correct SFException when conditions
   * are not met
   */
  @Test
  public void testSnowflakeAssertTrue() {
    SFException e =
        assertThrows(
            SFException.class,
            () -> {
              AssertUtil.assertTrue(1 == 0, "Numbers do not match");
            });
    assertEquals("JDBC driver internal error: Numbers do not match.", e.getMessage());
  }

  /** Test that Constants.getOS function is working as expected */
  @Test
  @DontRunOnGithubActions
  public void testgetOS() {
    Constants.clearOSForTesting();
    String originalOS = systemGetProperty("os.name");
    System.setProperty("os.name", "Windows");
    assertEquals(Constants.OS.WINDOWS, Constants.getOS());
    Constants.clearOSForTesting();
    System.setProperty("os.name", "Linux");
    assertEquals(Constants.OS.LINUX, Constants.getOS());
    Constants.clearOSForTesting();
    System.setProperty("os.name", "Macintosh");
    assertEquals(Constants.OS.MAC, Constants.getOS());
    Constants.clearOSForTesting();
    System.setProperty("os.name", "Sunos");
    assertEquals(Constants.OS.SOLARIS, Constants.getOS());
    // Set back to initial value at end of test
    Constants.clearOSForTesting();
    System.setProperty("os.name", originalOS);
  }

  @Test
  public void testHttpClientSettingsKey() {
    // Create 2 identical HTTPClientKeys with different nonProxyHost settings
    HttpClientSettingsKey testKey1 =
        new HttpClientSettingsKey(
            OCSPMode.FAIL_OPEN,
            "snowflakecomputing.com",
            443,
            "*.foo.com",
            "testuser",
            "pw",
            "https",
            "jdbc",
            false);
    HttpClientSettingsKey testKey2 =
        new HttpClientSettingsKey(
            OCSPMode.FAIL_OPEN,
            "snowflakecomputing.com",
            443,
            "*.baz.com",
            "testuser",
            "pw",
            "https",
            "jdbc",
            false);
    // Create an HTTPClientKey with all default settings
    HttpClientSettingsKey testKey3 = new HttpClientSettingsKey(OCSPMode.FAIL_CLOSED, "jdbc", false);
    // Assert the first 2 test keys are equal
    assertTrue(testKey1.equals(testKey2));
    // Assert that testKey2 has its non proxy hosts updated by the equals function
    assertEquals("*.foo.com", testKey2.getNonProxyHosts());
    // Assert that the test key with the default options is different from the others
    assertFalse(testKey1.equals(testKey3));
  }

  @Test
  public void testSetProxyForS3() {
    HttpClientSettingsKey testKey =
        new HttpClientSettingsKey(
            OCSPMode.FAIL_OPEN,
            "snowflakecomputing.com",
            443,
            "*.foo.com",
            "testuser",
            "pw",
            "https",
            "jdbc",
            false);
    ProxyConfiguration proxyConfiguration = S3HttpUtil.createProxyConfigurationForS3(testKey);
    assertEquals(HttpProtocol.HTTPS.toString(), proxyConfiguration.scheme().toUpperCase());
    assertEquals("snowflakecomputing.com", proxyConfiguration.host());
    assertEquals(443, proxyConfiguration.port());
    assertEquals(
        new HashSet<>(Collections.singletonList("\\Q\\E.*\\Q.foo.com\\E")),
        proxyConfiguration.nonProxyHosts());
    assertEquals("pw", proxyConfiguration.password());
    assertEquals("testuser", proxyConfiguration.username());
  }

  @Test
  public void testSetSessionlessProxyForS3() throws SnowflakeSQLException {
    Properties props = new Properties();
    props.put("useProxy", "true");
    props.put("proxyHost", "localhost");
    props.put("proxyPort", "8084");
    props.put("proxyUser", "testuser");
    props.put("proxyPassword", "pw");
    props.put("nonProxyHosts", "baz.com | foo.com");
    props.put("proxyProtocol", "http");
    ProxyConfiguration proxyConfiguration =
        S3HttpUtil.createSessionlessProxyConfigurationForS3(props);
    assertEquals(HttpProtocol.HTTP.toString(), proxyConfiguration.scheme().toUpperCase());
    assertEquals("localhost", proxyConfiguration.host());
    assertEquals(8084, proxyConfiguration.port());
    assertEquals(
        new HashSet<>(Arrays.asList("\\Qbaz.com\\E", "\\Qfoo.com\\E")),
        proxyConfiguration.nonProxyHosts());
    assertEquals("pw", proxyConfiguration.password());
    assertEquals("testuser", proxyConfiguration.username());
    // Test that exception is thrown when port number is invalid
    props.put("proxyPort", "invalidnumber");
    SnowflakeSQLException e =
        assertThrows(
            SnowflakeSQLException.class,
            () -> {
              S3HttpUtil.createSessionlessProxyConfigurationForS3(props);
            });
    assertEquals((int) ErrorCode.INVALID_PROXY_PROPERTIES.getMessageCode(), e.getErrorCode());
  }

  @Test
  public void testSetProxyForAzure() {
    HttpClientSettingsKey testKey =
        new HttpClientSettingsKey(
            OCSPMode.FAIL_OPEN,
            "snowflakecomputing.com",
            443,
            "*.foo.com",
            "testuser",
            "pw",
            "https",
            "jdbc",
            false);
    ProxyOptions proxyOptions = HttpUtil.createProxyOptionsForAzure(testKey);
    assertEquals(ProxyOptions.Type.HTTP, proxyOptions.getType());
    assertEquals(new InetSocketAddress("snowflakecomputing.com", 443), proxyOptions.getAddress());
    assertEquals("testuser", proxyOptions.getUsername());
    assertEquals("pw", proxyOptions.getPassword());
    assertEquals("(.*?\\.foo\\.com)", proxyOptions.getNonProxyHosts());
  }

  @Test
  public void testSetSessionlessProxyForAzure() throws SnowflakeSQLException {
    Properties props = new Properties();
    props.put("useProxy", "true");
    props.put("proxyHost", "localhost");
    props.put("proxyPort", "8084");
    props.put("proxyUser", "testuser");
    props.put("proxyPassword", "pw");
    props.put("nonProxyHosts", "*");
    ProxyOptions proxyOptions = HttpUtil.createSessionlessProxyOptionsForAzure(props);
    assertEquals(ProxyOptions.Type.HTTP, proxyOptions.getType());
    assertEquals(new InetSocketAddress("localhost", 8084), proxyOptions.getAddress());
    assertEquals("testuser", proxyOptions.getUsername());
    assertEquals("pw", proxyOptions.getPassword());
    assertEquals("(.*?)", proxyOptions.getNonProxyHosts());
    // Test that exception is thrown when port number is invalid
    props.put("proxyPort", "invalidnumber");
    SnowflakeSQLException e =
        assertThrows(
            SnowflakeSQLException.class,
            () -> {
              HttpUtil.createSessionlessProxyOptionsForAzure(props);
            });
    assertEquals((int) ErrorCode.INVALID_PROXY_PROPERTIES.getMessageCode(), e.getErrorCode());
  }

  @Test
  public void testSizeOfHttpClientMapWithVariableNonProxyHosts() {
    // clear httpClient hashmap before test
    HttpUtil.httpClient = new HashMap<>();
    // Clear client route planner hashmap before test
    HttpUtil.httpClientRoutePlanner = new HashMap<>();
    HttpClientSettingsKey key1 =
        new HttpClientSettingsKey(
            null,
            "localhost",
            8080,
            "google.com | baz.com",
            "testuser",
            "pw",
            "https",
            "jdbc",
            false);
    // Assert there is 1 entry in the hashmap now
    HttpUtil.getHttpClient(key1);
    assertEquals(1, HttpUtil.httpClient.size());
    HttpClientSettingsKey key2 =
        new HttpClientSettingsKey(
            null, "localhost", 8080, "snowflake.com", "testuser", "pw", "https", "jdbc", false);
    HttpUtil.getHttpClient(key2);
    // Assert there is still 1 entry because key is re-used when only proxy difference is
    // nonProxyHosts
    assertEquals(1, HttpUtil.httpClient.size());
    // Assert previous key has updated non-proxy hosts
    assertEquals("snowflake.com", key1.getNonProxyHosts());
    HttpClientSettingsKey key3 =
        new HttpClientSettingsKey(
            null,
            "differenthost.com",
            8080,
            "snowflake.com",
            "testuser",
            "pw",
            "https",
            "jdbc",
            false);
    // Assert proxy with different host generates new entry in httpClient map
    HttpUtil.getHttpClient(key3);
    assertEquals(2, HttpUtil.httpClient.size());
  }

  @Test
  public void testSizeOfHttpClientMapWithGzipAndUserAgentSuffix() {
    // clear httpClient hashmap before test
    HttpUtil.httpClient = new HashMap<>();
    HttpClientSettingsKey key1 =
        new HttpClientSettingsKey(
            null,
            "localhost",
            8080,
            "google.com | baz.com",
            "testuser",
            "pw",
            "https",
            "jdbc",
            false);
    // Assert there is 1 entry in the hashmap now
    HttpUtil.getHttpClient(key1);
    assertEquals(1, HttpUtil.httpClient.size());
    HttpClientSettingsKey key2 =
        new HttpClientSettingsKey(
            null,
            "localhost",
            8080,
            "google.com | baz.com",
            "testuser",
            "pw",
            "https",
            "jdbc",
            true);
    HttpUtil.getHttpClient(key2);
    // Assert there are 2 entries because gzip has changed
    assertEquals(2, HttpUtil.httpClient.size());
    HttpClientSettingsKey key3 =
        new HttpClientSettingsKey(
            null,
            "localhost",
            8080,
            "google.com | baz.com",
            "testuser",
            "pw",
            "https",
            "odbc",
            true);
    HttpUtil.getHttpClient(key3);
    // Assert there are 3 entries because userAgentSuffix has changed
    assertEquals(3, HttpUtil.httpClient.size());
  }

  @Test
  public void testSdkProxyRoutePlannerNonProxyHostsBypassesProxy() throws Exception {
    SdkProxyRoutePlanner planner =
        new SdkProxyRoutePlanner(
            "proxy.example.com", 8080, HttpProtocol.HTTP, "*.internal.com|localhost");
    // Hosts matching nonProxyHosts should bypass proxy (determineProxy returns null)
    org.apache.http.HttpHost internalHost = new org.apache.http.HttpHost("app.internal.com");
    org.apache.http.HttpHost localhostHost = new org.apache.http.HttpHost("localhost");
    org.apache.http.HttpHost externalHost = new org.apache.http.HttpHost("external.com");

    assertNull(planner.determineProxy(internalHost, null, null));
    assertNull(planner.determineProxy(localhostHost, null, null));
    assertNotNull(planner.determineProxy(externalHost, null, null));
  }

  @Test
  public void testSdkProxyRoutePlannerReDoSPatternDoesNotHang() throws Exception {
    // The exact ReDoS payload from the vulnerability report
    SdkProxyRoutePlanner planner =
        new SdkProxyRoutePlanner("proxy.example.com", 8080, HttpProtocol.HTTP, "(a+)+");
    String maliciousHost =
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaac";
    org.apache.http.HttpHost target = new org.apache.http.HttpHost(maliciousHost);

    long start = System.currentTimeMillis();
    assertNotNull(planner.determineProxy(target, null, null));
    long elapsed = System.currentTimeMillis() - start;
    assertTrue(
        elapsed < 1000,
        "Non-proxy host matching should complete nearly instantly, took " + elapsed + "ms");
  }

  @Test
  public void testConvertProxyPropertiesToHttpClientKey() throws SnowflakeSQLException {
    OCSPMode mode = OCSPMode.FAIL_OPEN;
    Properties props = new Properties();
    HttpClientSettingsKey expectedNoProxy = new HttpClientSettingsKey(mode);

    // Test for null proxy properties
    HttpClientSettingsKey settingsKey =
        SnowflakeUtil.convertProxyPropertiesToHttpClientKey(mode, props);
    assertTrue(expectedNoProxy.equals(settingsKey));

    // Set useProxy to false so proxy properties will not be set
    props.put("useProxy", "false");
    props.put("proxyHost", "localhost");
    props.put("proxyPort", "8084");
    settingsKey = SnowflakeUtil.convertProxyPropertiesToHttpClientKey(mode, props);
    assertTrue(expectedNoProxy.equals(settingsKey));

    // Test without gzip_disabled
    props.put("useProxy", "true");
    props.put("proxyHost", "localhost");
    props.put("proxyPort", "8084");
    props.put("proxyUser", "testuser");
    props.put("proxyPassword", "pw");
    props.put("nonProxyHosts", "baz.com | foo.com");
    props.put("proxyProtocol", "http");
    props.put("user_agent_suffix", "jdbc");
    settingsKey = SnowflakeUtil.convertProxyPropertiesToHttpClientKey(mode, props);
    HttpClientSettingsKey expectedWithProxy =
        new HttpClientSettingsKey(
            OCSPMode.FAIL_OPEN,
            "localhost",
            8084,
            "baz.com | foo.com",
            "testuser",
            "pw",
            "http",
            "jdbc",
            Boolean.valueOf(false));
    assertTrue(expectedWithProxy.equals(settingsKey));

    // Test with gzip_disabled
    props.put("gzipDisabled", "true");
    settingsKey = SnowflakeUtil.convertProxyPropertiesToHttpClientKey(mode, props);
    expectedWithProxy =
        new HttpClientSettingsKey(
            OCSPMode.FAIL_OPEN,
            "localhost",
            8084,
            "baz.com | foo.com",
            "testuser",
            "pw",
            "http",
            "jdbc",
            Boolean.valueOf(true));
    assertTrue(expectedWithProxy.equals(settingsKey));

    // Test that exception is thrown when port number is invalid
    props.put("proxyPort", "invalidnumber");
    SnowflakeSQLException e =
        assertThrows(
            SnowflakeSQLException.class,
            () -> {
              SnowflakeUtil.convertProxyPropertiesToHttpClientKey(mode, props);
            });
    assertEquals((int) ErrorCode.INVALID_PROXY_PROPERTIES.getMessageCode(), e.getErrorCode());
  }

  @Test
  public void testNullAndEmptyProxySettingsForS3() {
    HttpClientSettingsKey testKey =
        new HttpClientSettingsKey(OCSPMode.FAIL_OPEN, null, 443, null, null, null, "", "", false);
    ProxyConfiguration proxyConfiguration = S3HttpUtil.createProxyConfigurationForS3(testKey);
    assertEquals(HttpProtocol.HTTP.toString(), proxyConfiguration.scheme().toUpperCase());
    assertEquals("", proxyConfiguration.host());
    assertEquals(443, proxyConfiguration.port());
    assertEquals(Collections.emptySet(), proxyConfiguration.nonProxyHosts());
    assertNull(proxyConfiguration.username());
    assertNull(proxyConfiguration.password());
  }
}
