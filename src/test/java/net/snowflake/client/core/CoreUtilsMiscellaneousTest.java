package net.snowflake.client.core;

import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.microsoft.azure.storage.OperationContext;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.HashMap;
import java.util.Properties;
import net.snowflake.client.annotations.DontRunOnGithubActions;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.SnowflakeUtil;
import net.snowflake.client.jdbc.cloud.storage.S3HttpUtil;
import org.junit.jupiter.api.Test;

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
    ClientConfiguration clientConfig = new ClientConfiguration();
    S3HttpUtil.setProxyForS3(testKey, clientConfig);
    assertEquals(Protocol.HTTPS, clientConfig.getProxyProtocol());
    assertEquals("snowflakecomputing.com", clientConfig.getProxyHost());
    assertEquals(443, clientConfig.getProxyPort());
    assertEquals("*.foo.com", clientConfig.getNonProxyHosts());
    assertEquals("pw", clientConfig.getProxyPassword());
    assertEquals("testuser", clientConfig.getProxyUsername());
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
    ClientConfiguration clientConfig = new ClientConfiguration();
    S3HttpUtil.setSessionlessProxyForS3(props, clientConfig);
    assertEquals(Protocol.HTTP, clientConfig.getProxyProtocol());
    assertEquals("localhost", clientConfig.getProxyHost());
    assertEquals(8084, clientConfig.getProxyPort());
    assertEquals("baz.com | foo.com", clientConfig.getNonProxyHosts());
    assertEquals("pw", clientConfig.getProxyPassword());
    assertEquals("testuser", clientConfig.getProxyUsername());
    // Test that exception is thrown when port number is invalid
    props.put("proxyPort", "invalidnumber");
    SnowflakeSQLException e =
        assertThrows(
            SnowflakeSQLException.class,
            () -> {
              S3HttpUtil.setSessionlessProxyForS3(props, clientConfig);
            });
    assertEquals((int) ErrorCode.INVALID_PROXY_PROPERTIES.getMessageCode(), e.getErrorCode());
  }

  @Test
  public void testSetProxyForAzure() {
    OperationContext op = new OperationContext();
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
    HttpUtil.setProxyForAzure(testKey, op);
    Proxy proxy = op.getProxy();
    assertEquals(Proxy.Type.HTTP, proxy.type());
    assertEquals(new InetSocketAddress("snowflakecomputing.com", 443), proxy.address());
  }

  @Test
  public void testSetSessionlessProxyForAzure() throws SnowflakeSQLException {
    Properties props = new Properties();
    props.put("useProxy", "true");
    props.put("proxyHost", "localhost");
    props.put("proxyPort", "8084");
    OperationContext op = new OperationContext();
    HttpUtil.setSessionlessProxyForAzure(props, op);
    Proxy proxy = op.getProxy();
    assertEquals(Proxy.Type.HTTP, proxy.type());
    assertEquals(new InetSocketAddress("localhost", 8084), proxy.address());
    // Test that exception is thrown when port number is invalid
    props.put("proxyPort", "invalidnumber");
    SnowflakeSQLException e =
        assertThrows(
            SnowflakeSQLException.class,
            () -> {
              HttpUtil.setSessionlessProxyForAzure(props, op);
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
    ClientConfiguration clientConfig = new ClientConfiguration();
    S3HttpUtil.setProxyForS3(testKey, clientConfig);
    assertEquals(Protocol.HTTP, clientConfig.getProxyProtocol());
    assertEquals("", clientConfig.getProxyHost());
    assertEquals(443, clientConfig.getProxyPort());
    assertEquals("", clientConfig.getNonProxyHosts());
    assertNull(clientConfig.getProxyUsername());
    assertNull(clientConfig.getProxyPassword());
  }
}
