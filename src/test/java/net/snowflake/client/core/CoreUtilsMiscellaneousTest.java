/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.core;

import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;
import static org.junit.Assert.*;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.microsoft.azure.storage.OperationContext;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.Properties;
import net.snowflake.client.ConditionalIgnoreRule;
import net.snowflake.client.RunningOnGithubAction;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import org.junit.Test;

public class CoreUtilsMiscellaneousTest {

  /**
   * Assert that the AssertUtil.AssertTrue statement issues the correct SFException when conditions
   * are not met
   */
  @Test
  public void testSnowflakeAssertTrue() {
    try {
      AssertUtil.assertTrue(1 == 0, "Numbers do not match");
    } catch (SFException e) {
      assertEquals("JDBC driver internal error: Numbers do not match.", e.getMessage());
    }
  }

  /** Test that Constants.getOS function is working as expected */
  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
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
            "jdbc");
    HttpClientSettingsKey testKey2 =
        new HttpClientSettingsKey(
            OCSPMode.FAIL_OPEN,
            "snowflakecomputing.com",
            443,
            "*.baz.com",
            "testuser",
            "pw",
            "https",
            "jdbc");
    // Create an HTTPClientKey with all default settings
    HttpClientSettingsKey testKey3 = new HttpClientSettingsKey(OCSPMode.FAIL_CLOSED, "jdbc");
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
            "jdbc");
    ClientConfiguration clientConfig = new ClientConfiguration();
    HttpUtil.setProxyForS3(testKey, clientConfig);
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
    HttpUtil.setSessionlessProxyForS3(props, clientConfig);
    assertEquals(Protocol.HTTP, clientConfig.getProxyProtocol());
    assertEquals("localhost", clientConfig.getProxyHost());
    assertEquals(8084, clientConfig.getProxyPort());
    assertEquals("baz.com | foo.com", clientConfig.getNonProxyHosts());
    assertEquals("pw", clientConfig.getProxyPassword());
    assertEquals("testuser", clientConfig.getProxyUsername());
    // Test that exception is thrown when port number is invalid
    props.put("proxyPort", "invalidnumber");
    try {
      HttpUtil.setSessionlessProxyForS3(props, clientConfig);
    } catch (SnowflakeSQLException e) {
      assertEquals((int) ErrorCode.INVALID_PROXY_PROPERTIES.getMessageCode(), e.getErrorCode());
    }
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
            "jdbc");
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
    try {
      HttpUtil.setSessionlessProxyForAzure(props, op);
    } catch (SnowflakeSQLException e) {
      assertEquals((int) ErrorCode.INVALID_PROXY_PROPERTIES.getMessageCode(), e.getErrorCode());
    }
  }
}
