/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/** Driver unit test */
public class SnowflakeDriverTest {
  class TestCase {
    String url;
    String scheme;
    String host;
    int port;
    String account;
    Map<String, String> parameters;

    TestCase(
        String url,
        String scheme,
        String host,
        int port,
        String account,
        Map<String, String> parameters) {
      this.url = url;
      this.scheme = scheme;
      this.host = host;
      this.port = port;
      this.account = account;
      this.parameters = parameters;
    }

    void match(String url, SnowflakeConnectString sc) {
      String scheme = sc.getScheme();
      String host = sc.getHost();
      int port = sc.getPort();
      Map<String, Object> parameters = sc.getParameters();

      assertEquals(this.scheme, scheme, "URL scheme: " + url);
      assertEquals(this.host, host, "URL scheme: " + url);
      assertEquals(this.port, port, "URL scheme: " + url);
      assertEquals(this.parameters.size(), parameters.size(), "URL scheme: " + url);
      assertEquals(this.account, account, "URL scheme. " + url);

      for (Map.Entry<String, String> entry : this.parameters.entrySet()) {
        String k = entry.getKey().toUpperCase(Locale.US);
        Object v = parameters.get(k);
        assertEquals(entry.getValue(), v, "URL scheme: " + url + ", key: " + k);
      }
    }
  }

  private Map<String, String> EMPTY_PARAMETERS = Collections.emptyMap();

  @Test
  public void testAcceptUrls() throws Exception {
    SnowflakeDriver snowflakeDriver = SnowflakeDriver.INSTANCE;

    Map<String, String> expectedParameters;

    List<TestCase> testCases = new ArrayList<>();
    expectedParameters = new HashMap<>();
    expectedParameters.put("prop1", "value1");
    expectedParameters.put("ACCOUNT", "testaccount");
    testCases.add(
        new TestCase(
            "jdbc:snowflake://http://testaccount.localhost?prop1=value1",
            "http",
            "testaccount.localhost",
            443,
            "testaccount",
            expectedParameters));

    // value including dash
    expectedParameters = new HashMap<>();
    expectedParameters.put("CLIENT_SESSION_KEEP_ALIVE", Boolean.TRUE.toString());
    expectedParameters.put("db", "TEST_DB");
    expectedParameters.put("proxyHost", "your-host.com");
    expectedParameters.put("proxyPort", "1234");
    expectedParameters.put("schema", "PUBLIC");
    expectedParameters.put("useProxy", Boolean.TRUE.toString());
    expectedParameters.put("warehouse", "TEST_WH");
    expectedParameters.put("ACCOUNT", "testaccount");
    testCases.add(
        new TestCase(
            "jdbc:snowflake://testaccount.snowflakecomputing.com:443"
                + "?CLIENT_SESSION_KEEP_ALIVE=true&db=TEST_DB&proxyHost=your-host.com&proxyPort=1234"
                + "&schema=PUBLIC&useProxy=true&warehouse=TEST_WH",
            "https",
            "testaccount.snowflakecomputing.com",
            443,
            "testaccount",
            expectedParameters));

    // value including dot
    expectedParameters = new HashMap<>();
    expectedParameters.put("proxyHost", "72.1.32.55");
    expectedParameters.put("proxyPort", "portU");
    expectedParameters.put("ACCOUNT", "testaccount");
    testCases.add(
        new TestCase(
            "jdbc:snowflake://testaccount.snowflakecomputing.com"
                + "?proxyHost=72.1.32.55&proxyPort=port%55",
            "https", "testaccount.snowflakecomputing.com", 443, "testaccount", expectedParameters));

    // value in escaped value
    expectedParameters = new HashMap<>();
    expectedParameters.put("proxyHost", "=/");
    expectedParameters.put("proxyPort", "777");
    expectedParameters.put("ssl", "off");
    expectedParameters.put("account", "testaccount");
    testCases.add(
        new TestCase(
            "jdbc:snowflake://testaccount.com:8080?proxyHost=%3d%2f&proxyPort=777&ssl=off",
            "http", "testaccount.com", 8080, null, expectedParameters));

    // value including non ascii characters
    expectedParameters = new HashMap<>();
    expectedParameters.put("proxyHost", "cheese");
    expectedParameters.put("proxyPort", "!@");
    expectedParameters.put("account", "testaccount");
    testCases.add(
        new TestCase(
            "jdbc:snowflake://testaccount.com:8080?proxyHost=cheese&proxyPort=!@",
            "https",
            "testaccount.com",
            8080,
            null,
            expectedParameters));

    // value including non ascii characters
    expectedParameters = new HashMap<>();
    expectedParameters.put("proxyHost", "cheese");
    expectedParameters.put("proxyPort", "cake");
    expectedParameters.put("account", "testaccount");
    testCases.add(
        new TestCase(
            "jdbc:snowflake://https://testaccount.com:8080?proxyHost=cheese&proxyPort=cake",
            "https",
            "testaccount.com",
            8080,
            null,
            expectedParameters));

    // host including underscore
    expectedParameters = new HashMap<>();
    expectedParameters.put("ACCOUNT", "snowflake");
    testCases.add(
        new TestCase(
            "jdbc:snowflake://snowflake.reg-RT-Replication1-7387_2.local:8082",
            "https",
            "snowflake.reg-RT-Replication1-7387_2.local",
            8082,
            "snowflake",
            expectedParameters));

    // host including underscore with parameters
    expectedParameters = new HashMap<>();
    expectedParameters.put("db", "testdb");
    expectedParameters.put("schema", "testschema");
    expectedParameters.put("ACCOUNT", "snowflake");
    testCases.add(
        new TestCase(
            "jdbc:snowflake://snowflake.reg-RT-Replication1-7387_2.local:8082"
                + "?db=testdb&schema=testschema",
            "https",
            "snowflake.reg-RT-Replication1-7387_2.local",
            8082,
            "snowflake",
            expectedParameters));

    // host including underscore with parameters after a path slash
    expectedParameters = new HashMap<>();
    expectedParameters.put("db", "testdb");
    expectedParameters.put("schema", "testschema");
    expectedParameters.put("ACCOUNT", "snowflake");
    testCases.add(
        new TestCase(
            "jdbc:snowflake://snowflake.reg-RT-Replication1-7387_2.local:8082/"
                + "?db=testdb&schema=testschema",
            "https",
            "snowflake.reg-RT-Replication1-7387_2.local",
            8082,
            "snowflake",
            expectedParameters));

    // host including underscore with parameters after a path slash
    expectedParameters = new HashMap<>();
    expectedParameters.put("ACCOUNT", "testaccount");
    expectedParameters.put("authenticator", "https://snowflakecomputing.okta.com");
    testCases.add(
        new TestCase(
            "jdbc:snowflake://testaccount.snowflakecomputing.com/"
                + "?authenticator=https://snowflakecomputing.okta.com",
            "https",
            "testaccount.snowflakecomputing.com",
            443,
            "testaccount",
            expectedParameters));

    // localhost without port
    expectedParameters = new HashMap<>();
    expectedParameters.put("ACCOUNT", "testaccount");
    testCases.add(
        new TestCase(
            "jdbc:snowflake://testaccount.com:",
            "https",
            "testaccount.com",
            443,
            null,
            expectedParameters));

    expectedParameters = new HashMap<>();
    expectedParameters.put(
        "authenticator",
        "https://snowflakecomputing.okta.com/app/template_saml_2_0/ky7gy61iAOAMLLSOZSVX/sso/saml");
    expectedParameters.put("account", "testaccount");
    expectedParameters.put("user", "qa");
    expectedParameters.put("ssl", "off");
    expectedParameters.put("db", "testdb");
    expectedParameters.put("schema", "testschema");
    expectedParameters.put("networkTimeout", "3600");
    expectedParameters.put("retryQuery", "on");
    testCases.add(
        new TestCase(
            "jdbc:snowflake://snowflake.reg.local:8082/"
                + "?account=testaccount"
                + "&authenticator=https://snowflakecomputing.okta.com/app/template_saml_2_0/ky7gy61iAOAMLLSOZSVX/sso/saml"
                + "&user=qa&ssl=off"
                + "&schema=testschema&db=testdb&networkTimeout=3600&retryQuery=on",
            "http",
            "snowflake.reg.local",
            8082,
            "testaccount",
            expectedParameters));

    // invalid key value pair including multiple equal signs
    expectedParameters = new HashMap<>();
    expectedParameters.put("ACCOUNT", "testaccount");
    expectedParameters.put("proxyPort", "781");
    testCases.add(
        new TestCase(
            "jdbc:snowflake://testaccount.com?proxyHost=puppy==dog&proxyPort=781",
            "https",
            "testaccount.com",
            443,
            "testaccount",
            expectedParameters));

    // invalid key value including multiple amp
    expectedParameters = new HashMap<>();
    expectedParameters.put("ACCOUNT", "testaccount");
    expectedParameters.put("proxyPort", "5");
    testCases.add(
        new TestCase(
            "jdbc:snowflake://testaccount.com?proxyHost=&&&proxyPort=5",
            "https",
            "testaccount.com",
            443,
            "testaccount",
            expectedParameters));

    // NOT global url
    expectedParameters = new HashMap<>();
    expectedParameters.put("ACCOUNT", "globalaccount-12345");
    expectedParameters.put("proxyPort", "45.12.34.5");
    testCases.add(
        new TestCase(
            "jdbc:snowflake://globalaccount-12345.com?proxyHost=&&&proxyPort=45.12.34.5",
            "https",
            "globalaccount-12345.com",
            443,
            "globalaccount-12345",
            expectedParameters));

    // global url
    expectedParameters = new HashMap<>();
    expectedParameters.put("ACCOUNT", "globalaccount");
    expectedParameters.put("proxyPort", "45-12-34-5");
    testCases.add(
        new TestCase(
            "jdbc:snowflake://globalaccount-12345.global.snowflakecomputing.com"
                + "?proxyHost=&&&proxyPort=45-12-34-5",
            "https",
            "globalaccount-12345.global.snowflakecomputing.com",
            443,
            "globalaccount",
            expectedParameters));

    // rt-language1
    expectedParameters = new HashMap<>();
    expectedParameters.put("ACCOUNT", "snowflake");
    expectedParameters.put("user", "admin");
    expectedParameters.put("networkTimeout", "3600");
    expectedParameters.put("retryQuery", "on");
    expectedParameters.put("ssl", "off");
    testCases.add(
        new TestCase(
            "jdbc:snowflake://snowflake.reg-RT-PC-Language1-10850.local:8082/"
                + "?account=snowflake&user=admin&ssl=off&networkTimeout=3600&retryQuery=on",
            "http",
            "snowflake.reg-RT-PC-Language1-10850.local",
            8082,
            "snowflake",
            expectedParameters));

    // rt-language1 with account parameter
    expectedParameters = new HashMap<>();
    expectedParameters.put("ACCOUNT", "testaccount100");
    expectedParameters.put("user", "admin");
    expectedParameters.put("networkTimeout", "3600");
    expectedParameters.put("retryQuery", "on");
    expectedParameters.put("ssl", "off");
    testCases.add(
        new TestCase(
            "jdbc:snowflake://snowflake.reg-RT-PC-Language1-10850.local:8082/"
                + "?account=testaccount100&user=admin&ssl=off&networkTimeout=3600&retryQuery=on",
            "http",
            "snowflake.reg-RT-PC-Language1-10850.local",
            8082,
            "snowflake",
            expectedParameters));

    // first_t_single
    expectedParameters = new HashMap<>();
    expectedParameters.put("ACCOUNT", "cutoff_ds_consumer");
    expectedParameters.put("user", "snowman");
    expectedParameters.put("tracing", "off");
    expectedParameters.put("retryQuery", "on");
    expectedParameters.put("ssl", "off");
    expectedParameters.put("password", "test");
    testCases.add(
        new TestCase(
            "jdbc:snowflake://10.180.189.160:7510/?account=cutoff_ds_consumer&user=snowman&password=test&tracing=off&ssl=off&retryQuery=on",
            "http",
            "10.180.189.160",
            7510,
            "cutoff_ds_consumer",
            expectedParameters));

    for (TestCase t : testCases) {
      assertTrue(snowflakeDriver.acceptsURL(t.url), "URL is not valid: " + t.url);
      t.match(t.url, SnowflakeConnectString.parse(t.url, SnowflakeDriver.EMPTY_PROPERTIES));
    }

    // negative tests
    assertFalse(snowflakeDriver.acceptsURL("jdbc:"));
    assertFalse(snowflakeDriver.acceptsURL("jdbc:snowflake://"));
    assertFalse(snowflakeDriver.acceptsURL("jdbc:snowflake://:"));
    assertFalse(snowflakeDriver.acceptsURL("jdbc:snowflake://:8080"));
    assertFalse(snowflakeDriver.acceptsURL("jdbc:snowflake://localhost:xyz"));
    assertFalse(snowflakeDriver.acceptsURL("jdbc:snowflak://localhost:8080"));
    assertFalse(snowflakeDriver.acceptsURL("jdbc:snowflake://localhost:8080/a=b"));
    assertFalse(snowflakeDriver.acceptsURL("jdbc:snowflake://testaccount.com?proxyHost=%%"));
    assertFalse(
        snowflakeDriver.acceptsURL("jdbc:snowflake://testaccount.com?proxyHost=%b&proxyPort="));
    assertFalse(snowflakeDriver.acceptsURL("jdbc:mysql://localhost:3306/dbname"));
  }

  @Test
  public void testInvalidNullConnect() {
    SnowflakeDriver snowflakeDriver = SnowflakeDriver.INSTANCE;
    try {
      snowflakeDriver.connect(/* url= */ null, null);
      fail();
    } catch (SQLException ex) {
      assertEquals("Unable to connect to url of 'null'.", ex.getMessage());
    }
  }

  @Test
  public void testGetVersion() {
    SnowflakeDriver snowflakeDriver = SnowflakeDriver.INSTANCE;

    int majorVersion = snowflakeDriver.getMajorVersion();
    int minorVersion = snowflakeDriver.getMinorVersion();

    assertThat(majorVersion, greaterThanOrEqualTo(3));
    assertThat(minorVersion, greaterThanOrEqualTo(0));
  }

  @Test
  public void testJDBCCompliant() {
    SnowflakeDriver snowflakeDriver = SnowflakeDriver.INSTANCE;
    assertFalse(snowflakeDriver.jdbcCompliant());
  }

  @Test
  public void testGetParentLogger() throws SQLException {
    SnowflakeDriver snowflakeDriver = SnowflakeDriver.INSTANCE;
    assertNull(snowflakeDriver.getParentLogger());
  }

  @Test
  public void testMain() {
    // Can't get version information during test phase
    // Just make sure this function won't break
    SnowflakeDriver snowflakeDriver = SnowflakeDriver.INSTANCE;
    String[] args = {"--version"};
    snowflakeDriver.main(args);
  }

  @Test
  public void testSuppressIllegalReflectiveAccessWarning() {
    // Just to make sure this function won't break anything
    SnowflakeDriver.disableIllegalReflectiveAccessWarning();
  }

  @Test
  public void testParseConnectStringException() {
    SnowflakeDriver snowflakeDriver = SnowflakeDriver.INSTANCE;
    Properties info = new Properties();
    String jdbcConnectString =
        "jdbc:snowflake://abc-test.us-east-1.snowflakecomputing.com/?private_key_file=C:\\temp\\rsa_key.p8&private_key_pwd=test_password&user=test_user";
    try {
      snowflakeDriver.connect(jdbcConnectString, info);
      fail();
    } catch (Exception ex) {
      assertEquals("Connection string is invalid. Unable to parse.", ex.getMessage());
    }
  }

  @Test
  public void testReturnsNullForOtherJdbcConnectString() throws SQLException {
    SnowflakeDriver snowflakeDriver = SnowflakeDriver.INSTANCE;
    Properties info = new Properties();
    String jdbcConnectString = "jdbc:mysql://host:port/database";
    assertNull(snowflakeDriver.connect(jdbcConnectString, info));
  }

  @Test
  public void testConnectWithMissingAccountIdentifier() throws SQLException {
    SnowflakeDriver snowflakeDriver = SnowflakeDriver.INSTANCE;
    try {
      snowflakeDriver.getPropertyInfo("jdbc:snowflake://localhost:443/?&ssl=on", new Properties());
      fail();
    } catch (SnowflakeSQLException ex) {
      assertEquals(
          "Invalid Connect String: jdbc:snowflake://localhost:443/?&ssl=on.", ex.getMessage());
    }
  }
}
