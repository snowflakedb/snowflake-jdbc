/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Driver unit test
 */
public class SnowflakeDriverTest
{
  class TestCase
  {
    String url;
    String scheme;
    String host;
    int port;
    Map<String, String> parameters;

    TestCase(String url, String scheme, String host, int port, Map<String, String> parameters)
    {
      this.url = url;
      this.scheme = scheme;
      this.host = host;
      this.port = port;
      this.parameters = parameters;
    }

    void match(String url, SnowflakeConnectString sc)
    {
      String scheme = sc.getScheme();
      String host = sc.getHost();
      int port = sc.getPort();
      Map<String, Object> parameters = sc.getParameters();

      assertEquals("URL scheme: " + url, this.scheme, scheme);
      assertEquals("URL scheme: " + url, this.host, host);
      assertEquals("URL scheme: " + url, this.port, port);
      assertEquals("URL scheme: " + url, this.parameters.size(), parameters.size());

      for (Map.Entry<String, String> entry : this.parameters.entrySet())
      {
        String k = entry.getKey().toUpperCase(Locale.US);
        Object v = parameters.get(k);
        assertEquals("URL scheme: " + url + ", key: " + k, entry.getValue(), v);
      }
    }
  }

  private Map<String, String> EMPTY_PARAMETERS = Collections.emptyMap();


  @Test
  public void testAcceptUrls() throws Exception
  {
    SnowflakeDriver snowflakeDriver = SnowflakeDriver.INSTANCE;

    Map<String, String> expectedParameters;

    List<TestCase> testCases = new ArrayList<>();
    testCases.add(
        new TestCase(
            "jdbc:snowflake://localhost",
            "https", "localhost", 443, EMPTY_PARAMETERS));
    testCases.add(
        new TestCase(
            "jdbc:snowflake://localhost:8081",
            "https", "localhost", 8081, EMPTY_PARAMETERS));

    expectedParameters = new HashMap<>();
    expectedParameters.put("a", "b");
    testCases.add(
        new TestCase(
            "jdbc:snowflake://localhost8081?a=b",
            "https", "localhost8081", 443, expectedParameters));

    expectedParameters = new HashMap<>();
    expectedParameters.put("a", "b");
    testCases.add(
        new TestCase(
            "jdbc:snowflake://localhost:8081/?a=b",
            "https", "localhost", 8081, expectedParameters));

    expectedParameters = new HashMap<>();
    expectedParameters.put("a", "b");
    expectedParameters.put("c", "d");
    testCases.add(
        new TestCase(
            "jdbc:snowflake://localhost:8081?a=b&c=d",
            "https", "localhost", 8081, expectedParameters));

    expectedParameters = new HashMap<>();
    expectedParameters.put("a", "b");
    expectedParameters.put("c", "d");
    testCases.add(
        new TestCase(
            "jdbc:snowflake://localhost:8081/?a=b&c=d",
            "https", "localhost", 8081, expectedParameters));

    expectedParameters = new HashMap<>();
    expectedParameters.put("prop1", "value1");
    expectedParameters.put("ACCOUNT", "testaccount");
    testCases.add(
        new TestCase(
            "jdbc:snowflake://http://testaccount.localhost?prop1=value1",
            "http", "testaccount.localhost", 443,
            expectedParameters));

    // value including dash
    expectedParameters = new HashMap<>();
    expectedParameters.put("CLIENT_SESSION_KEEP_ALIVE", "true");
    expectedParameters.put("db", "TEST_DB");
    expectedParameters.put("proxyHost", "your-host.com");
    expectedParameters.put("proxyPort", "1234");
    expectedParameters.put("schema", "PUBLIC");
    expectedParameters.put("useProxy", "true");
    expectedParameters.put("warehouse", "TEST_WH");
    expectedParameters.put("ACCOUNT", "testaccount");
    testCases.add(
        new TestCase(
            "jdbc:snowflake://testaccount.snowflakecomputing.com:443" +
            "?CLIENT_SESSION_KEEP_ALIVE=true&db=TEST_DB&proxyHost=your-host.com&proxyPort=1234" +
            "&schema=PUBLIC&useProxy=true&warehouse=TEST_WH",
            "https", "testaccount.snowflakecomputing.com", 443,
            expectedParameters));

    // value including dot
    expectedParameters = new HashMap<>();
    expectedParameters.put("proxyHost", "72.1.32.55");
    expectedParameters.put("proxyPort", "portU");
    expectedParameters.put("ACCOUNT", "testaccount");
    testCases.add(
        new TestCase(
            "jdbc:snowflake://testaccount.snowflakecomputing.com" +
            "?proxyHost=72.1.32.55&proxyPort=port%55",
            "https", "testaccount.snowflakecomputing.com", 443,
            expectedParameters));

    // value in escaped value
    expectedParameters = new HashMap<>();
    expectedParameters.put("proxyHost", "=/");
    expectedParameters.put("proxyPort", "777");
    expectedParameters.put("ssl", "off");
    testCases.add(
        new TestCase(
            "jdbc:snowflake://localhost:8080?proxyHost=%3d%2f&proxyPort=777&ssl=off",
            "http", "localhost", 8080,
            expectedParameters));

    // value including non ascii characters
    expectedParameters = new HashMap<>();
    expectedParameters.put("proxyHost", "cheese");
    expectedParameters.put("proxyPort", "!@");
    testCases.add(
        new TestCase(
            "jdbc:snowflake://localhost:8080?proxyHost=cheese&proxyPort=!@",
            "https", "localhost", 8080,
            expectedParameters));

    // value including non ascii characters
    expectedParameters = new HashMap<>();
    expectedParameters.put("proxyHost", "cheese");
    expectedParameters.put("proxyPort", "cake");
    testCases.add(
        new TestCase(
            "jdbc:snowflake://https://localhost:8080?proxyHost=cheese&proxyPort=cake",
            "https", "localhost", 8080,
            expectedParameters));

    // host including underscore
    expectedParameters = new HashMap<>();
    expectedParameters.put("ACCOUNT", "snowflake");
    testCases.add(
        new TestCase(
            "jdbc:snowflake://snowflake.reg-RT-Replication1-7387_2.local:8082",
            "https", "snowflake.reg-RT-Replication1-7387_2.local", 8082,
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
            "https", "snowflake.reg-RT-Replication1-7387_2.local", 8082,
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
            "https", "snowflake.reg-RT-Replication1-7387_2.local", 8082,
            expectedParameters));

    // host including underscore with parameters after a path slash
    expectedParameters = new HashMap<>();
    expectedParameters.put("ACCOUNT", "testaccount");
    expectedParameters.put("authenticator", "https://snowflakecomputing.okta.com");
    testCases.add(
        new TestCase(
            "jdbc:snowflake://testaccount.snowflakecomputing.com/"
            + "?authenticator=https://snowflakecomputing.okta.com",
            "https", "testaccount.snowflakecomputing.com", 443,
            expectedParameters));

    // localhost without port
    expectedParameters = new HashMap<>();
    testCases.add(
        new TestCase(
            "jdbc:snowflake://localhost:",
            "https", "localhost", 443,
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
            "jdbc:snowflake://snowflake.reg.local:8082/" +
            "?account=testaccount" +
            "&authenticator=https://snowflakecomputing.okta.com/app/template_saml_2_0/ky7gy61iAOAMLLSOZSVX/sso/saml" +
            "&user=qa&ssl=off" +
            "&schema=testschema&db=testdb&networkTimeout=3600&retryQuery=on",
            "http", "snowflake.reg.local", 8082,
            expectedParameters));

    // invalid key value pair including multiple equal signs
    expectedParameters = new HashMap<>();
    expectedParameters.put("ACCOUNT", "testaccount");
    expectedParameters.put("proxyPort", "781");
    testCases.add(
        new TestCase(
            "jdbc:snowflake://testaccount.com?proxyHost=puppy==dog&proxyPort=781",
            "https", "testaccount.com", 443,
            expectedParameters));

    // invalid key value including multiple amp
    expectedParameters = new HashMap<>();
    expectedParameters.put("ACCOUNT", "testaccount");
    expectedParameters.put("proxyPort", "5");
    testCases.add(
        new TestCase(
            "jdbc:snowflake://testaccount.com?proxyHost=&&&proxyPort=5",
            "https", "testaccount.com", 443,
            expectedParameters));

    // NOT global url
    expectedParameters = new HashMap<>();
    expectedParameters.put("ACCOUNT", "globalaccount-12345");
    expectedParameters.put("proxyPort", "45.12.34.5");
    testCases.add(
        new TestCase(
            "jdbc:snowflake://globalaccount-12345.com?proxyHost=&&&proxyPort=45.12.34.5",
            "https", "globalaccount-12345.com", 443,
            expectedParameters));

    // global url
    expectedParameters = new HashMap<>();
    expectedParameters.put("ACCOUNT", "globalaccount");
    expectedParameters.put("proxyPort", "45-12-34-5");
    testCases.add(
        new TestCase(
            "jdbc:snowflake://globalaccount-12345.global.snowflakecomputing.com?proxyHost=&&&proxyPort=45-12-34-5",
            "https", "globalaccount-12345.global.snowflakecomputing.com", 443,
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
            "jdbc:snowflake://snowflake.reg-RT-PC-Language1-10850.local:8082/?account=snowflake&user=admin&ssl=off&networkTimeout=3600&retryQuery=on",
            "http", "snowflake.reg-RT-PC-Language1-10850.local", 8082,
            expectedParameters));

    for (TestCase t : testCases)
    {
      assertTrue("URL is not valid: " + t.url, snowflakeDriver.acceptsURL(t.url));
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
    assertFalse(snowflakeDriver.acceptsURL("jdbc:snowflake://testaccount.com?proxyHost=%b&proxyPort="));
  }
}
