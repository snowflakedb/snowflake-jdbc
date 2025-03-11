package net.snowflake.client.jdbc;

import static net.snowflake.client.AbstractDriverIT.getFullPathFileInResource;
import static net.snowflake.client.jdbc.SnowflakeDriverIT.findFile;
import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.net.Authenticator;
import java.net.PasswordAuthentication;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.core.HttpClientSettingsKey;
import net.snowflake.client.core.HttpProtocol;
import net.snowflake.client.core.HttpUtil;
import net.snowflake.client.core.SFSession;
import net.snowflake.common.core.SqlState;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

// To run these tests, you must:
// 1.) Start up a proxy connection. The simplest ways are via Squid or BurpSuite. Confluence doc on
// setup here:
// https://snowflakecomputing.atlassian.net/wiki/spaces/EN/pages/65438343/How+to+setup+Proxy+Server+for+Client+tests
// 2.) Enter your own username and password for the account you're connecting to
// 3.) Adjust parameters like role, database, schema, etc to match with account accordingly

@Tag(TestTags.OTHERS)
public class CustomProxyLatestIT {
  @TempDir private File tmpFolder;

  /**
   * Before running this test, change the user and password to appropriate values. Set up 2
   * different proxy ports that can run simultaneously. This is easy with Burpsuite.
   *
   * <p>This tests that separate, successful connections can be made with 2 separate proxies at the
   * same time.
   *
   * @throws SQLException
   */
  @Test
  @Disabled
  public void test2ProxiesWithSameJVM() throws SQLException {
    Properties props = new Properties();
    props.put("user", "USER");
    props.put("password", "PASSWORD");
    props.put("useProxy", true);
    props.put("proxyHost", "localhost");
    props.put("proxyPort", "8080");
    // Set up the first connection and proxy
    try (Connection con1 =
            DriverManager.getConnection(
                "jdbc:snowflake://s3testaccount.us-east-1.snowflakecomputing.com", props);
        Statement stmt = con1.createStatement();
        ResultSet rs = stmt.executeQuery("select 1")) {
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));
    }

    // Change the proxy settings for the 2nd connection, but all other properties can be re-used.
    // Set up the second connection.
    props.put("proxyPort", "8081");
    try (Connection con2 =
            DriverManager.getConnection(
                "jdbc:snowflake://aztestaccount.east-us-2.azure.snowflakecomputing.com", props);
        Statement statement = con2.createStatement();
        ResultSet rs = statement.executeQuery("select 2")) {
      assertTrue(rs.next());
      assertEquals(2, rs.getInt(1));
      // To ensure that the http client map is functioning properly, make a third connection with
      // the
      // same properties and proxy as the first connection.
    }

    props.put("proxyPort", "8080");
    try (Connection con3 =
            DriverManager.getConnection(
                "jdbc:snowflake://s3testaccount.us-east-1.snowflakecomputing.com", props);
        Statement stmt = con3.createStatement();
        ResultSet rs = stmt.executeQuery("select 1")) {
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));
      // Assert that although there are 3 connections, 2 of them (1st and 3rd) use the same
      // httpclient
      // object in the map. The total map size should be 2 for the 3 connections.
      assertEquals(2, HttpUtil.httpClient.size());
    }
  }

  /**
   * This requires a TLS proxy connection. This can be done by configuring the squid.conf file (with
   * squid proxy) and adding certs to the keystore. For info on setup, see
   * https://snowflakecomputing.atlassian.net/wiki/spaces/EN/pages/65438343/How+to+setup+Proxy+Server+for+Client+tests.
   *
   * @throws SQLException
   */
  @Test
  @Disabled
  public void testTLSIssue() throws SQLException {
    Properties props = new Properties();
    props.put("user", "USER");
    props.put("password", "PASSWORD");
    props.put("tracing", "ALL");
    props.put("useProxy", true);
    props.put("proxyHost", "localhost");
    props.put("proxyPort", "3128");
    // protocol must be specified for https (default is http)
    props.put("proxyProtocol", "https");
    try (Connection con =
            DriverManager.getConnection(
                "jdbc:snowflake://s3testaccount.us-east-1.snowflakecomputing.com", props);
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("select 1")) {
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));
    }

    // Test with jvm properties instead
    props.put("useProxy", false);
    System.setProperty("http.useProxy", "true");
    System.setProperty("http.proxyHost", "localhost");
    System.setProperty("http.proxyPort", "3128");
    try (Connection con =
            DriverManager.getConnection(
                "jdbc:snowflake://s3testaccount.us-east-1.snowflakecomputing.com", props);
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("select 1")) {
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));
    }
  }

  /**
   * Test nonProxyHosts is honored with JVM proxy parameters. Recommended test: 1. Test that proxy
   * is not used with current settings since nonProxyHosts = * 2. Change nonProxyHosts value to
   * nonsense value to ensure it can be set and proxy still works 3. Run same 2 above tests but with
   * http instead of https proxy parameters for non-TLS proxy
   */
  @Test
  @Disabled
  public void testJVMParamsWithNonProxyHostsHonored() throws SQLException {
    Properties props = new Properties();
    props.put("user", "USER");
    props.put("password", "PASSWORD");
    props.put("tracing", "ALL");
    // Set JVM properties. Test once with TLS proxy, and edit to test with non-TLS proxy
    System.setProperty("http.useProxy", "true");
    System.setProperty("https.proxyHost", "localhost");
    System.setProperty("https.proxyPort", "3128");
    System.setProperty("http.nonProxyHosts", "*");
    try (Connection con =
            DriverManager.getConnection(
                "jdbc:snowflake://s3testaccount.us-east-1.snowflakecomputing.com", props);
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("select 1")) {
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));
    }
  }

  /** Test TLS issue against S3 client to ensure proxy works with PUT/GET statements */
  @Test
  @Disabled
  public void testTLSIssueWithConnectionStringAgainstS3()
      throws ClassNotFoundException, SQLException {

    String connectionUrl =
        "jdbc:snowflake://s3testaccount.us-east-1.snowflakecomputing.com/?tracing=ALL"
            + "&proxyHost=localhost&proxyPort=3128&useProxy=true&proxyProtocol=https";
    // should finish correctly
    runProxyConnection(connectionUrl);
  }

  /**
   * Before running this test, change the user and password to appropriate values. Set up a proxy
   * with Burpsuite so you can see what POST and GET requests are going through the proxy.
   *
   * <p>This tests that the NonProxyHosts field is successfully updated for the same HttpClient
   * object.
   *
   * @throws SQLException
   */
  @Test
  @Disabled
  public void testNonProxyHostAltering() throws SQLException {
    Properties props = new Properties();
    props.put("user", "USER");
    props.put("password", "PASSWORD");
    props.put("useProxy", true);
    props.put("proxyHost", "localhost");
    props.put("proxyPort", "8080");
    props.put("nonProxyHosts", "*.snowflakecomputing.com");
    // Set up the first connection and proxy
    try (Connection con =
            DriverManager.getConnection(
                "jdbc:snowflake://s3testaccount.us-east-1.snowflakecomputing.com", props);
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("select 1")) {
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));
      // Assert that nonProxyHosts string is correct for initial value
      HttpUtil.httpClient
          .entrySet()
          .forEach((entry) -> assertEquals(".foo.com|.baz.com", entry.getKey().getNonProxyHosts()));
    }
    // Now make 2nd connection with all the same settings except different nonProxyHosts field
    props.put("nonProxyHosts", "*.snowflakecomputing.com");
    // Manually check here that nonProxyHost setting works by checking that nothing else goes
    // through proxy from this point onward. *.snowflakecomputing.com should ensure that proxy is
    // skipped.
    try (Connection con =
            DriverManager.getConnection(
                "jdbc:snowflake://s3testaccount.us-east-1.snowflakecomputing.com", props);
        Statement statement = con.createStatement();
        ResultSet rs = statement.executeQuery("select 2")) {
      assertTrue(rs.next());
      assertEquals(2, rs.getInt(1));
      assertEquals(1, HttpUtil.httpClient.size());
      // Assert that the entry contains the correct updated value for nonProxyHosts string
      HttpUtil.httpClient
          .entrySet()
          .forEach(
              (entry) ->
                  assertEquals("*.snowflakecomputing.com", entry.getKey().getNonProxyHosts()));
    }
  }

  /**
   * This tests that the HttpClient object is re-used when no proxies are present.
   *
   * @throws SQLException
   */
  @Test
  @Disabled
  public void testSizeOfHttpClientNoProxies() throws SQLException {
    Properties props = new Properties();
    props.put("user", "USER");
    props.put("password", "PASSWORD");
    // Set up the first connection and proxy
    try (Connection con =
            DriverManager.getConnection(
                "jdbc:snowflake://s3testaccount.us-east-1.snowflakecomputing.com", props);
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("select 1")) {
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1));
    }

    // put in some fake properties that won't get picked up because useProxy=false
    props.put("useProxy", false);
    props.put("proxyHost", "localhost");
    props.put("proxyPort", "8080");
    try (Connection con =
        DriverManager.getConnection(
            "jdbc:snowflake://s3testaccount.us-east-1.snowflakecomputing.com", props)) {
      // Assert that the HttpClient table has only 1 entry for both non-proxy entries
      assertEquals(1, HttpUtil.httpClient.size());
    }

    props.put("ocspFailOpen", "false");
    try (Connection con =
        DriverManager.getConnection(
            "jdbc:snowflake://s3testaccount.us-east-1.snowflakecomputing.com", props)) {
      // Table should grow in size by 1 when OCSP mode changes
      assertEquals(2, HttpUtil.httpClient.size());
    }
  }

  @Test
  @Disabled
  public void testCorrectProxySettingFromConnectionString()
      throws ClassNotFoundException, SQLException {
    String connectionUrl =
        "jdbc:snowflake://s3testaccount.us-east-1.snowflakecomputing.com/?tracing=ALL"
            + "&proxyHost=localhost&proxyPort=8080"
            + "&useProxy=true";
    // should finish correctly
    runProxyConnection(connectionUrl);

    connectionUrl =
        "jdbc:snowflake://s3testaccount.us-east-1.snowflakecomputing.com/?tracing=ALL"
            + "&proxyHost=localhost&proxyPort=8080"
            + "&proxyUser=testuser1&proxyPassword=test"
            + "&useProxy=true";
    // should finish correctly
    runProxyConnection(connectionUrl);
  }

  @Test
  @Disabled
  public void testWrongProxyPortSettingFromConnectionString()
      throws ClassNotFoundException, SQLException {

    String connectionUrl =
        "jdbc:snowflake://s3testaccount.us-east-1.snowflakecomputing.com/?tracing=ALL"
            + "&proxyHost=localhost&proxyPort=31281"
            + "&proxyUser=testuser1&proxyPassword=test"
            + "&nonProxyHosts=*.foo.com|localhost&useProxy=true";
    // should show warning for null response for the requests
    runProxyConnection(connectionUrl);
  }

  @Test
  @Disabled
  public void testWrongProxyPasswordSettingFromConnectionString()
      throws ClassNotFoundException, SQLException {

    String connectionUrl =
        "jdbc:snowflake://s3testaccount.us-east-1.snowflakecomputing.com/?tracing=ALL"
            + "&proxyHost=localhost&proxyPort=3128"
            + "&proxyUser=testuser2&proxyPassword=test111"
            + "&nonProxyHosts=*.foo.com|localhost&useProxy=true";
    // should show warning for null response for the requests
    try {
      runProxyConnection(connectionUrl);
    } catch (SQLException e) {
      assertThat(
          "JDBC driver encountered communication error",
          e.getErrorCode(),
          equalTo(ErrorCode.NETWORK_ERROR.getMessageCode()));
    }
  }

  @Test
  @Disabled
  public void testInvalidProxyPortFromConnectionString()
      throws ClassNotFoundException, SQLException {

    String connectionUrl =
        "jdbc:snowflake://s3testaccount.us-east-1.snowflakecomputing.com/?tracing=ALL"
            + "&proxyHost=localhost"
            + "&proxyUser=testuser1&proxyPassword=test"
            + "&nonProxyHosts=*.foo.com|localhost&useProxy=true";
    // should throw SnowflakeSQLException: 200051
    try {
      runProxyConnection(connectionUrl);
    } catch (SQLException e) {
      assertThat(
          "invalid proxy error",
          e.getErrorCode(),
          equalTo(ErrorCode.INVALID_PROXY_PROPERTIES.getMessageCode()));
    }
  }

  @Test
  @Disabled
  public void testNonProxyHostsFromConnectionString() throws ClassNotFoundException, SQLException {

    String connectionUrl =
        "jdbc:snowflake://s3testaccount.us-east-1.snowflakecomputing.com/?tracing=ALL"
            + "&proxyHost=localhost&proxyPort=31281"
            + "&proxyUser=testuser1&proxyPassword=test"
            + "&nonProxyHosts=*.snowflakecomputing.com|localhost&useProxy=true";
    // should finish correctly
    runProxyConnection(connectionUrl);
  }

  @Test
  @Disabled
  public void testWrongNonProxyHostsFromConnectionString()
      throws ClassNotFoundException, SQLException {

    String connectionUrl =
        "jdbc:snowflake://s3testaccount.us-east-1.snowflakecomputing.com/?tracing=ALL"
            + "&proxyHost=localhost&proxyPort=31281"
            + "&proxyUser=testuser1&proxyPassword=test"
            + "&nonProxyHosts=*.foo.com|localhost&useProxy=true";
    // should fail to connect

    runProxyConnection(connectionUrl);
  }

  @Test
  @Disabled
  public void testUnsetJvmPropertiesForInvalidSettings() throws SQLException {
    Properties props = new Properties();
    props.put("user", "USER");
    props.put("password", "PASSWORD");
    props.put("tracing", "ALL");
    // Set JVM properties.
    System.setProperty("proxyHost", "localhost");
    System.setProperty("proxyPort", "3128");
    try (Connection con =
        DriverManager.getConnection(
            "jdbc:snowflake://s3testaccount.us-east-1.snowflakecomputing.com", props)) {
      assertEquals(System.getProperty("proxyHost"), null);
      assertEquals(System.getProperty("proxyPort"), null);
    }
  }

  public void runProxyConnection(String connectionUrl) throws ClassNotFoundException, SQLException {
    Authenticator.setDefault(
        new Authenticator() {
          @Override
          public PasswordAuthentication getPasswordAuthentication() {
            System.out.println("RequestorType: " + getRequestorType());
            System.out.println("Protocol: " + getRequestingProtocol().toLowerCase());
            return new PasswordAuthentication(
                systemGetProperty("http.proxyUser"),
                systemGetProperty("http.proxyPassword").toCharArray());
          }
        });

    System.setProperty("http.useProxy", "true");
    System.setProperty("https.proxyHost", "localhost");
    System.setProperty("https.proxyPort", "3128");

    // SET USER AND PASSWORD FIRST
    String user = "USER";
    String passwd = "PASSWORD";
    Properties _connectionProperties = new Properties();
    _connectionProperties.put("user", user);
    _connectionProperties.put("password", passwd);
    _connectionProperties.put("role", "accountadmin");
    _connectionProperties.put("database", "SNOWHOUSE_IMPORT");
    _connectionProperties.put("schema", "DEV");

    Class.forName("net.snowflake.client.jdbc.SnowflakeDriver");
    long counter = 0;
    while (true) {
      try (Connection con = DriverManager.getConnection(connectionUrl, _connectionProperties);
          Statement stmt = con.createStatement()) {
        stmt.execute("use warehouse TINY_WAREHOUSE");
        stmt.execute("CREATE OR REPLACE STAGE testPutGet_stage");
        assertTrue(
            stmt.execute(
                "PUT file://" + getFullPathFileInResource("orders_100.csv") + " @testPutGet_stage"),
            "Failed to put a file");
        String sql = "select $1 from values(1),(3),(5),(7)";
        try (ResultSet res = stmt.executeQuery(sql)) {
          while (res.next()) {
            System.out.println("value: " + res.getInt(1));
          }
          System.out.println("OK - " + counter);
        }
      }
      counter++;
      break;
    }
  }

  @Test
  @Disabled
  public void testProxyConnectionWithAzure() throws ClassNotFoundException, SQLException {
    String connectionUrl =
        "jdbc:snowflake://aztestaccount.east-us-2.azure.snowflakecomputing.com/?tracing=ALL";
    runAzureProxyConnection(
        connectionUrl, /* usesConnectionProperties */ true, /* usesIncorrectJVMParameters */ true);
  }

  @Test
  @Disabled
  public void testProxyConnectionWithAzureWithConnectionString()
      throws ClassNotFoundException, SQLException {
    String connectionUrl =
        "jdbc:snowflake://aztestaccount.east-us-2.azure.snowflakecomputing.com/?tracing=ALL"
            + "&proxyHost=localhost&proxyPort=8080"
            + "&proxyUser=testuser1&proxyPassword=test"
            + "&useProxy=true";
    runAzureProxyConnection(
        connectionUrl, /* usesConnectionProperties */ false, /* usesIncorrectJVMParameters */ true);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "jdbc:snowflake://aztestaccount.east-us-2.azure.snowflakecomputing.com/?tracing=ALL"
            + "&proxyHost=localhost&proxyPort="
            + "&proxyUser=testuser1&proxyPassword=test"
            + "&useProxy=true",
        "jdbc:snowflake://aztestaccount.east-us-2.azure.snowflakecomputing.com/?tracing=ALL"
            + "&proxyHost=localhost&proxyPort=cheese"
            + "&proxyUser=testuser1&proxyPassword=test"
            + "&useProxy=true",
        "jdbc:snowflake://aztestaccount.east-us-2.azure.snowflakecomputing.com/?tracing=ALL"
            + "&proxyHost=&proxyPort=3128"
            + "&proxyUser=testuser1&proxyPassword=test"
            + "&useProxy=true",
        "jdbc:snowflake://aztestaccount.east-us-2.azure.snowflakecomputing.com/?tracing=ALL"
            + "&proxyUser=testuser1&proxyPassword=test"
            + "&useProxy=true"
      })
  @Disabled
  public void testProxyConnectionWithoutProxyPortOrHost(String connectionUrl) {
    SQLException e =
        assertThrows(
            SQLException.class,
            () ->
                runAzureProxyConnection(
                    connectionUrl, /* usesConnectionProperties */
                    false, /* usesIncorrectJVMParameters */
                    true));
    assertEquals(SqlState.CONNECTION_EXCEPTION, e.getSQLState());
  }

  /**
   * Before running this test, change the user and password in runAzureProxyConnection() to
   * appropriate values. Set up a proxy with Burpsuite so you can see what POST and GET requests are
   * going through the proxy.
   *
   * <p>This tests that the NonProxyHosts field is successfully updated for the same HttpClient
   * object.
   *
   * @throws SQLException
   */
  @Test
  @Disabled
  public void testProxyConnectionWithJVMParameters() throws SQLException, ClassNotFoundException {
    String connectionUrl =
        "jdbc:snowflake://aztestaccount.east-us-2.azure.snowflakecomputing.com/?tracing=ALL";
    // Set valid JVM system properties
    System.setProperty("http.useProxy", "true");
    System.setProperty("http.proxyHost", "localhost");
    System.setProperty("http.proxyPort", "8080");
    System.setProperty("http.nonProxyHosts", "*.snowflakecomputing.com");
    SnowflakeUtil.systemSetEnv("NO_PROXY", "*.google.com");
    runAzureProxyConnection(
        connectionUrl, /* usesConnectionProperties */
        false, /* usesIncorrectJVMParameters */
        false);
    SnowflakeUtil.systemUnsetEnv("NO_PROXY");
  }

  @Test
  @Disabled
  public void testProxyConnectionWithAzureWithWrongConnectionString()
      throws ClassNotFoundException {
    String connectionUrl =
        "jdbc:snowflake://aztestaccount.east-us-2.azure.snowflakecomputing.com/?tracing=ALL"
            + "&proxyHost=localhost&proxyPort=31281"
            + "&proxyUser=testuser1&proxyPassword=test"
            + "&nonProxyHosts=*.foo.com%7Clocalhost&useProxy=true";

    try {
      runAzureProxyConnection(
          connectionUrl, /* usesConnectionProperties */
          false, /* usesIncorrectJVMParameters */
          true);
    } catch (SQLException e) {
      assertThat(
          "JDBC driver encountered communication error",
          e.getErrorCode(),
          equalTo(ErrorCode.NETWORK_ERROR.getMessageCode()));
    }
  }

  /**
   * Test that http proxy is used when http.proxyProtocol is http and http.proxyHost/http.proxyPort
   * is specified. Set up a http proxy and change the settings below.
   */
  @Test
  @Disabled
  public void testSetJVMProxyHttp() throws SQLException {
    Properties props = new Properties();
    props.put("user", "USER");
    props.put("password", "PASSWORD");

    System.setProperty("http.useProxy", "true");
    System.setProperty("http.proxyHost", "localhost");
    System.setProperty("http.proxyPort", "3128");
    System.setProperty("http.nonProxyHosts", "*");
    System.setProperty("http.proxyProtocol", "http");
    try (Connection con =
        DriverManager.getConnection(
            "jdbc:snowflake://s3testaccount.us-east-1.snowflakecomputing.com", props)) {
      SFSession sfSession = con.unwrap(SnowflakeConnectionV1.class).getSfSession();
      HttpClientSettingsKey clientSettingsKey = sfSession.getHttpClientKey();
      assertEquals(HttpProtocol.HTTP, clientSettingsKey.getProxyHttpProtocol());
    }
  }

  /**
   * Test that https proxy is used when http.proxyProtocol is https and
   * https.proxyHost/https.proxyPort is specified. Set up a https proxy and change the settings
   * below.
   */
  @Test
  @Disabled
  public void testSetJVMProxyHttps() throws SQLException {
    Properties props = new Properties();
    props.put("user", "USER");
    props.put("password", "PASSWORD");

    System.setProperty("http.useProxy", "true");
    System.setProperty("https.proxyHost", "localhost");
    System.setProperty("https.proxyPort", "3128");
    System.setProperty("http.nonProxyHosts", "*");
    System.setProperty("http.proxyProtocol", "https");
    try (Connection con =
        DriverManager.getConnection(
            "jdbc:snowflake://s3testaccount.us-east-1.snowflakecomputing.com", props)) {
      SFSession sfSession = con.unwrap(SnowflakeConnectionV1.class).getSfSession();
      HttpClientSettingsKey clientSettingsKey = sfSession.getHttpClientKey();
      assertEquals(HttpProtocol.HTTPS, clientSettingsKey.getProxyHttpProtocol());
    }
  }

  /**
   * Test that https proxy is used when https.proxyHost and https.proxyPort are specified. Set up a
   * https proxy and change the settings below.
   */
  @Test
  @Disabled
  public void testSetJVMProxyDefaultHttps() throws SQLException {
    Properties props = new Properties();
    props.put("user", "USER");
    props.put("password", "PASSWORD");

    System.setProperty("http.useProxy", "true");
    System.setProperty("https.proxyHost", "localhost");
    System.setProperty("https.proxyPort", "3128");
    System.setProperty("http.nonProxyHosts", "*");

    try (Connection con =
        DriverManager.getConnection(
            "jdbc:snowflake://s3testaccount.us-east-1.snowflakecomputing.com", props)) {
      SFSession sfSession = con.unwrap(SnowflakeConnectionV1.class).getSfSession();
      HttpClientSettingsKey clientSettingsKey = sfSession.getHttpClientKey();
      assertEquals(HttpProtocol.HTTPS, clientSettingsKey.getProxyHttpProtocol());
    }
  }

  public void runAzureProxyConnection(
      String connectionUrl, boolean usesProperties, boolean usesIncorrectJVMProperties)
      throws ClassNotFoundException, SQLException {
    Authenticator.setDefault(
        new Authenticator() {
          @Override
          public PasswordAuthentication getPasswordAuthentication() {
            System.out.println("RequestorType: " + getRequestorType());
            System.out.println("Protocol: " + getRequestingProtocol().toLowerCase());
            return new PasswordAuthentication(
                systemGetProperty("http.proxyUser"),
                systemGetProperty("http.proxyPassword").toCharArray());
          }
        });

    // Enable these parameters to use JVM proxy parameters instead of connection string proxy
    // parameters.
    // Connection parameters override JVM  proxy params, so these incorrect params won't cause
    // failures IF connection proxy params are enabled and working.
    if (usesIncorrectJVMProperties) {
      System.setProperty("http.useProxy", "true");
      System.setProperty("http.proxyHost", "fakehost");
      System.setProperty("http.proxyPort", "8081");
      System.setProperty("https.proxyHost", "fakehost");
      System.setProperty("https.proxyPort", "8081");
    }

    // SET USER AND PASSWORD FIRST
    String user = "USER";
    String passwd = "PASSWORD";
    Properties _connectionProperties = new Properties();
    _connectionProperties.put("user", user);
    _connectionProperties.put("password", passwd);
    _connectionProperties.put("role", "SYSADMIN");
    _connectionProperties.put("tracing", "ALL");
    if (usesProperties) {
      _connectionProperties.put("useProxy", true);
      _connectionProperties.put("proxyHost", "localhost");
      _connectionProperties.put("proxyPort", "8080");
      _connectionProperties.put("proxyUser", "testuser1");
      _connectionProperties.put("proxyPassword", "test");
    }

    Class.forName("net.snowflake.client.jdbc.SnowflakeDriver");

    String fileName = "test_copy.csv";
    try (Connection con = DriverManager.getConnection(connectionUrl, _connectionProperties);
        Statement stmt = con.createStatement()) {
      try {
        stmt.execute("create or replace warehouse MEGTEST");
        stmt.execute("use database MEGDB");
        stmt.execute("use schema MEGSCHEMA");
        stmt.execute("CREATE OR REPLACE STAGE testPutGet_stage");

        String TEST_DATA_FILE = "orders_100.csv";
        String sourceFilePath = getFullPathFileInResource(TEST_DATA_FILE);
        File destFolder = new File(tmpFolder, "dest");
        destFolder.mkdirs();
        String destFolderCanonicalPath = destFolder.getCanonicalPath();
        String destFolderCanonicalPathWithSeparator = destFolderCanonicalPath + File.separator;
        assertTrue(
            stmt.execute("PUT file://" + sourceFilePath + " @testPutGet_stage"),
            "Failed to put a file");
        findFile(stmt, "ls @testPutGet_stage/");

        // download the file we just uploaded to stage
        assertTrue(
            stmt.execute(
                "GET @testPutGet_stage 'file://" + destFolderCanonicalPath + "' parallel=8"),
            "Failed to get a file");

        // Make sure that the downloaded file exists, it should be gzip compressed
        File downloaded = new File(destFolderCanonicalPathWithSeparator + TEST_DATA_FILE + ".gz");
        assertTrue(downloaded.exists());

        Process p =
            Runtime.getRuntime()
                .exec("gzip -d " + destFolderCanonicalPathWithSeparator + TEST_DATA_FILE + ".gz");
        p.waitFor();

        File original = new File(sourceFilePath);
        File unzipped = new File(destFolderCanonicalPathWithSeparator + TEST_DATA_FILE);
        assertEquals(original.length(), unzipped.length());
      } catch (Throwable t) {
        t.printStackTrace();
      } finally {
        stmt.execute("DROP STAGE IF EXISTS testGetPut_stage");
      }
    }
  }
}
