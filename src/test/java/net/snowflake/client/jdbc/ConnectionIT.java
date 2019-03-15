/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import net.minidev.json.JSONObject;
import net.snowflake.client.ConditionalIgnoreRule.ConditionalIgnore;
import net.snowflake.client.RunningOnTravisCI;
import net.snowflake.client.jdbc.telemetryOOB.TelemetryEvent;
import net.snowflake.client.jdbc.telemetryOOB.TelemetryService;
import net.snowflake.common.core.SqlState;
import org.apache.commons.codec.binary.Base64;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Connection integration tests
 */
public class ConnectionIT extends BaseJDBCTest
{
  private boolean defaultState;

  @Before
  public void setUp()
  {
    TelemetryService service = TelemetryService.getInstance();
    service.updateContext(getConnectionParameters());
    defaultState = service.isEnabled();
    service.setNumOfRetryToTriggerTelemetry(3);
    service.disableRunFlushBeforeException();
    service.enable();
  }

  @After
  public void tearDown() throws InterruptedException
  {
    TelemetryService service = TelemetryService.getInstance();
    service.flush();
    // wait 5 seconds while the service is flushing
    TimeUnit.SECONDS.sleep(5);

    if (defaultState)
    {
      service.enable();
    }
    else
    {
      service.disable();
    }
    service.resetNumOfRetryToTriggerTelemetry();
    service.enableRunFlushBeforeException();
  }

  @Test
  public void testSimpleConnection() throws SQLException
  {
    Connection con = getConnection();
    Statement statement = con.createStatement();
    ResultSet resultSet = statement.executeQuery("show parameters");
    assertTrue(resultSet.next());
    assertTrue(!con.isClosed());
    statement.close();
    con.close();
    assertTrue(con.isClosed());
    con.close(); // ensure no exception
  }

  /**
   * Test that login timeout kick in when connect through datasource
   *
   * @throws SQLException if any SQL error occurs
   */
  @Test
  public void testLoginTimeoutViaDataSource() throws SQLException
  {
    SnowflakeBasicDataSource ds = new SnowflakeBasicDataSource();
    ds.setUrl("jdbc:snowflake://fakeaccount.snowflakecomputing.com");
    ds.setUser("fakeUser");
    ds.setPassword("fakePassword");
    ds.setAccount("fakeAccount");
    ds.setLoginTimeout(10);

    long startLoginTime = System.currentTimeMillis();
    try
    {
      ds.getConnection();
      fail();
    }
    catch (SQLException e)
    {
      assertThat(e.getErrorCode(), is(ErrorCode.NETWORK_ERROR.getMessageCode()));
    }
    long endLoginTime = System.currentTimeMillis();

    assertTrue(endLoginTime - startLoginTime < 30000);
  }

  /**
   * Test production connectivity in case cipher suites or tls protocol change
   * Use fake username and password but correct url
   * Expectation is receiving incorrect username or password response from server
   */
  @Test
  public void testProdConnectivity() throws SQLException
  {
    String[] deploymentUrls = {
        "jdbc:snowflake://sfcsupport.snowflakecomputing.com",
        "jdbc:snowflake://sfcsupportva.us-east-1.snowflakecomputing.com",
        "jdbc:snowflake://sfcsupporteu.eu-central-1.snowflakecomputing.com"};

    Properties properties = new Properties();

    properties.put("user", "fakesuer");
    properties.put("password", "fakepwd");
    properties.put("account", "fakeaccount");

    for (String url : deploymentUrls)
    {
      try
      {
        DriverManager.getConnection(url, properties);
        fail();
      }
      catch (SQLException e)
      {
        assertThat(e.getErrorCode(), is(
            ErrorCode.CONNECTION_ERROR.getMessageCode()));
      }
    }
  }

  @Test
  public void testSetCatalogSchema() throws Throwable
  {
    try (Connection connection = getConnection())
    {
      String db = connection.getCatalog();
      String schema = connection.getSchema();
      connection.setCatalog(db);
      connection.setSchema("PUBLIC");

      // get the current schema
      ResultSet rst = connection.createStatement().executeQuery("select current_schema()");
      assertTrue(rst.next());
      assertEquals("PUBLIC", rst.getString(1));
      assertEquals(db, connection.getCatalog());
      assertEquals("PUBLIC", connection.getSchema());

      // get the current schema
      connection.setSchema(schema);
      rst = connection.createStatement().executeQuery("select current_schema()");
      assertTrue(rst.next());
      assertEquals(schema, rst.getString(1));
      rst.close();
    }
  }

  @Test
  @ConditionalIgnore(condition = RunningOnTravisCI.class)
  public void testConnectionGetAndSetDBAndSchema() throws SQLException
  {
    Connection con = getConnection();

    final String database = System.getenv("SNOWFLAKE_TEST_DATABASE").toUpperCase();
    final String schema = System.getenv("SNOWFLAKE_TEST_SCHEMA").toUpperCase();

    assertEquals(database, con.getCatalog());
    assertEquals(schema, con.getSchema());

    final String SECOND_DATABASE = "SECOND_DATABASE";
    final String SECOND_SCHEMA = "SECOND_SCHEMA";
    Statement statement = con.createStatement();
    statement.execute(String.format("create or replace database %s", SECOND_DATABASE));
    statement.execute(String.format("create or replace schema %s", SECOND_SCHEMA));
    statement.execute(String.format("use database %s", database));

    // TODO: use the other database and schema
    con.setCatalog(SECOND_DATABASE);
    assertEquals(SECOND_DATABASE, con.getCatalog());
    assertEquals("PUBLIC", con.getSchema());

    con.setSchema(SECOND_SCHEMA);
    assertEquals(SECOND_SCHEMA, con.getSchema());

    statement.execute(String.format("use database %s", database));
    statement.execute(String.format("use schema %s", schema));

    assertEquals(database, con.getCatalog());
    assertEquals(schema, con.getSchema());

    statement.execute(String.format("drop database if exists %s", SECOND_DATABASE));
    con.close();
  }

  @Test
  public void testConnectionClientInfo() throws SQLException
  {
    Connection con = getConnection();
    Properties property = con.getClientInfo();
    assertEquals(0, property.size());
    Properties clientInfo = new Properties();
    clientInfo.setProperty("name", "Peter");
    clientInfo.setProperty("description", "SNOWFLAKE JDBC");

    con.setClientInfo(clientInfo);
    con.setClientInfo("clinetinfoA", "valueA");
    Properties ci = con.getClientInfo();
    assertEquals(3, ci.size());
    String name = con.getClientInfo("name");
    assertEquals("Peter", name);
    con.close();
  }

  // only support get and set
  @Test
  public void testNetworkTimeout() throws SQLException
  {
    Connection con = getConnection();
    int millis = con.getNetworkTimeout();
    assertEquals(0, millis);
    con.setNetworkTimeout(null, 200);
    assertEquals(200, con.getNetworkTimeout());
    con.close();
  }

  @Test
  public void testAbort() throws SQLException
  {
    Connection con = getConnection();
    assertTrue(!con.isClosed());
    con.abort(null);
    assertTrue(con.isClosed());
  }

  @Test
  public void testSetQueryTimeoutInConnectionStr() throws SQLException
  {
    Properties properties = new Properties();
    properties.put("queryTimeout", "5");
    Connection connection = getConnection(properties);
    Statement statement = connection.createStatement();
    try
    {
      statement.executeQuery("select count(*) from table(generator(timeLimit => 1000000))");
    }
    catch (SQLException e)
    {
      assertTrue(true);
      assertEquals(SqlState.QUERY_CANCELED, e.getSQLState());
      assertEquals("SQL execution canceled", e.getMessage());
    }
    statement.close();
    connection.close();
  }

  @Test
  public void testHttpsLoginTimeoutWithSSL() throws SQLException
  {
    long connStart = 0, conEnd;
    Properties properties = new Properties();
    properties.put("account", "wrongaccount");
    properties.put("loginTimeout", "20");
    properties.put("user", "fakeuser");
    properties.put("password", "fakepassword");
    // only when ssl is on can trigger the login timeout
    // ssl is off will trigger 404
    properties.put("ssl", "on");
    int queueSize = TelemetryService.getInstance().size();
    try
    {
      connStart = System.currentTimeMillis();
      Map<String, String> params = getConnectionParameters();
      // use wrongaccount in url
      String host = params.get("host");
      String[] hostItems = host.split("\\.");
      String wrongUri = params.get("uri").replace("://" + hostItems[0], "://wrongaccount");

      DriverManager.getConnection(wrongUri, properties);
    }
    catch (SQLException e)
    {
      assertThat("Communication error", e.getErrorCode(),
                 equalTo(ErrorCode.NETWORK_ERROR.getMessageCode()));

      conEnd = System.currentTimeMillis();
      assertThat("Login time out not taking effective",
                 conEnd - connStart < 60000);

      if (TelemetryService.getInstance().isDeploymentEnabled())
      {
        assertEquals(TelemetryService.getInstance().size(), queueSize + 2);
        TelemetryEvent te = TelemetryService.getInstance().peek();
        JSONObject values = (JSONObject) te.get("Value");
        assertThat("Communication error",
                   values.get("errorCode").toString().compareTo(
                       ErrorCode.NETWORK_ERROR.getMessageCode().toString()) == 0);
      }
      return;
    }
    fail();
  }

  @Test
  public void testHttpsLoginTimeoutWithOutSSL() throws SQLException
  {
    Properties properties = new Properties();
    properties.put("account", "wrongaccount");
    properties.put("loginTimeout", "20");
    properties.put("user", "fakeuser");
    properties.put("password", "fakepassword");
    properties.put("ssl", "off");
    int queueSize = TelemetryService.getInstance().size();
    try
    {
      Map<String, String> params = getConnectionParameters();
      // use wrongaccount in url
      String host = params.get("host");
      String[] hostItems = host.split("\\.");
      String wrongUri = params.get("uri").replace("://" + hostItems[0], "://wrongaccount");

      DriverManager.getConnection(wrongUri, properties);
    }
    catch (SQLException e)
    {
      if (TelemetryService.getInstance().getServerDeploymentName().equals(
          TelemetryService.TELEMETRY_SERVER_DEPLOYMENT.DEV.getName()) ||
          TelemetryService.getInstance().getServerDeploymentName().equals(
              TelemetryService.TELEMETRY_SERVER_DEPLOYMENT.REG.getName()))
      {
        // a connection error response (wrong user and password)
        // with status code 200 is returned in RT
        assertThat("Communication error", e.getErrorCode(),
                   equalTo(ErrorCode.CONNECTION_ERROR.getMessageCode()));

        // since it returns normal response,
        // the telemetry does not create new event
        if (TelemetryService.getInstance().isDeploymentEnabled())
        {
          assertEquals(0, TelemetryService.getInstance().size() - queueSize);
        }
      }
      else
      {
        // in qa1 and others, 404 http status code should be returned
        assertThat("Communication error", e.getErrorCode(),
                   equalTo(ErrorCode.NETWORK_ERROR.getMessageCode()));

        if (TelemetryService.getInstance().isDeploymentEnabled())
        {
          assertEquals(1, TelemetryService.getInstance().size() - queueSize);
          TelemetryEvent te = TelemetryService.getInstance().peek();
          String name = (String) te.get("Name");
          int statusCode = (int) ((JSONObject) te.get("Value")).get("responseStatusCode");
          assertEquals(name, "HttpError404");
          assertEquals(statusCode, 404);
        }
      }
      return;
    }
    fail();
  }

  @Test
  public void testWrongHostNameTimeout() throws SQLException
  {
    long connStart = 0, conEnd;
    Properties properties = new Properties();
    properties.put("account", "testaccount");
    properties.put("loginTimeout", "20");
    properties.put("user", "fakeuser");
    properties.put("password", "fakepassword");
    int queueSize = TelemetryService.getInstance().size();
    try
    {
      connStart = System.currentTimeMillis();
      Map<String, String> params = getConnectionParameters();
      // use wrongaccount in url
      String host = params.get("host");
      String[] hostItems = host.split("\\.");
      String wrongUri = params.get("uri").replace(
          "." + hostItems[hostItems.length - 2] + ".", ".wronghostname.");

      DriverManager.getConnection(wrongUri, properties);
    }
    catch (SQLException e)
    {
      assertThat("Communication error", e.getErrorCode(),
                 equalTo(ErrorCode.NETWORK_ERROR.getMessageCode()));

      conEnd = System.currentTimeMillis();
      assertThat("Login time out not taking effective",
                 conEnd - connStart < 60000);

      if (TelemetryService.getInstance().isDeploymentEnabled())
      {
        assertEquals(TelemetryService.getInstance().size(), queueSize + 2);
        TelemetryEvent te = TelemetryService.getInstance().peek();
        JSONObject values = (JSONObject) te.get("Value");
        assertThat("Communication error",
                   values.get("errorCode").toString().compareTo(
                       ErrorCode.NETWORK_ERROR.getMessageCode().toString()) == 0);
      }
      return;
    }
    fail();
  }

  @Test
  public void testInvalidDbOrSchemaOrRole() throws SQLException
  {
    Properties properties = new Properties();

    properties.put("db", "invalid_db");
    properties.put("schema", "invalid_schema");

    Connection connection = getConnection(properties);

    SQLWarning warning = connection.getWarnings();
    assertEquals("01000", warning.getSQLState());
    warning = warning.getNextWarning();
    assertEquals("01000", warning.getSQLState());
    assertEquals(null, warning.getNextWarning());

    connection.clearWarnings();
    assertEquals(null, connection.getWarnings());
  }

  @Test
  public void testConnectViaDataSource() throws SQLException
  {
    SnowflakeBasicDataSource ds = new SnowflakeBasicDataSource();

    Map<String, String> params = getConnectionParameters();
    String account = params.get("account");
    String host = params.get("host");
    String port = params.get("port");
    String user = params.get("user");
    String password = params.get("password");
    String ssl = params.get("ssl");

    String connectStr = String.format("jdbc:snowflake://%s:%s", host, port);

    ds.setUrl(connectStr);
    ds.setAccount(account);
    ds.setSsl("on".equals(ssl));

    Connection connection = ds.getConnection(user, password);
    ResultSet resultSet = connection.createStatement()
        .executeQuery("select 1");
    resultSet.next();
    assertThat("select 1", resultSet.getInt(1), equalTo(1));

    connection.close();

    // get connection by server name
    // this is used by ibm cast iron studio
    ds = new SnowflakeBasicDataSource();
    ds.setServerName(params.get("host"));
    ds.setSsl("on".equals(ssl));
    ds.setAccount(account);
    ds.setPortNumber(Integer.parseInt(port));
    connection = ds.getConnection(params.get("user"), params.get("password"));
    resultSet = connection.createStatement()
        .executeQuery("select 1");
    resultSet.next();
    assertThat("select 1", resultSet.getInt(1), equalTo(1));

    connection.close();
  }

  @Test
  @ConditionalIgnore(condition = RunningOnTravisCI.class)
  public void testConnectUsingKeyPair() throws Exception
  {
    Map<String, String> parameters = getConnectionParameters();
    String testUser = parameters.get("user");

    KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
    SecureRandom random = SecureRandom.getInstance("SHA1PRNG", "SUN");
    keyPairGenerator.initialize(2048, random);

    KeyPair keyPair = keyPairGenerator.generateKeyPair();
    PublicKey publicKey = keyPair.getPublic();
    PrivateKey privateKey = keyPair.getPrivate();

    Connection connection = getConnection();
    Statement statement = connection.createStatement();
    statement.execute("use role accountadmin");

    String encodePublicKey = Base64.encodeBase64String(publicKey.getEncoded());

    statement.execute(String.format(
        "alter user %s set rsa_public_key='%s'", testUser, encodePublicKey));

    connection.close();

    String uri = parameters.get("uri");

    Properties properties = new Properties();
    properties.put("account", parameters.get("account"));
    properties.put("user", testUser);
    properties.put("ssl", parameters.get("ssl"));
    properties.put("port", parameters.get("port"));

    // test correct private key one
    properties.put("privateKey", privateKey);
    connection = DriverManager.getConnection(uri, properties);
    connection.close();

    // test wrong private key
    keyPair = keyPairGenerator.generateKeyPair();
    PublicKey publicKey2 = keyPair.getPublic();
    PrivateKey privateKey2 = keyPair.getPrivate();
    properties.put("privateKey", privateKey2);
    try
    {
      connection = DriverManager.getConnection(uri, properties);
      fail();
    }
    catch (SQLException e)
    {
      Assert.assertEquals(200002, e.getErrorCode());
    }

    // test multiple key pair
    connection = getConnection();
    statement = connection.createStatement();
    statement.execute("use role accountadmin");

    String encodePublicKey2 = Base64.encodeBase64String(publicKey2.getEncoded());

    statement.execute(String.format(
        "alter user %s set rsa_public_key_2='%s'", testUser, encodePublicKey2));
    connection.close();

    connection = DriverManager.getConnection(uri, properties);

    // clean up
    statement = connection.createStatement();
    statement.execute("use role accountadmin");
    statement.execute(String.format("alter user %s unset rsa_public_key", testUser));
    statement.execute(String.format("alter user %s unset rsa_public_key_2", testUser));
    connection.close();
  }

  @Test
  public void testBadPrivateKey() throws Exception
  {
    Map<String, String> parameters = getConnectionParameters();
    String testUser = parameters.get("user");

    String uri = parameters.get("uri");

    Properties properties = new Properties();
    properties.put("account", parameters.get("account"));
    properties.put("user", testUser);
    properties.put("ssl", parameters.get("ssl"));

    KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("DSA");
    PrivateKey dsaPrivateKey = keyPairGenerator.generateKeyPair().getPrivate();

    try
    {
      properties.put("privateKey", "bad string");
      DriverManager.getConnection(uri, properties);
      fail();
    }
    catch (SQLException e)
    {
      assertThat(e.getErrorCode(), is(
          ErrorCode.INVALID_PARAMETER_TYPE.getMessageCode()));
    }

    try
    {
      properties.put("privateKey", dsaPrivateKey);
      DriverManager.getConnection(uri, properties);
      fail();
    }
    catch (SQLException e)
    {
      assertThat(e.getErrorCode(), is(
          ErrorCode.INVALID_OR_UNSUPPORTED_PRIVATE_KEY.getMessageCode()));
    }
  }

  @Test
  @ConditionalIgnore(condition = RunningOnTravisCI.class)
  public void testDifferentKeyLength() throws Exception
  {
    Map<String, String> parameters = getConnectionParameters();
    String testUser = parameters.get("user");

    Integer[] testCases = {2048, 4096, 8192};

    KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
    SecureRandom random = SecureRandom.getInstance("SHA1PRNG", "SUN");

    for (Integer keyLength : testCases)
    {
      keyPairGenerator.initialize(keyLength, random);

      KeyPair keyPair = keyPairGenerator.generateKeyPair();
      PublicKey publicKey = keyPair.getPublic();
      PrivateKey privateKey = keyPair.getPrivate();

      Connection connection = getConnection();
      Statement statement = connection.createStatement();
      statement.execute("use role accountadmin");

      String encodePublicKey = Base64.encodeBase64String(publicKey.getEncoded());

      statement.execute(String.format(
          "alter user %s set rsa_public_key='%s'", testUser, encodePublicKey));

      connection.close();

      String uri = parameters.get("uri");

      Properties properties = new Properties();
      properties.put("account", parameters.get("account"));
      properties.put("user", testUser);
      properties.put("ssl", parameters.get("ssl"));
      properties.put("port", parameters.get("port"));
      properties.put("role", "accountadmin");

      // test correct private key one
      properties.put("privateKey", privateKey);
      connection = DriverManager.getConnection(uri, properties);

      connection.createStatement().execute(
          String.format("alter user %s unset rsa_public_key", testUser));
      connection.close();
    }
  }

  /**
   * Test production connectivity with insecure mode disabled.
   */
  @Test
  public void testInsecureMode() throws SQLException
  {
    String deploymentUrl =
        "jdbc:snowflake://sfcsupport.snowflakecomputing.com";

    Properties properties = new Properties();

    properties.put("user", "fakesuer");
    properties.put("password", "fakepwd");
    properties.put("account", "fakeaccount");
    properties.put("insecureMode", false);
    try
    {
      DriverManager.getConnection(deploymentUrl, properties);
      fail();
    }
    catch (SQLException e)
    {
      assertThat(e.getErrorCode(), is(
          ErrorCode.CONNECTION_ERROR.getMessageCode()));
    }

    deploymentUrl =
        "jdbc:snowflake://sfcsupport.snowflakecomputing.com?insecureMode=false";

    properties = new Properties();

    properties.put("user", "fakesuer");
    properties.put("password", "fakepwd");
    properties.put("account", "fakeaccount");
    try
    {
      DriverManager.getConnection(deploymentUrl, properties);
      fail();
    }
    catch (SQLException e)
    {
      assertThat(e.getErrorCode(), is(
          ErrorCode.CONNECTION_ERROR.getMessageCode()));
    }
  }

  /**
   * Verify the passed memory parameters are set in the session
   */
  @Test
  public void testClientMemoryParameters() throws Exception
  {
    Properties paramProperties = new Properties();
    paramProperties.put("CLIENT_PREFETCH_THREADS", "6");
    paramProperties.put("CLIENT_RESULT_CHUNK_SIZE", 48);
    paramProperties.put("CLIENT_MEMORY_LIMIT", 1000);
    Connection connection = getConnection(paramProperties);

    for (Enumeration<String> enums = (Enumeration<String>) paramProperties.propertyNames();
         enums.hasMoreElements(); )
    {
      String key = enums.nextElement();
      ResultSet rs = connection.createStatement().executeQuery(
          String.format("show parameters like '%s'", key));
      rs.next();
      String value = rs.getString("value");
      assertThat(key, value, equalTo(paramProperties.get(key).toString()));
    }
  }

  /**
   * Verify the JVM memory parameters are set in the session
   */
  @Test
  public void testClientMemoryJvmParameteres() throws Exception
  {
    Properties paramProperties = new Properties();
    paramProperties.put("CLIENT_PREFETCH_THREADS", "6");
    paramProperties.put("CLIENT_RESULT_CHUNK_SIZE", 48);
    paramProperties.put("CLIENT_MEMORY_LIMIT", 1000L);

    // set JVM parameters
    System.setProperty("net.snowflake.jdbc.clientPrefetchThreads",
                       paramProperties.get("CLIENT_PREFETCH_THREADS").toString());
    System.setProperty("net.snowflake.jdbc.clientResultChunkSize",
                       paramProperties.get("CLIENT_RESULT_CHUNK_SIZE").toString());
    System.setProperty("net.snowflake.jdbc.clientMemoryLimit",
                       paramProperties.get("CLIENT_MEMORY_LIMIT").toString());

    try
    {
      Connection connection = getConnection();

      for (Enumeration<String> enums = (Enumeration<String>) paramProperties.propertyNames();
           enums.hasMoreElements(); )
      {
        String key = enums.nextElement();
        ResultSet rs = connection.createStatement().executeQuery(
            String.format("show parameters like '%s'", key));
        rs.next();
        String value = rs.getString("value");
        assertThat(key, value, equalTo(paramProperties.get(key).toString()));
      }
    }
    finally
    {
      System.clearProperty("net.snowflake.jdbc.clientPrefetchThreads");
      System.clearProperty("net.snowflake.jdbc.clientResultChunkSize");
      System.clearProperty("net.snowflake.jdbc.clientMemoryLimit");
    }
  }

  /**
   * Verify the connection and JVM memory parameters are set in the session.
   * The connection parameters take precedence over JVM.
   */
  @Test
  public void testClientMixedMemoryJvmParameteres() throws Exception
  {
    Properties paramProperties = new Properties();
    paramProperties.put("CLIENT_PREFETCH_THREADS", "6");
    paramProperties.put("CLIENT_RESULT_CHUNK_SIZE", 48);
    paramProperties.put("CLIENT_MEMORY_LIMIT", 1000L);

    // set JVM parameters
    System.setProperty("net.snowflake.jdbc.clientPrefetchThreads",
                       paramProperties.get("CLIENT_PREFETCH_THREADS").toString());
    System.setProperty("net.snowflake.jdbc.clientResultChunkSize",
                       paramProperties.get("CLIENT_RESULT_CHUNK_SIZE").toString());
    System.setProperty("net.snowflake.jdbc.clientMemoryLimit",
                       paramProperties.get("CLIENT_MEMORY_LIMIT").toString());

    paramProperties.put("CLIENT_PREFETCH_THREADS", "8");
    paramProperties.put("CLIENT_RESULT_CHUNK_SIZE", 64);
    paramProperties.put("CLIENT_MEMORY_LIMIT", 2000L);

    try
    {
      Connection connection = getConnection(paramProperties);

      for (Enumeration<String> enums = (Enumeration<String>) paramProperties.propertyNames();
           enums.hasMoreElements(); )
      {
        String key = enums.nextElement();
        ResultSet rs = connection.createStatement().executeQuery(
            String.format("show parameters like '%s'", key));
        rs.next();
        String value = rs.getString("value");
        assertThat(key, value, equalTo(paramProperties.get(key).toString()));
      }
    }
    finally
    {
      System.clearProperty("net.snowflake.jdbc.clientPrefetchThreads");
      System.clearProperty("net.snowflake.jdbc.clientResultChunkSize");
      System.clearProperty("net.snowflake.jdbc.clientMemoryLimit");
    }
  }

  @Test
  public void testReadOnly() throws Throwable
  {
    try (Connection connection = getConnection())
    {
      try
      {
        connection.setReadOnly(true);
        fail("must raise SQLFeatureNotSupportedException");
      }
      catch (SQLFeatureNotSupportedException ex)
      {
        // nop
      }

      connection.setReadOnly(false);
      connection.createStatement().execute("create or replace table readonly_test(c1 int)");
      assertFalse(connection.isReadOnly());
      connection.createStatement().execute("drop table if exists readonly_test");
    }
  }

  @Test
  public void testNativeSQL() throws Throwable
  {
    try (Connection connection = getConnection())
    {
      // today returning the source SQL.
      assertEquals("select 1", connection.nativeSQL("select 1"));
    }
  }

  @Test
  public void testGetTypeMap() throws Throwable
  {
    try (Connection connection = getConnection())
    {
      // return an empty type map. setTypeMap is not supported.
      assertEquals(Collections.emptyMap(), connection.getTypeMap());
    }
  }

  @Test
  public void testGetHolderbility() throws Throwable
  {
    try (Connection connection = getConnection())
    {
      // return an empty type map. setTypeMap is not supported.
      assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, connection.getHoldability());
    }
  }

  @Test
  public void testIsValid() throws Throwable
  {
    try (Connection connection = getConnection())
    {
      assertTrue(connection.isValid(10));
      try
      {
        assertTrue(connection.isValid(-10));
        fail("must fail");
      }
      catch (SQLException ex)
      {
        // nop, no specific error code is provided.
      }
    }
  }

  @Test
  public void testClientInfo() throws Throwable
  {
    try (Connection connection = getConnection())
    {
      // no name or value validation is in place.
      // in fact, nop.
      Properties props = new Properties();
      props.setProperty("name", "value");
      connection.setClientInfo(props);
      connection.setClientInfo("name2", "value2");
      Properties retProps = connection.getClientInfo();
      assertEquals(2, retProps.size());
      assertEquals("value", props.getProperty("name"));
      assertEquals("value", connection.getClientInfo("name"));
    }
  }

  @Test
  public void testWrapper() throws Throwable
  {
    try (Connection connection = getConnection())
    {
      boolean canUnwrap = connection.isWrapperFor(SnowflakeConnectionV1.class);
      assertTrue(canUnwrap);
      if (canUnwrap)
      {
        SnowflakeConnectionV1 sfconnection = connection.unwrap(SnowflakeConnectionV1.class);
        sfconnection.createStatement();
      }
      else
      {
        fail("should be able to unwrap");
      }
      try
      {
        connection.unwrap(SnowflakeDriver.class);
        fail("should fail to cast");
      }
      catch (SQLException ex)
      {
        // nop
      }
    }
  }

  @Test
  public void testVersion() throws Throwable
  {
    try (Connection connection = getConnection())
    {
    }
  }

}