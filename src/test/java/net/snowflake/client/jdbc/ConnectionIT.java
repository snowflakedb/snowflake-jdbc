/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import net.snowflake.client.ConditionalIgnoreRule.ConditionalIgnore;
import net.snowflake.client.RunningOnTravisCI;
import net.snowflake.common.core.SqlState;
import org.apache.commons.codec.binary.Base64;
import org.junit.Assert;
import org.junit.Ignore;
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
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Connection integration tests
 */
public class ConnectionIT extends BaseJDBCTest
{
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
  }

  /**
   * Test that login timeout kick in when connect through datasource
   *
   * @throws SQLException
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

  @Ignore("not supported yet")
  @Test
  public void testReadOnlyConneciton() throws SQLException
  {
    Connection con = getConnection();
    assertTrue(!con.isReadOnly());
    con.setReadOnly(true);
    assertTrue(con.isReadOnly());
    Statement statement = con.createStatement();
    statement.execute("create or replace table TEST_JDBC_READ_ONLY(colA VARCHAR)");
    statement.execute("drop table if exists TEST_JDBC_READ_ONLY");
    statement.close();
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
  public void testLoginTimeout() throws SQLException
  {
    long connStart = 0, conEnd;
    Properties properties = new Properties();
    properties.put("account", "wrongaccount");
    properties.put("loginTimeout", "20");
    properties.put("user", "fakeuser");
    properties.put("password", "fakepassword");
    try
    {
      connStart = System.currentTimeMillis();
      DriverManager.getConnection(
          "jdbc:snowflake://wrongaccount.snowflakecomputing.com", properties);
    }
    catch (SQLException e)
    {
      assertThat("Communication error", e.getErrorCode(),
          equalTo(ErrorCode.NETWORK_ERROR.getMessageCode()));

      conEnd = System.currentTimeMillis();
      assertThat("Login time out not taking effective",
          conEnd - connStart < 60000);
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
    try
    {
      connStart = System.currentTimeMillis();
      DriverManager.getConnection(
          "jdbc:snowflake://testaccount.wronghostname.com", properties);
    }
    catch (SQLException e)
    {
      assertThat("Communication error", e.getErrorCode(),
          equalTo(ErrorCode.NETWORK_ERROR.getMessageCode()));

      conEnd = System.currentTimeMillis();
      assertThat("Login time out not taking effective",
          conEnd - connStart < 60000);
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
    ds.setSsl("on". equals(ssl));
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
}