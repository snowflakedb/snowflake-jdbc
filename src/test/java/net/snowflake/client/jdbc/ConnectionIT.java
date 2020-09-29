/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static net.snowflake.client.core.QueryStatus.RUNNING;
import static net.snowflake.client.core.SessionUtil.CLIENT_SESSION_KEEP_ALIVE_HEARTBEAT_FREQUENCY;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.*;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.*;
import java.sql.*;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import net.snowflake.client.ConditionalIgnoreRule.ConditionalIgnore;
import net.snowflake.client.RunningNotOnTestaccount;
import net.snowflake.client.RunningOnGithubAction;
import net.snowflake.client.category.TestCategoryConnection;
import net.snowflake.client.core.QueryStatus;
import net.snowflake.client.jdbc.telemetryOOB.TelemetryService;
import net.snowflake.common.core.SqlState;
import org.apache.commons.codec.binary.Base64;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

/** Connection integration tests */
@Category(TestCategoryConnection.class)
public class ConnectionIT extends BaseJDBCTest {
  // create a local constant for this code for testing purposes (already defined in GS)
  private static final int INVALID_CONNECTION_INFO_CODE = 390100;
  private static final int SESSION_CREATION_OBJECT_DOES_NOT_EXIST_NOT_AUTHORIZED = 390201;
  private static final int ROLE_IN_CONNECT_STRING_DOES_NOT_EXIST = 390189;

  private static final int WAIT_FOR_TELEMETRY_REPORT_IN_MILLISECS = 5000;

  String errorMessage = null;

  private boolean defaultState;

  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Before
  public void setUp() {
    TelemetryService service = TelemetryService.getInstance();
    service.updateContextForIT(getConnectionParameters());
    defaultState = service.isEnabled();
    service.setNumOfRetryToTriggerTelemetry(3);
    service.enable();
  }

  @After
  public void tearDown() throws InterruptedException {
    TelemetryService service = TelemetryService.getInstance();
    // wait 5 seconds while the service is flushing
    TimeUnit.SECONDS.sleep(5);

    if (defaultState) {
      service.enable();
    } else {
      service.disable();
    }
    service.resetNumOfRetryToTriggerTelemetry();
  }

  @Test
  public void testSimpleConnection() throws SQLException {
    Connection con = getConnection();
    Statement statement = con.createStatement();
    ResultSet resultSet = statement.executeQuery("show parameters");
    assertTrue(resultSet.next());
    assertFalse(con.isClosed());
    statement.close();
    con.close();
    assertTrue(con.isClosed());
    con.close(); // ensure no exception
  }

  @Test
  public void testError() throws SQLException {
    TimeZone.setDefault(TimeZone.getTimeZone("CET"));
    Connection con = getConnection();
    Statement statement = con.createStatement();
    statement.execute("alter session set jdbc_query_result_format = 'JSON'");
    statement.execute("alter session set timezone = 'CET'");
    ResultSet rs2 = statement.executeQuery("show parameters like 'timezone'");
    rs2.next();
    System.out.println("System timezone: " + rs2.getString(2));
    ResultSet rs =
        statement.executeQuery(
            "SELECT DATE '2019-04-06 00:00:00' as datefield, TIMESTAMP '2019-04-06 00:00:00' as timestampfield");

    // We are using a calendar with UTC time zone to retrieve dates and timestamps
    Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

    rs.next();

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    sdf.setTimeZone(cal.getTimeZone());

    System.out.println("TEST 1: getDate and getTimestamp with calendar option");

    Date d_01 = rs.getDate(1, cal);
    Timestamp t_01 = rs.getTimestamp(2, cal);

    System.out.println("getDate:(1, cal): " + sdf.format(d_01));
    System.out.println("getTimestamp(2, cal): " + sdf.format(t_01));

    System.out.println("TEST 2: getTimestamp for both columns with calendar option");

    Timestamp d_02 = rs.getTimestamp(1, cal);
    Timestamp t_02 = rs.getTimestamp(2, cal);

    System.out.println("getTimestamp:(1, cal): " + sdf.format(d_02));
    System.out.println("getTimestamp(2, cal): " + sdf.format(t_02));

    System.out.println("TEST 3: getDate and getTimestamp and NO calendar option");

    Date d_03 = rs.getDate(1);
    Timestamp t_03 = rs.getTimestamp(2);

    System.out.println("getDate:(1): " + sdf.format(d_03));
    System.out.println("getTimestamp(2): " + sdf.format(t_03));

    statement.close();
    con.close();
  }

  @Test
  @Ignore
  public void test300ConnectionsWithSingleClientInstance() throws SQLException {
    // concurrent testing
    int size = 300;
    Connection con = getConnection();
    String database = con.getCatalog();
    String schema = con.getSchema();
    con.createStatement()
        .execute(
            "create or replace table bigTable(rowNum number,rando "
                + "number) as (select seq4(),"
                + "uniform(1, 10, random()) from table(generator(rowcount=>10000000)) v)");
    con.createStatement().execute("create or replace table conTable(colA number)");

    ExecutorService taskRunner = Executors.newFixedThreadPool(size);
    for (int i = 0; i < size; i++) {
      ConcurrentConnections newTask = new ConcurrentConnections();
      taskRunner.submit(newTask);
    }
    assertEquals(null, errorMessage);
    taskRunner.shutdownNow();
  }

  /**
   * Test that login timeout kick in when connect through datasource
   *
   * @throws SQLException if any SQL error occurs
   */
  @Test
  public void testLoginTimeoutViaDataSource() throws SQLException {
    SnowflakeBasicDataSource ds = new SnowflakeBasicDataSource();
    ds.setUrl("jdbc:snowflake://fakeaccount.snowflakecomputing.com");
    ds.setUser("fakeUser");
    ds.setPassword("fakePassword");
    ds.setAccount("fakeAccount");
    ds.setLoginTimeout(10);

    long startLoginTime = System.currentTimeMillis();
    try {
      ds.getConnection();
      fail();
    } catch (SQLException e) {
      assertThat(e.getErrorCode(), is(ErrorCode.NETWORK_ERROR.getMessageCode()));
    }
    long endLoginTime = System.currentTimeMillis();

    assertTrue(endLoginTime - startLoginTime < 30000);
  }

  /**
   * Test production connectivity in case cipher suites or tls protocol change Use fake username and
   * password but correct url Expectation is receiving incorrect username or password response from
   * server
   */
  @Test
  public void testProdConnectivity() throws SQLException {
    String[] deploymentUrls = {
      "jdbc:snowflake://sfcsupport.snowflakecomputing.com",
      "jdbc:snowflake://sfcsupportva.us-east-1.snowflakecomputing.com",
      "jdbc:snowflake://sfcsupporteu.eu-central-1.snowflakecomputing.com"
    };

    Properties properties = new Properties();

    properties.put("user", "fakesuer");
    properties.put("password", "fakepwd");
    properties.put("account", "fakeaccount");

    for (String url : deploymentUrls) {
      try {
        DriverManager.getConnection(url, properties);
        fail();
      } catch (SQLException e) {
        assertThat(e.getErrorCode(), is(INVALID_CONNECTION_INFO_CODE));
      }
    }
  }

  @Test
  public void testSetCatalogSchema() throws Throwable {
    try (Connection connection = getConnection()) {
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
  @ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testConnectionGetAndSetDBAndSchema() throws SQLException {
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
  public void testConnectionClientInfo() throws SQLException {
    try (Connection con = getConnection()) {
      Properties property = con.getClientInfo();
      assertEquals(0, property.size());
      Properties clientInfo = new Properties();
      clientInfo.setProperty("name", "Peter");
      clientInfo.setProperty("description", "SNOWFLAKE JDBC");
      try {
        con.setClientInfo(clientInfo);
        fail("setClientInfo should fail for any parameter.");
      } catch (SQLClientInfoException e) {
        assertEquals(SqlState.INVALID_PARAMETER_VALUE, e.getSQLState());
        assertEquals(200047, e.getErrorCode());
        assertEquals(2, e.getFailedProperties().size());
      }
      try {
        con.setClientInfo("ApplicationName", "valueA");
        fail("setClientInfo should fail for any parameter.");
      } catch (SQLClientInfoException e) {
        assertEquals(SqlState.INVALID_PARAMETER_VALUE, e.getSQLState());
        assertEquals(200047, e.getErrorCode());
        assertEquals(1, e.getFailedProperties().size());
      }
    }
  }

  // only support get and set
  @Test
  public void testNetworkTimeout() throws SQLException {
    Connection con = getConnection();
    int millis = con.getNetworkTimeout();
    assertEquals(0, millis);
    con.setNetworkTimeout(null, 200);
    assertEquals(200, con.getNetworkTimeout());
    con.close();
  }

  @Test
  public void testAbort() throws SQLException {
    Connection con = getConnection();
    assertTrue(!con.isClosed());
    con.abort(null);
    assertTrue(con.isClosed());
  }

  @Test
  public void testSetQueryTimeoutInConnectionStr() throws SQLException {
    Properties properties = new Properties();
    properties.put("queryTimeout", "5");
    Connection connection = getConnection(properties);
    Statement statement = connection.createStatement();
    try {
      statement.executeQuery("select count(*) from table(generator(timeLimit => 1000000))");
    } catch (SQLException e) {
      assertTrue(true);
      assertEquals(SqlState.QUERY_CANCELED, e.getSQLState());
      assertEquals("SQL execution canceled", e.getMessage());
    }
    statement.close();
    connection.close();
  }

  @Test
  public void testHttpsLoginTimeoutWithSSL() throws InterruptedException {
    long connStart = 0, conEnd;
    Properties properties = new Properties();
    properties.put("account", "wrongaccount");
    properties.put("loginTimeout", "5");
    properties.put("user", "fakeuser");
    properties.put("password", "fakepassword");
    // only when ssl is on can trigger the login timeout
    // ssl is off will trigger 404
    properties.put("ssl", "on");
    try {
      connStart = System.currentTimeMillis();
      Map<String, String> params = getConnectionParameters();
      // use wrongaccount in url
      String host = params.get("host");
      String[] hostItems = host.split("\\.");
      String wrongUri = params.get("uri").replace("://" + hostItems[0], "://wrongaccount");
      DriverManager.getConnection(wrongUri, properties);
    } catch (SQLException e) {
      assertThat(
          "Communication error",
          e.getErrorCode(),
          equalTo(ErrorCode.NETWORK_ERROR.getMessageCode()));

      conEnd = System.currentTimeMillis();
      assertThat("Login time out not taking effective", conEnd - connStart < 60000);
      Thread.sleep(WAIT_FOR_TELEMETRY_REPORT_IN_MILLISECS);
      if (TelemetryService.getInstance().isDeploymentEnabled()) {
        assertThat(
            "Telemetry event has not been reported successfully. Error: "
                + TelemetryService.getInstance().getLastClientError(),
            TelemetryService.getInstance().getClientFailureCount(),
            equalTo(0));
      }
      return;
    }
    fail();
  }

  @Test
  public void testHttpsLoginTimeoutWithOutSSL() throws InterruptedException {
    Properties properties = new Properties();
    properties.put("account", "wrongaccount");
    properties.put("loginTimeout", "20");
    properties.put("user", "fakeuser");
    properties.put("password", "fakepassword");
    properties.put("ssl", "off");
    int count = TelemetryService.getInstance().getEventCount();
    try {
      Map<String, String> params = getConnectionParameters();
      // use wrongaccount in url
      String host = params.get("host");
      String[] hostItems = host.split("\\.");
      String wrongUri = params.get("uri").replace("://" + hostItems[0], "://wrongaccount");

      DriverManager.getConnection(wrongUri, properties);
    } catch (SQLException e) {
      if (TelemetryService.getInstance()
              .getServerDeploymentName()
              .equals(TelemetryService.TELEMETRY_SERVER_DEPLOYMENT.DEV.getName())
          || TelemetryService.getInstance()
              .getServerDeploymentName()
              .equals(TelemetryService.TELEMETRY_SERVER_DEPLOYMENT.REG.getName())) {
        // a connection error response (wrong user and password)
        // with status code 200 is returned in RT
        assertThat("Communication error", e.getErrorCode(), equalTo(INVALID_CONNECTION_INFO_CODE));

        // since it returns normal response,
        // the telemetry does not create new event
        Thread.sleep(WAIT_FOR_TELEMETRY_REPORT_IN_MILLISECS);
        if (TelemetryService.getInstance().isDeploymentEnabled()) {
          assertThat(
              "Telemetry should not create new event",
              TelemetryService.getInstance().getEventCount(),
              equalTo(count));
        }
      } else {
        // in qa1 and others, 404 http status code should be returned
        assertThat(
            "Communication error",
            e.getErrorCode(),
            equalTo(ErrorCode.NETWORK_ERROR.getMessageCode()));

        if (TelemetryService.getInstance().isDeploymentEnabled()) {
          assertThat(
              "Telemetry event has not been reported successfully. Error: "
                  + TelemetryService.getInstance().getLastClientError(),
              TelemetryService.getInstance().getClientFailureCount(),
              equalTo(0));
        }
      }
      return;
    }
    fail();
  }

  @Test
  public void testWrongHostNameTimeout() throws InterruptedException {
    long connStart = 0, conEnd;
    Properties properties = new Properties();
    properties.put("account", "testaccount");
    properties.put("loginTimeout", "20");
    properties.put("user", "fakeuser");
    properties.put("password", "fakepassword");
    try {
      connStart = System.currentTimeMillis();
      Map<String, String> params = getConnectionParameters();
      // use wrongaccount in url
      String host = params.get("host");
      String[] hostItems = host.split("\\.");
      String wrongUri =
          params.get("uri").replace("." + hostItems[hostItems.length - 2] + ".", ".wronghostname.");

      DriverManager.getConnection(wrongUri, properties);
    } catch (SQLException e) {
      assertThat(
          "Communication error",
          e.getErrorCode(),
          equalTo(ErrorCode.NETWORK_ERROR.getMessageCode()));

      conEnd = System.currentTimeMillis();
      assertThat("Login time out not taking effective", conEnd - connStart < 60000);

      Thread.sleep(WAIT_FOR_TELEMETRY_REPORT_IN_MILLISECS);
      if (TelemetryService.getInstance().isDeploymentEnabled()) {
        assertThat(
            "Telemetry event has not been reported successfully. Error: "
                + TelemetryService.getInstance().getLastClientError(),
            TelemetryService.getInstance().getClientFailureCount(),
            equalTo(0));
      }
      return;
    }
    fail();
  }

  @Test
  public void testConnectViaDataSource() throws SQLException {
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
    ResultSet resultSet = connection.createStatement().executeQuery("select 1");
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
    resultSet = connection.createStatement().executeQuery("select 1");
    resultSet.next();
    assertThat("select 1", resultSet.getInt(1), equalTo(1));

    connection.close();
  }

  @Test
  @ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testBasicDataSourceSerialization() throws Exception {
    // test with username/password authentication
    // set up DataSource object and ensure connection works
    Map<String, String> params = getConnectionParameters();
    SnowflakeBasicDataSource ds = new SnowflakeBasicDataSource();
    ds.setServerName(params.get("host"));
    ds.setSsl("on".equals(params.get("ssl")));
    ds.setAccount(params.get("account"));
    ds.setPortNumber(Integer.parseInt(params.get("port")));
    ds.setUser(params.get("user"));
    ds.setPassword(params.get("password"));
    Connection con = ds.getConnection();
    ResultSet resultSet = con.createStatement().executeQuery("select 1");
    resultSet.next();
    assertThat("select 1", resultSet.getInt(1), equalTo(1));
    con.close();
    File serializedFile = tmpFolder.newFile("serializedStuff.ser");
    // serialize datasource object into a file
    FileOutputStream outputFile = new FileOutputStream(serializedFile);
    ObjectOutputStream out = new ObjectOutputStream(outputFile);
    out.writeObject(ds);
    out.close();
    outputFile.close();
    // deserialize into datasource object again
    FileInputStream inputFile = new FileInputStream(serializedFile);
    ObjectInputStream in = new ObjectInputStream(inputFile);
    SnowflakeBasicDataSource ds2 = (SnowflakeBasicDataSource) in.readObject();
    in.close();
    inputFile.close();
    // test connection a second time
    con = ds2.getConnection();
    resultSet = con.createStatement().executeQuery("select 1");
    resultSet.next();
    assertThat("select 1", resultSet.getInt(1), equalTo(1));
    con.close();
  }

  @Test
  @ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testKeyPairFileDataSourceSerialization() throws Exception {
    // test with key/pair authentication where key is in file
    // set up DataSource object and ensure connection works
    Map<String, String> params = getConnectionParameters();
    SnowflakeBasicDataSource ds = new SnowflakeBasicDataSource();
    ds.setServerName(params.get("host"));
    ds.setSsl("on".equals(params.get("ssl")));
    ds.setAccount(params.get("account"));
    ds.setPortNumber(Integer.parseInt(params.get("port")));
    ds.setUser(params.get("user"));
    String privateKeyLocation = getFullPathFileInResource("encrypted_rsa_key.p8");
    ds.setPrivateKeyFile(privateKeyLocation, "test");

    // set up public key
    try (Connection con = getConnection()) {
      Statement statement = con.createStatement();
      statement.execute("use role accountadmin");
      String pathfile = getFullPathFileInResource("encrypted_rsa_key.pub");
      String pubKey = new String(Files.readAllBytes(Paths.get(pathfile)));
      pubKey = pubKey.replace("-----BEGIN PUBLIC KEY-----", "");
      pubKey = pubKey.replace("-----END PUBLIC KEY-----", "");
      statement.execute(
          String.format("alter user %s set rsa_public_key='%s'", params.get("user"), pubKey));
    }

    Connection con = ds.getConnection();
    ResultSet resultSet = con.createStatement().executeQuery("select 1");
    resultSet.next();
    assertThat("select 1", resultSet.getInt(1), equalTo(1));
    con.close();
    File serializedFile = tmpFolder.newFile("serializedStuff.ser");
    // serialize datasource object into a file
    FileOutputStream outputFile = new FileOutputStream(serializedFile);
    ObjectOutputStream out = new ObjectOutputStream(outputFile);
    out.writeObject(ds);
    out.close();
    outputFile.close();
    // deserialize into datasource object again
    FileInputStream inputFile = new FileInputStream(serializedFile);
    ObjectInputStream in = new ObjectInputStream(inputFile);
    SnowflakeBasicDataSource ds2 = (SnowflakeBasicDataSource) in.readObject();
    in.close();
    inputFile.close();
    // test connection a second time
    con = ds2.getConnection();
    resultSet = con.createStatement().executeQuery("select 1");
    resultSet.next();
    assertThat("select 1", resultSet.getInt(1), equalTo(1));
    con.close();

    // clean up
    try (Connection connection = getConnection()) {
      Statement statement = connection.createStatement();
      statement.execute("use role accountadmin");
      statement.execute(String.format("alter user %s unset rsa_public_key", params.get("user")));
    }
  }

  @Test
  @Ignore
  public void testDataSourceOktaSerialization() throws Exception {
    // test with username/password authentication
    // set up DataSource object and ensure connection works
    Map<String, String> params = getConnectionParameters();
    SnowflakeBasicDataSource ds = new SnowflakeBasicDataSource();
    ds.setServerName(params.get("host"));
    ds.setSsl("on".equals(params.get("ssl")));
    ds.setAccount(params.get("account"));
    ds.setPortNumber(Integer.parseInt(params.get("port")));
    ds.setUser(params.get("ssoUser"));
    ds.setPassword(params.get("ssoPassword"));
    ds.setAuthenticator("https://snowflakecomputing.okta.com/");
    Connection con = ds.getConnection();
    ResultSet resultSet = con.createStatement().executeQuery("select 1");
    resultSet.next();
    assertThat("select 1", resultSet.getInt(1), equalTo(1));
    File serializedFile = tmpFolder.newFile("serializedStuff.ser");
    // serialize datasource object into a file
    FileOutputStream outputFile = new FileOutputStream(serializedFile);
    ObjectOutputStream out = new ObjectOutputStream(outputFile);
    out.writeObject(ds);
    out.close();
    outputFile.close();
    // deserialize into datasource object again
    FileInputStream inputFile = new FileInputStream(serializedFile);
    ObjectInputStream in = new ObjectInputStream(inputFile);
    SnowflakeBasicDataSource ds2 = (SnowflakeBasicDataSource) in.readObject();
    in.close();
    inputFile.close();
    // test connection a second time
    con = ds2.getConnection();
    resultSet = con.createStatement().executeQuery("select 1");
    resultSet.next();
    assertThat("select 1", resultSet.getInt(1), equalTo(1));
    con.close();
  }

  @Test
  @ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testConnectUsingKeyPair() throws Exception {
    Map<String, String> parameters = getConnectionParameters();
    String testUser = parameters.get("user");

    KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
    SecureRandom random = SecureRandom.getInstance("SHA1PRNG");
    keyPairGenerator.initialize(2048, random);

    KeyPair keyPair = keyPairGenerator.generateKeyPair();
    PublicKey publicKey = keyPair.getPublic();
    PrivateKey privateKey = keyPair.getPrivate();

    Connection connection = getConnection();
    Statement statement = connection.createStatement();
    statement.execute("use role accountadmin");

    String encodePublicKey = Base64.encodeBase64String(publicKey.getEncoded());

    statement.execute(
        String.format("alter user %s set rsa_public_key='%s'", testUser, encodePublicKey));

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

    // test datasource connection using private key
    SnowflakeBasicDataSource ds = new SnowflakeBasicDataSource();
    ds.setUrl(uri);
    ds.setAccount(parameters.get("account"));
    ds.setUser(parameters.get("user"));
    ds.setSsl("on".equals(parameters.get("ssl")));
    ds.setPortNumber(Integer.valueOf(parameters.get("port")));
    ds.setPrivateKey(privateKey);
    Connection con = ds.getConnection();
    con.close();

    // test wrong private key
    keyPair = keyPairGenerator.generateKeyPair();
    PublicKey publicKey2 = keyPair.getPublic();
    PrivateKey privateKey2 = keyPair.getPrivate();
    properties.put("privateKey", privateKey2);
    try {
      DriverManager.getConnection(uri, properties);
      fail();
    } catch (SQLException e) {
      Assert.assertEquals(390144, e.getErrorCode());
    }

    // test multiple key pair
    connection = getConnection();
    statement = connection.createStatement();
    statement.execute("use role accountadmin");

    String encodePublicKey2 = Base64.encodeBase64String(publicKey2.getEncoded());

    statement.execute(
        String.format("alter user %s set rsa_public_key_2='%s'", testUser, encodePublicKey2));
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
  @ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testPrivateKeyInConnectionString() throws SQLException, IOException {
    Map<String, String> parameters = getConnectionParameters();
    String testUser = parameters.get("user");

    // Test with non-password-protected private key file (.pem)
    Connection connection = getConnection();
    Statement statement = connection.createStatement();
    statement.execute("use role accountadmin");
    String pathfile = getFullPathFileInResource("rsa_key.pub");
    String pubKey = new String(Files.readAllBytes(Paths.get(pathfile)));
    pubKey = pubKey.replace("-----BEGIN PUBLIC KEY-----", "");
    pubKey = pubKey.replace("-----END PUBLIC KEY-----", "");
    statement.execute(String.format("alter user %s set rsa_public_key='%s'", testUser, pubKey));
    connection.close();

    // PKCS #8
    String privateKeyLocation = getFullPathFileInResource("rsa_key.p8");
    String uri = parameters.get("uri") + "/?private_key_file=" + privateKeyLocation;
    Properties properties = new Properties();
    properties.put("account", parameters.get("account"));
    properties.put("user", testUser);
    properties.put("ssl", parameters.get("ssl"));
    properties.put("port", parameters.get("port"));
    connection = DriverManager.getConnection(uri, properties);
    connection.close();

    // PKCS #1
    privateKeyLocation = getFullPathFileInResource("rsa_key.pem");
    uri = parameters.get("uri") + "/?private_key_file=" + privateKeyLocation;
    properties = new Properties();
    properties.put("account", parameters.get("account"));
    properties.put("user", testUser);
    properties.put("ssl", parameters.get("ssl"));
    properties.put("port", parameters.get("port"));
    connection = DriverManager.getConnection(uri, properties);
    connection.close();

    // test with password-protected private key file (.p8)
    connection = getConnection();
    statement = connection.createStatement();
    statement.execute("use role accountadmin");
    pathfile = getFullPathFileInResource("encrypted_rsa_key.pub");
    pubKey = new String(Files.readAllBytes(Paths.get(pathfile)));
    pubKey = pubKey.replace("-----BEGIN PUBLIC KEY-----", "");
    pubKey = pubKey.replace("-----END PUBLIC KEY-----", "");
    statement.execute(String.format("alter user %s set rsa_public_key='%s'", testUser, pubKey));
    connection.close();

    privateKeyLocation = getFullPathFileInResource("encrypted_rsa_key.p8");
    uri =
        parameters.get("uri")
            + "/?private_key_file_pwd=test&private_key_file="
            + privateKeyLocation;

    connection = DriverManager.getConnection(uri, properties);
    connection.close();

    // test with incorrect password for private key
    uri =
        parameters.get("uri")
            + "/?private_key_file_pwd=wrong_password&private_key_file="
            + privateKeyLocation;
    try {
      connection = DriverManager.getConnection(uri, properties);
      fail();
    } catch (SQLException e) {
      assertEquals(
          (int) ErrorCode.INVALID_OR_UNSUPPORTED_PRIVATE_KEY.getMessageCode(), e.getErrorCode());
    }
    connection.close();

    // test with invalid public/private key combo (using 1st public key with 2nd private key)
    connection = getConnection();
    statement = connection.createStatement();
    statement.execute("use role accountadmin");
    pathfile = getFullPathFileInResource("rsa_key.pub");
    pubKey = new String(Files.readAllBytes(Paths.get(pathfile)));
    pubKey = pubKey.replace("-----BEGIN PUBLIC KEY-----", "");
    pubKey = pubKey.replace("-----END PUBLIC KEY-----", "");
    statement.execute(String.format("alter user %s set rsa_public_key='%s'", testUser, pubKey));
    connection.close();

    privateKeyLocation = getFullPathFileInResource("encrypted_rsa_key.p8");
    uri =
        parameters.get("uri")
            + "/?private_key_file_pwd=test&private_key_file="
            + privateKeyLocation;
    try {
      connection = DriverManager.getConnection(uri, properties);
      fail();
    } catch (SQLException e) {
      assertEquals(390144, e.getErrorCode());
    }
    connection.close();

    // test with invalid private key
    privateKeyLocation = getFullPathFileInResource("invalid_private_key.pem");
    uri = parameters.get("uri") + "/?private_key_file=" + privateKeyLocation;
    try {
      connection = DriverManager.getConnection(uri, properties);
      fail();
    } catch (SQLException e) {
      assertEquals(
          (int) ErrorCode.INVALID_OR_UNSUPPORTED_PRIVATE_KEY.getMessageCode(), e.getErrorCode());
    }
    connection.close();

    // clean up
    connection = getConnection();
    statement = connection.createStatement();
    statement.execute("use role accountadmin");
    statement.execute(String.format("alter user %s unset rsa_public_key", testUser));
    connection.close();
  }

  @Test
  public void testBadPrivateKey() throws Exception {
    Map<String, String> parameters = getConnectionParameters();
    String testUser = parameters.get("user");

    String uri = parameters.get("uri");

    Properties properties = new Properties();
    properties.put("account", parameters.get("account"));
    properties.put("user", testUser);
    properties.put("ssl", parameters.get("ssl"));

    KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("DSA");
    PrivateKey dsaPrivateKey = keyPairGenerator.generateKeyPair().getPrivate();

    try {
      properties.put("privateKey", "bad string");
      DriverManager.getConnection(uri, properties);
      fail();
    } catch (SQLException e) {
      assertThat(e.getErrorCode(), is(ErrorCode.INVALID_PARAMETER_TYPE.getMessageCode()));
    }

    try {
      properties.put("privateKey", dsaPrivateKey);
      DriverManager.getConnection(uri, properties);
      fail();
    } catch (SQLException e) {
      assertThat(
          e.getErrorCode(), is(ErrorCode.INVALID_OR_UNSUPPORTED_PRIVATE_KEY.getMessageCode()));
    }
  }

  @Test
  @ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testDifferentKeyLength() throws Exception {
    Map<String, String> parameters = getConnectionParameters();
    String testUser = parameters.get("user");

    Integer[] testCases = {2048, 4096, 8192};

    KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
    SecureRandom random = SecureRandom.getInstance("SHA1PRNG");

    for (Integer keyLength : testCases) {
      keyPairGenerator.initialize(keyLength, random);

      KeyPair keyPair = keyPairGenerator.generateKeyPair();
      PublicKey publicKey = keyPair.getPublic();
      PrivateKey privateKey = keyPair.getPrivate();

      Connection connection = getConnection();
      Statement statement = connection.createStatement();
      statement.execute("use role accountadmin");

      String encodePublicKey = Base64.encodeBase64String(publicKey.getEncoded());

      statement.execute(
          String.format("alter user %s set rsa_public_key='%s'", testUser, encodePublicKey));

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

      connection
          .createStatement()
          .execute(String.format("alter user %s unset rsa_public_key", testUser));
      connection.close();
    }
  }

  /** Test production connectivity with insecure mode enabled. */
  @Test
  public void testInsecureMode() throws SQLException {
    String deploymentUrl = "jdbc:snowflake://sfcsupport.snowflakecomputing.com";

    Properties properties = new Properties();

    properties.put("user", "fakesuer");
    properties.put("password", "fakepwd");
    properties.put("account", "fakeaccount");
    properties.put("insecureMode", true);
    try {
      DriverManager.getConnection(deploymentUrl, properties);
      fail();
    } catch (SQLException e) {
      assertThat(e.getErrorCode(), is(INVALID_CONNECTION_INFO_CODE));
    }

    deploymentUrl = "jdbc:snowflake://sfcsupport.snowflakecomputing.com?insecureMode=true";

    properties = new Properties();

    properties.put("user", "fakesuer");
    properties.put("password", "fakepwd");
    properties.put("account", "fakeaccount");
    try {
      DriverManager.getConnection(deploymentUrl, properties);
      fail();
    } catch (SQLException e) {
      assertThat(e.getErrorCode(), is(INVALID_CONNECTION_INFO_CODE));
    }
  }

  /** Verify the passed memory parameters are set in the session */
  @Test
  public void testClientMemoryParameters() throws Exception {
    Properties paramProperties = new Properties();
    paramProperties.put("CLIENT_PREFETCH_THREADS", "6");
    paramProperties.put("CLIENT_RESULT_CHUNK_SIZE", 48);
    paramProperties.put("CLIENT_MEMORY_LIMIT", 1000);
    Connection connection = getConnection(paramProperties);

    for (Enumeration<?> enums = paramProperties.propertyNames(); enums.hasMoreElements(); ) {
      String key = (String) enums.nextElement();
      ResultSet rs =
          connection
              .createStatement()
              .executeQuery(String.format("show parameters like '%s'", key));
      rs.next();
      String value = rs.getString("value");
      assertThat(key, value, equalTo(paramProperties.get(key).toString()));
    }
  }

  /** Verify the JVM memory parameters are set in the session */
  @Test
  public void testClientMemoryJvmParameteres() throws Exception {
    Properties paramProperties = new Properties();
    paramProperties.put("CLIENT_PREFETCH_THREADS", "6");
    paramProperties.put("CLIENT_RESULT_CHUNK_SIZE", 48);
    paramProperties.put("CLIENT_MEMORY_LIMIT", 1000L);

    // set JVM parameters
    System.setProperty(
        "net.snowflake.jdbc.clientPrefetchThreads",
        paramProperties.get("CLIENT_PREFETCH_THREADS").toString());
    System.setProperty(
        "net.snowflake.jdbc.clientResultChunkSize",
        paramProperties.get("CLIENT_RESULT_CHUNK_SIZE").toString());
    System.setProperty(
        "net.snowflake.jdbc.clientMemoryLimit",
        paramProperties.get("CLIENT_MEMORY_LIMIT").toString());

    try {
      Connection connection = getConnection();

      for (Enumeration<?> enums = paramProperties.propertyNames(); enums.hasMoreElements(); ) {
        String key = (String) enums.nextElement();
        ResultSet rs =
            connection
                .createStatement()
                .executeQuery(String.format("show parameters like '%s'", key));
        rs.next();
        String value = rs.getString("value");
        assertThat(key, value, equalTo(paramProperties.get(key).toString()));
      }
    } finally {
      System.clearProperty("net.snowflake.jdbc.clientPrefetchThreads");
      System.clearProperty("net.snowflake.jdbc.clientResultChunkSize");
      System.clearProperty("net.snowflake.jdbc.clientMemoryLimit");
    }
  }

  /**
   * Verify the connection and JVM memory parameters are set in the session. The connection
   * parameters take precedence over JVM.
   */
  @Test
  public void testClientMixedMemoryJvmParameteres() throws Exception {
    Properties paramProperties = new Properties();
    paramProperties.put("CLIENT_PREFETCH_THREADS", "6");
    paramProperties.put("CLIENT_RESULT_CHUNK_SIZE", 48);
    paramProperties.put("CLIENT_MEMORY_LIMIT", 1000L);

    // set JVM parameters
    System.setProperty(
        "net.snowflake.jdbc.clientPrefetchThreads",
        paramProperties.get("CLIENT_PREFETCH_THREADS").toString());
    System.setProperty(
        "net.snowflake.jdbc.clientResultChunkSize",
        paramProperties.get("CLIENT_RESULT_CHUNK_SIZE").toString());
    System.setProperty(
        "net.snowflake.jdbc.clientMemoryLimit",
        paramProperties.get("CLIENT_MEMORY_LIMIT").toString());

    paramProperties.put("CLIENT_PREFETCH_THREADS", "8");
    paramProperties.put("CLIENT_RESULT_CHUNK_SIZE", 64);
    paramProperties.put("CLIENT_MEMORY_LIMIT", 2000L);

    try {
      Connection connection = getConnection(paramProperties);

      for (Enumeration<?> enums = paramProperties.propertyNames(); enums.hasMoreElements(); ) {
        String key = (String) enums.nextElement();
        ResultSet rs =
            connection
                .createStatement()
                .executeQuery(String.format("show parameters like '%s'", key));
        rs.next();
        String value = rs.getString("value");
        assertThat(key, value, equalTo(paramProperties.get(key).toString()));
      }
    } finally {
      System.clearProperty("net.snowflake.jdbc.clientPrefetchThreads");
      System.clearProperty("net.snowflake.jdbc.clientResultChunkSize");
      System.clearProperty("net.snowflake.jdbc.clientMemoryLimit");
    }
  }

  /**
   * Verify the passed heartbeat frequency, which is too small, is changed to the smallest valid
   * value.
   */
  @Test
  public void testHeartbeatFrequencyTooSmall() throws Exception {
    Properties paramProperties = new Properties();
    paramProperties.put(CLIENT_SESSION_KEEP_ALIVE_HEARTBEAT_FREQUENCY, 2);
    Connection connection = getConnection(paramProperties);

    connection.getClientInfo(CLIENT_SESSION_KEEP_ALIVE_HEARTBEAT_FREQUENCY);

    for (Enumeration<?> enums = paramProperties.propertyNames(); enums.hasMoreElements(); ) {
      String key = (String) enums.nextElement();
      ResultSet rs =
          connection
              .createStatement()
              .executeQuery(String.format("show parameters like '%s'", key));
      rs.next();
      String value = rs.getString("value");

      assertThat(key, value, equalTo("900"));
    }
  }

  /**
   * Verify the passed heartbeat frequency, which is too large, is changed to the maximum valid
   * value.
   */
  @Test
  public void testHeartbeatFrequencyTooLarge() throws Exception {
    Properties paramProperties = new Properties();
    paramProperties.put(CLIENT_SESSION_KEEP_ALIVE_HEARTBEAT_FREQUENCY, 4000);
    Connection connection = getConnection(paramProperties);

    connection.getClientInfo(CLIENT_SESSION_KEEP_ALIVE_HEARTBEAT_FREQUENCY);

    for (Enumeration<?> enums = paramProperties.propertyNames(); enums.hasMoreElements(); ) {
      String key = (String) enums.nextElement();
      ResultSet rs =
          connection
              .createStatement()
              .executeQuery(String.format("show parameters like '%s'", key));
      rs.next();
      String value = rs.getString("value");

      assertThat(key, value, equalTo("3600"));
    }
  }

  /**
   * Verify the passed heartbeat frequency matches the output value if the input is valid (between
   * 900 and 3600).
   */
  @Test
  public void testHeartbeatFrequencyValidValue() throws Exception {
    Properties paramProperties = new Properties();
    paramProperties.put(CLIENT_SESSION_KEEP_ALIVE_HEARTBEAT_FREQUENCY, 1800);
    Connection connection = getConnection(paramProperties);

    connection.getClientInfo(CLIENT_SESSION_KEEP_ALIVE_HEARTBEAT_FREQUENCY);

    for (Enumeration<?> enums = paramProperties.propertyNames(); enums.hasMoreElements(); ) {
      String key = (String) enums.nextElement();
      ResultSet rs =
          connection
              .createStatement()
              .executeQuery(String.format("show parameters like '%s'", key));
      rs.next();
      String value = rs.getString("value");

      assertThat(key, value, equalTo(paramProperties.get(key).toString()));
    }
  }

  @Test
  public void testReadOnly() throws Throwable {
    try (Connection connection = getConnection()) {
      try {
        connection.setReadOnly(true);
        fail("must raise SQLFeatureNotSupportedException");
      } catch (SQLFeatureNotSupportedException ex) {
        // nop
      }

      connection.setReadOnly(false);
      connection.createStatement().execute("create or replace table readonly_test(c1 int)");
      assertFalse(connection.isReadOnly());
      connection.createStatement().execute("drop table if exists readonly_test");
    }
  }

  @Test
  public void testNativeSQL() throws Throwable {
    try (Connection connection = getConnection()) {
      // today returning the source SQL.
      assertEquals("select 1", connection.nativeSQL("select 1"));
    }
  }

  @Test
  public void testGetTypeMap() throws Throwable {
    try (Connection connection = getConnection()) {
      // return an empty type map. setTypeMap is not supported.
      assertEquals(Collections.emptyMap(), connection.getTypeMap());
    }
  }

  @Test
  public void testHolderbility() throws Throwable {
    try (Connection connection = getConnection()) {
      try {
        connection.setHoldability(0);
      } catch (SQLFeatureNotSupportedException ex) {
        // nop
      }
      // return an empty type map. setTypeMap is not supported.
      assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, connection.getHoldability());
    }
  }

  @Test
  public void testIsValid() throws Throwable {
    try (Connection connection = getConnection()) {
      assertTrue(connection.isValid(10));
      try {
        assertTrue(connection.isValid(-10));
        fail("must fail");
      } catch (SQLException ex) {
        // nop, no specific error code is provided.
      }
    }
  }

  @Test
  public void testUnwrapper() throws Throwable {
    try (Connection connection = getConnection()) {
      boolean canUnwrap = connection.isWrapperFor(SnowflakeConnectionV1.class);
      assertTrue(canUnwrap);
      if (canUnwrap) {
        SnowflakeConnectionV1 sfconnection = connection.unwrap(SnowflakeConnectionV1.class);
        sfconnection.createStatement();
      } else {
        fail("should be able to unwrap");
      }
      try {
        connection.unwrap(SnowflakeDriver.class);
        fail("should fail to cast");
      } catch (SQLException ex) {
        // nop
      }
    }
  }

  @Test
  public void testStatementsAndResultSetsClosedByConnection() throws SQLException {
    Connection connection = getConnection();
    Statement statement1 = connection.createStatement();
    Statement statement2 = connection.createStatement();
    ResultSet rs1 = statement2.executeQuery("select 2;");
    ResultSet rs2 = statement2.executeQuery("select 2;");
    ResultSet rs3 = statement2.executeQuery("select 2;");
    PreparedStatement statement3 = connection.prepareStatement("select 2;");
    connection.close();
    assertTrue(statement1.isClosed());
    assertTrue(statement2.isClosed());
    assertTrue(statement3.isClosed());
    assertTrue(rs1.isClosed());
    assertTrue(rs2.isClosed());
    assertTrue(rs3.isClosed());
  }

  @Test
  public void testResultSetsClosedByStatement() throws SQLException {
    Connection connection = getConnection();
    Statement statement2 = connection.createStatement();
    ResultSet rs1 = statement2.executeQuery("select 2;");
    ResultSet rs2 = statement2.executeQuery("select 2;");
    ResultSet rs3 = statement2.executeQuery("select 2;");
    PreparedStatement statement3 = connection.prepareStatement("select 2;");
    ResultSet rs4 = statement3.executeQuery();
    assertFalse(rs1.isClosed());
    assertFalse(rs2.isClosed());
    assertFalse(rs3.isClosed());
    assertFalse(rs4.isClosed());
    statement2.close();
    statement3.close();
    assertTrue(rs1.isClosed());
    assertTrue(rs2.isClosed());
    assertTrue(rs3.isClosed());
    assertTrue(rs4.isClosed());
    connection.close();
  }

  @Test
  @ConditionalIgnore(condition = RunningNotOnTestaccount.class)
  public void testOKTAConnection() throws Throwable {
    Map<String, String> params = getConnectionParameters();
    Properties properties = new Properties();
    properties.put("user", params.get("ssoUser"));
    properties.put("password", params.get("ssoPassword"));
    properties.put("ssl", params.get("ssl"));
    properties.put("authenticator", "https://snowflakecomputing.okta.com/");

    DriverManager.getConnection(
        String.format(
            "jdbc:snowflake://%s.reg.snowflakecomputing.com:%s/",
            params.get("account"), params.get("port")),
        properties);
  }

  @Test
  @ConditionalIgnore(condition = RunningNotOnTestaccount.class)
  public void testOKTAConnectionWithOktauserParam() throws Throwable {
    Map<String, String> params = getConnectionParameters();
    Properties properties = new Properties();
    properties.put("user", "test");
    properties.put("password", params.get("ssoPassword"));
    properties.put("ssl", params.get("ssl"));
    properties.put(
        "authenticator",
        String.format(
            "https://snowflakecomputing.okta.com;oktausername=%s;", params.get("ssoUser")));

    DriverManager.getConnection(
        String.format(
            "jdbc:snowflake://%s.reg.snowflakecomputing.com:%s/",
            params.get("account"), params.get("port")),
        properties);
  }

  @Test
  public void testValidateDefaultParameters() throws Throwable {
    Map<String, String> params = getConnectionParameters();
    Properties props;

    props = setCommonConnectionParameters(true);
    props.put("db", "NOT_EXISTS");
    try {
      DriverManager.getConnection(params.get("uri"), props);
      fail("should fail");
    } catch (SQLException ex) {
      assertEquals(
          "error code", ex.getErrorCode(), SESSION_CREATION_OBJECT_DOES_NOT_EXIST_NOT_AUTHORIZED);
    }

    // schema is invalid
    props = setCommonConnectionParameters(true);
    props.put("schema", "NOT_EXISTS");
    try {
      DriverManager.getConnection(params.get("uri"), props);
      fail("should fail");
    } catch (SQLException ex) {
      assertEquals(
          "error code", ex.getErrorCode(), SESSION_CREATION_OBJECT_DOES_NOT_EXIST_NOT_AUTHORIZED);
    }

    // warehouse is invalid
    props = setCommonConnectionParameters(true);
    props.put("warehouse", "NOT_EXISTS");
    try {
      DriverManager.getConnection(params.get("uri"), props);
      fail("should fail");
    } catch (SQLException ex) {
      assertEquals(
          "error code", ex.getErrorCode(), SESSION_CREATION_OBJECT_DOES_NOT_EXIST_NOT_AUTHORIZED);
    }

    // role is invalid
    props = setCommonConnectionParameters(true);
    props.put("role", "NOT_EXISTS");
    try {
      DriverManager.getConnection(params.get("uri"), props);
      fail("should fail");
    } catch (SQLException ex) {
      assertEquals("error code", ex.getErrorCode(), ROLE_IN_CONNECT_STRING_DOES_NOT_EXIST);
    }
  }

  @Test
  public void testNoValidateDefaultParameters() throws Throwable {
    Map<String, String> params = getConnectionParameters();
    Properties props;

    props = setCommonConnectionParameters(false);
    props.put("db", "NOT_EXISTS");
    DriverManager.getConnection(params.get("uri"), props);

    // schema is invalid
    props = setCommonConnectionParameters(false);
    props.put("schema", "NOT_EXISTS");
    DriverManager.getConnection(params.get("uri"), props);

    // warehouse is invalid
    props = setCommonConnectionParameters(false);
    props.put("warehouse", "NOT_EXISTS");
    DriverManager.getConnection(params.get("uri"), props);

    // role is invalid
    props = setCommonConnectionParameters(false);
    props.put("role", "NOT_EXISTS");
    try {
      DriverManager.getConnection(params.get("uri"), props);
      fail("should fail");
    } catch (SQLException ex) {
      assertEquals("error code", ex.getErrorCode(), ROLE_IN_CONNECT_STRING_DOES_NOT_EXIST);
    }
  }

  private Properties setCommonConnectionParameters(boolean validateDefaultParameters) {
    Map<String, String> params = getConnectionParameters();
    Properties props = new Properties();
    props.put("validateDefaultParameters", validateDefaultParameters);
    props.put("account", params.get("account"));
    props.put("ssl", params.get("ssl"));
    props.put("role", params.get("role"));
    props.put("user", params.get("user"));
    props.put("password", params.get("password"));
    props.put("db", params.get("database"));
    props.put("schema", params.get("schema"));
    props.put("warehouse", params.get("warehouse"));
    return props;
  }

  private class ConcurrentConnections implements Runnable {
    Connection con = null;

    ConcurrentConnections() {}

    @Override
    public void run() {
      try {
        con = getConnection();
        con.createStatement().executeQuery("select * from bigTable");
        con.close();

      } catch (SQLException ex) {
        try {
          con.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
        ex.printStackTrace();
      }
    }
  }

  @Test
  public void testAsyncQueryOpenAndCloseConnection()
      throws SQLException, IOException, InterruptedException {
    // open connection and run asynchronous query
    Connection con = getConnection();
    Statement statement = con.createStatement();
    ResultSet rs1 =
        statement
            .unwrap(SnowflakeStatement.class)
            .executeAsyncQuery("select count(*) from table(generator(timeLimit => 40))");
    // Retrieve query ID for part 2 of test, check status of query
    String queryID = rs1.unwrap(SnowflakeResultSet.class).getQueryID();
    Thread.sleep(100);
    QueryStatus status = rs1.unwrap(SnowflakeResultSet.class).getStatus();
    // Query should take 60 seconds so should be running
    assertEquals(RUNNING, status);
    // close connection and wait for 1 minute while query finishes running
    statement.close();
    con.close();
    Thread.sleep(1000 * 70);
    // Create a new connection and new instance of a resultSet using query ID
    con = getConnection();
    try {
      ResultSet rs =
          con.unwrap(SnowflakeConnection.class).createResultSet("Totally invalid query ID");
      fail("Query ID should be rejected");
    } catch (SQLException e) {
      assertEquals(SqlState.INVALID_PARAMETER_VALUE, e.getSQLState());
    }
    ResultSet rs = con.unwrap(SnowflakeConnection.class).createResultSet(queryID);
    status = rs.unwrap(SnowflakeResultSet.class).getStatus();
    // Assert status of query is a success
    assertEquals(QueryStatus.SUCCESS, status);
    assertEquals("No error reported", status.getErrorMessage());
    assertEquals(0, status.getErrorCode());
    assertEquals(1, getSizeOfResultSet(rs));
    statement = con.createStatement();
    // Create another query that will not be successful (querying table that does not exist)
    rs1 =
        statement
            .unwrap(SnowflakeStatement.class)
            .executeAsyncQuery("select * from nonexistentTable");
    Thread.sleep(100);
    status = rs1.unwrap(SnowflakeResultSet.class).getStatus();
    assertEquals(QueryStatus.FAILED_WITH_ERROR, status);
    assertEquals(2003, status.getErrorCode());
    assertEquals(
        "SQL compilation error:\n"
            + "Object 'NONEXISTENTTABLE' does not exist or not "
            + "authorized.",
        status.getErrorMessage());
    statement.close();
    con.close();
  }

  @Test
  public void testAsyncAndSynchronousQueries() throws SQLException {
    Connection con = getConnection();
    Statement statement = con.createStatement();
    // execute some statements that you want to be synchronous
    statement.execute("alter session set CLIENT_TIMESTAMP_TYPE_MAPPING=TIMESTAMP_TZ");
    statement.execute("create or replace table smallTable (colA string, colB int)");
    statement.execute("create or replace table uselessTable (colA string, colB int)");
    statement.execute("insert into smallTable values ('row1', 1), ('row2', 2), ('row3', 3)");
    statement.execute("insert into uselessTable values ('row1', 1), ('row2', 2), ('row3', 3)");
    // Select from uselessTable asynchronously; drop it synchronously afterwards
    ResultSet rs =
        statement.unwrap(SnowflakeStatement.class).executeAsyncQuery("select * from smallTable");
    // execute a query that you don't want to wait for
    ResultSet rs1 =
        statement.unwrap(SnowflakeStatement.class).executeAsyncQuery("select * from uselessTable");
    // Drop the table that was queried asynchronously. Should not drop until after async query
    // finishes, because this
    // query IS synchronous
    ResultSet rs2 = statement.executeQuery("drop table uselessTable");
    while (rs2.next()) {
      assertEquals("USELESSTABLE successfully dropped.", rs2.getString(1));
    }
    // able to successfully fetch results in spite of table being dropped
    assertEquals(3, getSizeOfResultSet(rs1));
    statement.execute("alter session set CLIENT_TIMESTAMP_TYPE_MAPPING=TIMESTAMP_LTZ");

    // come back to the asynchronously executed result set after finishing other things
    rs.next();
    assertEquals(rs.getString(1), "row1");
    assertEquals(rs.getInt(2), 1);
    rs.next();
    assertEquals(rs.getString(1), "row2");
    assertEquals(rs.getInt(2), 2);
    rs.next();
    assertEquals(rs.getString(1), "row3");
    assertEquals(rs.getInt(2), 3);
    statement.execute("drop table smallTable");
    statement.close();
    con.close();
  }

  @Test
  public void testPreparedStatementAsyncQuery() throws SQLException {
    Connection con = getConnection();
    con.createStatement().execute("create or replace table testTable(colA string, colB boolean)");
    PreparedStatement prepStatement = con.prepareStatement("insert into testTable values (?,?)");
    prepStatement.setInt(1, 33);
    prepStatement.setBoolean(2, true);
    // call executeAsyncQuery
    ResultSet rs = prepStatement.unwrap(SnowflakePreparedStatement.class).executeAsyncQuery();
    // Get access to results by calling next() function
    // next () will block until results are ready
    assertTrue(rs.next());
    // the resultSet consists of a single row, single column containing the number of rows that have
    // been updated by the insert
    // the number of updated rows in testTable is 1 so 1 is returned
    assertEquals(rs.getString(1), "1");
    con.createStatement().execute("drop table testTable");
    prepStatement.close();
    con.close();
  }

  @Test
  public void testIsStillRunning() {
    QueryStatus[] runningStatuses = {
      QueryStatus.RUNNING,
      QueryStatus.RESUMING_WAREHOUSE,
      QueryStatus.QUEUED,
      QueryStatus.QUEUED_REPAIRING_WAREHOUSE,
      QueryStatus.NO_DATA
    };

    QueryStatus[] otherStatuses = {
      QueryStatus.ABORTED,
      QueryStatus.ABORTING,
      QueryStatus.SUCCESS,
      QueryStatus.FAILED_WITH_ERROR,
      QueryStatus.FAILED_WITH_INCIDENT,
      QueryStatus.DISCONNECTED,
      QueryStatus.RESTARTED,
      QueryStatus.BLOCKED
    };

    for (QueryStatus qs : runningStatuses) {
      assertEquals(true, QueryStatus.isStillRunning(qs));
    }

    for (QueryStatus qs : otherStatuses) {
      assertEquals(false, QueryStatus.isStillRunning(qs));
    }
  }

  @Test
  public void testIsAnError() {
    QueryStatus[] otherStatuses = {
      QueryStatus.RUNNING, QueryStatus.RESUMING_WAREHOUSE, QueryStatus.QUEUED,
      QueryStatus.QUEUED_REPAIRING_WAREHOUSE, QueryStatus.SUCCESS, QueryStatus.RESTARTED,
      QueryStatus.NO_DATA
    };

    QueryStatus[] errorStatuses = {
      QueryStatus.ABORTED, QueryStatus.ABORTING,
      QueryStatus.FAILED_WITH_ERROR, QueryStatus.FAILED_WITH_INCIDENT,
      QueryStatus.DISCONNECTED, QueryStatus.BLOCKED
    };

    for (QueryStatus qs : errorStatuses) {
      assertEquals(true, QueryStatus.isAnError(qs));
    }

    for (QueryStatus qs : otherStatuses) {
      assertEquals(false, QueryStatus.isAnError(qs));
    }
  }

  /**
   * MANUAL TESTING OF ASYNCHRONOUS QUERYING
   *
   * <p>This test does not provide reliable results because the status of the queries often depends
   * on the GS server's behavior. We can often replicate QUEUED and RESUMING_WAREHOUSE statuses,
   * however.
   */
  // @Test
  public void testQueryStatuses() throws SQLException, IOException, InterruptedException {
    // Before running test, close warehouse and re-open it!
    Connection con = getConnection();
    Statement statement = con.createStatement();
    ResultSet rs =
        statement
            .unwrap(SnowflakeStatement.class)
            .executeAsyncQuery("select count(*) from table(generator(timeLimit => 5))");
    Thread.sleep(100);
    QueryStatus status = rs.unwrap(SnowflakeResultSet.class).getStatus();
    // Since warehouse has just been restarted, warehouse should still be booting
    assertEquals(QueryStatus.RESUMING_WAREHOUSE, status);

    // now try to get QUEUED status
    ResultSet rs1 =
        statement
            .unwrap(SnowflakeStatement.class)
            .executeAsyncQuery("select count(*) from table(generator(timeLimit => 60))");
    ResultSet rs2 =
        statement
            .unwrap(SnowflakeStatement.class)
            .executeAsyncQuery("select count(*) from table(generator(timeLimit => 60))");
    ResultSet rs3 =
        statement
            .unwrap(SnowflakeStatement.class)
            .executeAsyncQuery("select count(*) from table(generator(timeLimit => 60))");
    ResultSet rs4 =
        statement
            .unwrap(SnowflakeStatement.class)
            .executeAsyncQuery("select count(*) from table(generator(timeLimit => 60))");
    // Retrieve query ID for part 2 of test, check status of query
    Thread.sleep(100);
    status = rs4.unwrap(SnowflakeResultSet.class).getStatus();
    // Since 4 queries were started at once, status is most likely QUEUED
    assertEquals(QueryStatus.QUEUED, status);
  }
}
