package net.snowflake.client.jdbc;

import static net.snowflake.client.AssumptionUtils.assumeRunningOnGithubActions;
import static net.snowflake.client.core.SessionUtil.CLIENT_SESSION_KEEP_ALIVE_HEARTBEAT_FREQUENCY;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import net.snowflake.client.TestUtil;
import net.snowflake.client.annotations.DontRunOnGithubActions;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.core.SFSession;
import net.snowflake.common.core.SqlState;
import org.apache.commons.codec.binary.Base64;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/** Connection integration tests */
@Tag(TestTags.CONNECTION)
public class ConnectionIT extends BaseJDBCWithSharedConnectionIT {
  // create a local constant for this code for testing purposes (already defined in GS)
  public static final int INVALID_CONNECTION_INFO_CODE = 390100;
  private static final int SESSION_CREATION_OBJECT_DOES_NOT_EXIST_NOT_AUTHORIZED = 390201;
  private static final int ROLE_IN_CONNECT_STRING_DOES_NOT_EXIST = 390189;
  public static final int BAD_REQUEST_GS_CODE = 390400;
  public static final int NETWORK_ERROR_CODE = 200015;

  public static final int WAIT_FOR_TELEMETRY_REPORT_IN_MILLISECS = 5000;

  String errorMessage = null;

  @TempDir private File tmpFolder;

  @Test
  public void testSimpleConnection() throws SQLException {
    Connection con = getConnection();
    try (Statement statement = con.createStatement();
        ResultSet resultSet = statement.executeQuery("show parameters")) {
      assertTrue(resultSet.next());
      assertFalse(con.isClosed());
    }
    con.close();
    assertTrue(con.isClosed());
    con.close(); // ensure no exception
  }

  @Test
  @Disabled
  public void test300ConnectionsWithSingleClientInstance() throws SQLException {
    // concurrent testing
    int size = 300;
    try (Statement statement = connection.createStatement()) {
      String database = connection.getCatalog();
      String schema = connection.getSchema();
      statement.execute(
          "create or replace table bigTable(rowNum number,rando "
              + "number) as (select seq4(),"
              + "uniform(1, 10, random()) from table(generator(rowcount=>10000000)) v)");
      statement.execute("create or replace table conTable(colA number)");

      ExecutorService taskRunner = Executors.newFixedThreadPool(size);
      for (int i = 0; i < size; i++) {
        ConcurrentConnections newTask = new ConcurrentConnections();
        taskRunner.submit(newTask);
      }
      assertEquals(null, errorMessage);
      taskRunner.shutdownNow();
    }
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
    SQLException e =
        assertThrows(
            SQLException.class,
            () -> {
              ds.getConnection();
            });
    assertThat(e.getErrorCode(), is(ErrorCode.NETWORK_ERROR.getMessageCode()));

    long endLoginTime = System.currentTimeMillis();

    assertTrue(endLoginTime - startLoginTime < 30000);
  }

  /**
   * Test production connectivity in case cipher suites or tls protocol change Use fake username and
   * password but correct url Expectation is receiving incorrect username or password response from
   * server
   */
  @Disabled("Disable due to changed error response in backend. Follow up: SNOW-2021007")
  @Test
  public void testProdConnectivity() throws SQLException {
    String[] deploymentUrls = {
      "jdbc:snowflake://sfcsupport.snowflakecomputing.com",
      "jdbc:snowflake://sfcsupportva.us-east-1.snowflakecomputing.com",
      "jdbc:snowflake://sfcsupporteu.eu-central-1.snowflakecomputing.com"
    };

    Properties properties = new Properties();

    properties.put("user", "fakeuser");
    properties.put("password", "fakepwd");
    properties.put("account", "fakeaccount");

    for (String url : deploymentUrls) {
      SQLException e =
          assertThrows(
              SQLException.class,
              () -> {
                DriverManager.getConnection(url, properties);
              });
      assertThat(
          e.getErrorCode(), anyOf(is(INVALID_CONNECTION_INFO_CODE), is(BAD_REQUEST_GS_CODE)));
    }
  }

  @Test
  public void testSetCatalogSchema() throws Throwable {
    try (Statement statement = connection.createStatement()) {
      String db = connection.getCatalog();
      String schema = connection.getSchema();
      connection.setCatalog(db);
      connection.setSchema("PUBLIC");

      // get the current schema
      try (ResultSet rst = statement.executeQuery("select current_schema()")) {
        assertTrue(rst.next());
        assertEquals("PUBLIC", rst.getString(1));
        assertEquals(db, connection.getCatalog());
        assertEquals("PUBLIC", connection.getSchema());
      }
      // get the current schema
      connection.setSchema(schema);
      try (ResultSet rst = statement.executeQuery("select current_schema()")) {
        assertTrue(rst.next());
        assertEquals(schema, rst.getString(1));
      }
    }
  }

  @Test
  public void testDataCompletenessInLowMemory() throws Exception {
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      for (int i = 0; i < 6; i++) {
        int resultSize = 1000000 + i;
        statement.execute("ALTER SESSION SET CLIENT_MEMORY_LIMIT=10");
        try (ResultSet resultSet =
            statement.executeQuery(
                "select randstr(80, random()) from table(generator(rowcount => "
                    + resultSize
                    + "))")) {

          int size = 0;
          while (resultSet.next()) {
            size++;
          }
          System.out.println("Total records: " + size);
          assertEquals(size, resultSize);
        }
      }
    }
  }

  @Test
  @DontRunOnGithubActions
  public void testConnectionGetAndSetDBAndSchema() throws SQLException {
    final String SECOND_DATABASE = "SECOND_DATABASE";
    final String SECOND_SCHEMA = "SECOND_SCHEMA";
    try (Statement statement = connection.createStatement()) {
      try {
        final String database = TestUtil.systemGetEnv("SNOWFLAKE_TEST_DATABASE").toUpperCase();
        final String schema = TestUtil.systemGetEnv("SNOWFLAKE_TEST_SCHEMA").toUpperCase();

        assertEquals(database, connection.getCatalog());
        assertEquals(schema, connection.getSchema());

        statement.execute(String.format("create or replace database %s", SECOND_DATABASE));
        statement.execute(String.format("create or replace schema %s", SECOND_SCHEMA));
        statement.execute(String.format("use database %s", database));

        connection.setCatalog(SECOND_DATABASE);
        assertEquals(SECOND_DATABASE, connection.getCatalog());
        assertEquals("PUBLIC", connection.getSchema());

        connection.setSchema(SECOND_SCHEMA);
        assertEquals(SECOND_SCHEMA, connection.getSchema());

        statement.execute(String.format("use database %s", database));
        statement.execute(String.format("use schema %s", schema));

        assertEquals(database, connection.getCatalog());
        assertEquals(schema, connection.getSchema());
      } finally {
        statement.execute(String.format("drop database if exists %s", SECOND_DATABASE));
      }
    }
  }

  @Test
  public void testConnectionClientInfo() throws SQLException {
    Properties property = connection.getClientInfo();
    assertEquals(0, property.size());
    Properties clientInfo = new Properties();
    clientInfo.setProperty("name", "Peter");
    clientInfo.setProperty("description", "SNOWFLAKE JDBC");
    SQLClientInfoException e =
        assertThrows(
            SQLClientInfoException.class,
            () -> {
              connection.setClientInfo(clientInfo);
            });
    assertEquals(SqlState.INVALID_PARAMETER_VALUE, e.getSQLState());
    assertEquals(200047, e.getErrorCode());
    assertEquals(2, e.getFailedProperties().size());

    e =
        assertThrows(
            SQLClientInfoException.class,
            () -> {
              connection.setClientInfo("ApplicationName", "valueA");
            });
    assertEquals(SqlState.INVALID_PARAMETER_VALUE, e.getSQLState());
    assertEquals(200047, e.getErrorCode());
    assertEquals(1, e.getFailedProperties().size());
  }

  // only support get and set
  @Test
  public void testNetworkTimeout() throws SQLException {
    int millis = connection.getNetworkTimeout();
    assertEquals(0, millis);
    connection.setNetworkTimeout(null, 200);
    assertEquals(200, connection.getNetworkTimeout());
    // Reset timeout to 0 since we are reusing connection in tests
    connection.setNetworkTimeout(null, 0);
    assertEquals(0, millis);
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
    try (Connection connection = getConnection(properties);
        Statement statement = connection.createStatement()) {
      SQLException e =
          assertThrows(
              SQLException.class,
              () -> {
                statement.executeQuery(
                    "select count(*) from table(generator(timeLimit => 1000000))");
              });
      assertEquals(SqlState.QUERY_CANCELED, e.getSQLState());
      assertEquals("SQL execution canceled", e.getMessage());
    }
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

    try (Connection connection = ds.getConnection(user, password);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("select 1")) {
      resultSet.next();
      assertThat("select 1", resultSet.getInt(1), equalTo(1));
    }

    // get connection by server name
    // this is used by ibm cast iron studio
    ds = new SnowflakeBasicDataSource();
    ds.setServerName(params.get("host"));
    ds.setSsl("on".equals(ssl));
    ds.setAccount(account);
    ds.setPortNumber(Integer.parseInt(port));
    try (Connection connection = ds.getConnection(params.get("user"), params.get("password"));
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("select 1")) {
      resultSet.next();
      assertThat("select 1", resultSet.getInt(1), equalTo(1));
    }
  }

  @Test
  @Disabled
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
    try (Connection con = ds.getConnection();
        Statement statement = con.createStatement();
        ResultSet resultSet = statement.executeQuery("select 1")) {
      resultSet.next();
      assertThat("select 1", resultSet.getInt(1), equalTo(1));
      File serializedFile = new File(tmpFolder, "serializedStuff.ser");
      serializedFile.createNewFile();
      // serialize datasource object into a file
      try (FileOutputStream outputFile = new FileOutputStream(serializedFile);
          ObjectOutputStream out = new ObjectOutputStream(outputFile)) {
        out.writeObject(ds);
      }
      // deserialize into datasource object again
      try (FileInputStream inputFile = new FileInputStream(serializedFile);
          ObjectInputStream in = new ObjectInputStream(inputFile)) {
        SnowflakeBasicDataSource ds2 = (SnowflakeBasicDataSource) in.readObject();

        // test connection a second time
        try (Connection connection = ds2.getConnection();
            Statement statement2 = connection.createStatement();
            ResultSet resultSet2 = statement2.executeQuery("select 1")) {
          resultSet2.next();
          assertThat("select 1", resultSet2.getInt(1), equalTo(1));
        }
      }
    }
  }

  @Test
  @DontRunOnGithubActions
  public void testConnectUsingKeyPair() throws Exception {
    Map<String, String> parameters = getConnectionParameters();
    String testUser = parameters.get("user");

    KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
    SecureRandom random = SecureRandom.getInstance("SHA1PRNG");
    keyPairGenerator.initialize(2048, random);

    KeyPair keyPair = keyPairGenerator.generateKeyPair();
    PublicKey publicKey = keyPair.getPublic();
    PrivateKey privateKey = keyPair.getPrivate();

    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("use role accountadmin");

      String encodePublicKey = Base64.encodeBase64String(publicKey.getEncoded());

      statement.execute(
          String.format("alter user %s set rsa_public_key='%s'", testUser, encodePublicKey));
    }

    String uri = parameters.get("uri");

    Properties properties = new Properties();
    properties.put("account", parameters.get("account"));
    properties.put("user", testUser);
    properties.put("ssl", parameters.get("ssl"));
    properties.put("port", parameters.get("port"));

    // test correct private key one
    properties.put("privateKey", privateKey);
    try (Connection connection = DriverManager.getConnection(uri, properties)) {
      assertFalse(connection.isClosed());
    }

    // test datasource connection using private key
    SnowflakeBasicDataSource ds = new SnowflakeBasicDataSource();
    ds.setUrl(uri);
    ds.setAccount(parameters.get("account"));
    ds.setUser(parameters.get("user"));
    ds.setSsl("on".equals(parameters.get("ssl")));
    ds.setPortNumber(Integer.valueOf(parameters.get("port")));
    ds.setPrivateKey(privateKey);

    try (Connection con = ds.getConnection()) {
      assertFalse(con.isClosed());
    }
    // test wrong private key
    keyPair = keyPairGenerator.generateKeyPair();
    PublicKey publicKey2 = keyPair.getPublic();
    PrivateKey privateKey2 = keyPair.getPrivate();
    properties.put("privateKey", privateKey2);
    SQLException e =
        assertThrows(
            SQLException.class,
            () -> {
              DriverManager.getConnection(uri, properties);
            });
    assertEquals(390144, e.getErrorCode());

    // test multiple key pair
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("use role accountadmin");
      String encodePublicKey2 = Base64.encodeBase64String(publicKey2.getEncoded());
      statement.execute(
          String.format("alter user %s set rsa_public_key_2='%s'", testUser, encodePublicKey2));
    }

    try (Connection connection = DriverManager.getConnection(uri, properties)) {
      assertFalse(connection.isClosed());
    }

    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("use role accountadmin");
      statement.execute(String.format("alter user %s unset rsa_public_key", testUser));
      statement.execute(String.format("alter user %s unset rsa_public_key_2", testUser));
    }
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

    SQLException e =
        assertThrows(
            SQLException.class,
            () -> {
              properties.put("privateKey", "bad string");
              DriverManager.getConnection(uri, properties);
            });
    assertThat(e.getErrorCode(), is(ErrorCode.INVALID_PARAMETER_TYPE.getMessageCode()));

    e =
        assertThrows(
            SQLException.class,
            () -> {
              properties.put("privateKey", dsaPrivateKey);
              DriverManager.getConnection(uri, properties);
            });
    assertThat(e.getErrorCode(), is(ErrorCode.INVALID_OR_UNSUPPORTED_PRIVATE_KEY.getMessageCode()));
  }

  @Test
  @DontRunOnGithubActions
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

      try (Connection connection = getConnection();
          Statement statement = connection.createStatement()) {
        statement.execute("use role accountadmin");

        String encodePublicKey = Base64.encodeBase64String(publicKey.getEncoded());

        statement.execute(
            String.format("alter user %s set rsa_public_key='%s'", testUser, encodePublicKey));
      }

      String uri = parameters.get("uri");

      Properties properties = new Properties();
      properties.put("account", parameters.get("account"));
      properties.put("user", testUser);
      properties.put("ssl", parameters.get("ssl"));
      properties.put("port", parameters.get("port"));
      properties.put("role", "accountadmin");

      // test correct private key one
      properties.put("privateKey", privateKey);
      try (Connection connection = DriverManager.getConnection(uri, properties)) {
        assertFalse(connection.isClosed());
      }

      try (Connection connection = getConnection();
          Statement statement = connection.createStatement()) {
        statement.execute("use role accountadmin");
        statement.execute(String.format("alter user %s unset rsa_public_key", testUser));
      }
    }
  }

  /** Test production connectivity with insecure mode enabled. */
  @Disabled("Disable due to changed error response in backend. Follow up: SNOW-2021007")
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testInsecureMode(boolean insecureModeInProperties) {
    String deploymentUrl;

    Properties properties = new Properties();

    properties.put("user", "fakeuser");
    properties.put("password", "fakepwd");
    properties.put("account", "fakeaccount");
    if (insecureModeInProperties) {
      properties.put("insecureMode", true);
      deploymentUrl = "jdbc:snowflake://sfcsupport.snowflakecomputing.com";
    } else {
      deploymentUrl = "jdbc:snowflake://sfcsupport.snowflakecomputing.com?insecureMode=true";
    }
    SQLException e =
        assertThrows(
            SQLException.class,
            () -> {
              DriverManager.getConnection(deploymentUrl, properties);
            });
    assertThat(e.getErrorCode(), anyOf(is(INVALID_CONNECTION_INFO_CODE), is(BAD_REQUEST_GS_CODE)));
  }

  /** Verify the passed memory parameters are set in the session */
  @Test
  public void testClientMemoryParameters() throws Exception {
    Properties paramProperties = new Properties();
    paramProperties.put("CLIENT_PREFETCH_THREADS", "6");
    paramProperties.put("CLIENT_RESULT_CHUNK_SIZE", 48);
    paramProperties.put("CLIENT_MEMORY_LIMIT", 1000);
    try (Connection connection = getConnection(paramProperties);
        Statement statement = connection.createStatement()) {
      for (Enumeration<?> enums = paramProperties.propertyNames(); enums.hasMoreElements(); ) {
        String key = (String) enums.nextElement();
        try (ResultSet rs =
            statement.executeQuery(String.format("show parameters like '%s'", key))) {
          assertTrue(rs.next());
          String value = rs.getString("value");
          assertThat(key, value, equalTo(paramProperties.get(key).toString()));
        }
      }
    }
  }

  /** Verify the JVM memory parameters are set in the session */
  @Test
  public void testClientMemoryJvmParameters() throws Exception {
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

    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      for (Enumeration<?> enums = paramProperties.propertyNames(); enums.hasMoreElements(); ) {
        String key = (String) enums.nextElement();
        try (ResultSet rs =
            statement.executeQuery(String.format("show parameters like '%s'", key))) {
          assertTrue(rs.next());
          String value = rs.getString("value");
          assertThat(key, value, equalTo(paramProperties.get(key).toString()));
        }
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
  public void testClientMixedMemoryJvmParameters() throws Exception {
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

    try (Connection connection = getConnection(paramProperties);
        Statement statement = connection.createStatement()) {
      for (Enumeration<?> enums = paramProperties.propertyNames(); enums.hasMoreElements(); ) {
        String key = (String) enums.nextElement();
        try (ResultSet rs =
            statement.executeQuery(String.format("show parameters like '%s'", key))) {
          assertTrue(rs.next());
          String value = rs.getString("value");
          assertThat(key, value, equalTo(paramProperties.get(key).toString()));
        }
      }
    } finally {
      System.clearProperty("net.snowflake.jdbc.clientPrefetchThreads");
      System.clearProperty("net.snowflake.jdbc.clientResultChunkSize");
      System.clearProperty("net.snowflake.jdbc.clientMemoryLimit");
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
    try (Connection connection = getConnection(paramProperties);
        Statement statement = connection.createStatement()) {
      connection.getClientInfo(CLIENT_SESSION_KEEP_ALIVE_HEARTBEAT_FREQUENCY);

      for (Enumeration<?> enums = paramProperties.propertyNames(); enums.hasMoreElements(); ) {
        String key = (String) enums.nextElement();
        try (ResultSet rs =
            statement.executeQuery(String.format("show parameters like '%s'", key))) {
          assertTrue(rs.next());
          String value = rs.getString("value");

          assertThat(key, value, equalTo("3600"));
        }
      }
      SFSession session = connection.unwrap(SnowflakeConnectionV1.class).getSfSession();
      assertEquals(3600, session.getHeartbeatFrequency());
    }
  }

  @Test
  public void testNativeSQL() throws Throwable {
    // today returning the source SQL.
    assertEquals("select 1", connection.nativeSQL("select 1"));
  }

  @Test
  public void testGetTypeMap() throws Throwable {
    // return an empty type map. setTypeMap is not supported.
    assertEquals(Collections.emptyMap(), connection.getTypeMap());
  }

  @Test
  public void testHolderbility() throws Throwable {
    try (Connection connection = getConnection()) {
      try {
        connection.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);
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
      assertThrows(
          SQLException.class,
          () -> {
            assertTrue(connection.isValid(-10));
          });
    }
  }

  @Test
  public void testUnwrapper() throws Throwable {
    try (Connection connection = getConnection()) {
      boolean canUnwrap = connection.isWrapperFor(SnowflakeConnectionV1.class);
      assertTrue(canUnwrap);
      SnowflakeConnectionV1 sfconnection = connection.unwrap(SnowflakeConnectionV1.class);
      sfconnection.createStatement();
      assertThrows(
          SQLException.class,
          () -> {
            connection.unwrap(SnowflakeDriver.class);
          });
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
  public void testReadDateAfterSplittingResultSet() throws Exception {
    Connection conn = getConnection();
    try (Statement statement = conn.createStatement()) {
      statement.execute("create or replace table table_with_date (int_c int, date_c date)");
      statement.execute("insert into table_with_date values (1, '2015-10-25')");

      try (ResultSet rs = statement.executeQuery("select * from table_with_date")) {
        final SnowflakeResultSet resultSet = rs.unwrap(SnowflakeResultSet.class);
        final long arbitrarySizeInBytes = 1024;
        final List<SnowflakeResultSetSerializable> serializables =
            resultSet.getResultSetSerializables(arbitrarySizeInBytes);
        final ArrayList<Date> dates = new ArrayList<>();
        for (SnowflakeResultSetSerializable s : serializables) {
          ResultSet srs = s.getResultSet();
          srs.next();
          dates.add(srs.getDate(2));
        }
        assertEquals(1, dates.size());
        assertEquals("2015-10-25", dates.get(0).toString());
      }
    }
  }

  @Test
  public void testResultSetsClosedByStatement() throws SQLException {
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
  }

  @ParameterizedTest
  @CsvSource({
    "db," + SESSION_CREATION_OBJECT_DOES_NOT_EXIST_NOT_AUTHORIZED,
    "schema," + SESSION_CREATION_OBJECT_DOES_NOT_EXIST_NOT_AUTHORIZED,
    "warehouse," + SESSION_CREATION_OBJECT_DOES_NOT_EXIST_NOT_AUTHORIZED,
    "role," + ROLE_IN_CONNECT_STRING_DOES_NOT_EXIST
  })
  public void testValidateDefaultParameters(String propertyName, int expectedErrorCode)
      throws Throwable {
    Map<String, String> params = getConnectionParameters();
    Properties props;

    props = setCommonConnectionParameters(true);
    props.put(propertyName, "NOT_EXISTS");
    SQLException ex =
        assertThrows(
            SQLException.class,
            () -> {
              DriverManager.getConnection(params.get("uri"), props);
            });
    assertEquals(ex.getErrorCode(), expectedErrorCode, "error code");
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
    Properties finalProps = props;
    SQLException ex =
        assertThrows(
            SQLException.class,
            () -> {
              DriverManager.getConnection(params.get("uri"), finalProps);
            });
    assertEquals(ex.getErrorCode(), ROLE_IN_CONNECT_STRING_DOES_NOT_EXIST, "error code");
  }

  /**
   * This is a manual test to check that making connections with the new regionless URL setup
   * (org-account.snowflake.com) works correctly in the driver. Currently dev/reg do not support
   * this format so the test must be manually run with a qa account.
   *
   * @throws SQLException
   */
  @Disabled
  @Test
  public void testOrgAccountUrl() throws SQLException {
    Properties props = new Properties();
    props.put("user", "admin");
    props.put("password", "Password1");
    props.put("role", "accountadmin");
    props.put("timezone", "UTC");
    try (Connection con =
            DriverManager.getConnection(
                "jdbc:snowflake://amoghorgurl-keypairauth_test_alias.testdns.snowflakecomputing.com",
                props);
        Statement statement = con.createStatement()) {
      statement.execute("select 1");
    }
  }

  /**
   * * This is a manual test to check that making connections with the new regionless URL setup
   * (org-account.snowflake.com) works correctly with key/pair authentication in the driver.
   * Currently dev/reg do not support this format so the test must be manually run with a qa
   * account.
   *
   * @throws SQLException
   * @throws NoSuchAlgorithmException
   */
  @Disabled
  @Test
  public void testOrgAccountUrlWithKeyPair() throws SQLException, NoSuchAlgorithmException {

    String uri =
        "jdbc:snowflake://amoghorgurl-keypairauth_test_alias.testdns.snowflakecomputing.com";
    KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
    SecureRandom random = SecureRandom.getInstance("SHA1PRNG");
    keyPairGenerator.initialize(2048, random);

    KeyPair keyPair = keyPairGenerator.generateKeyPair();
    PublicKey publicKey = keyPair.getPublic();
    PrivateKey privateKey = keyPair.getPrivate();

    Properties props = new Properties();
    props.put("user", "admin");
    props.put("password", "Password1");
    props.put("role", "accountadmin");
    props.put("timezone", "UTC");
    try (Connection connection = DriverManager.getConnection(uri, props);
        Statement statement = connection.createStatement()) {
      String encodePublicKey = Base64.encodeBase64String(publicKey.getEncoded());
      statement.execute(
          String.format("alter user %s set rsa_public_key='%s'", "admin", encodePublicKey));
    }

    props.remove("password");
    // test correct private key one
    props.put("privateKey", privateKey);
    try (Connection connection = DriverManager.getConnection(uri, props)) {}
  }

  private Properties kvMap2Properties(
      Map<String, String> params, boolean validateDefaultParameters) {
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

  private Properties setCommonConnectionParameters(boolean validateDefaultParameters) {
    Map<String, String> params = getConnectionParameters();
    return kvMap2Properties(params, validateDefaultParameters);
  }

  @Test
  public void testFailOverOrgAccount() throws SQLException {
    // only when set_git_info.sh picks up a SOURCE_PARAMETER_FILE
    assumeRunningOnGithubActions();

    Map<String, String> kvParams = getConnectionParameters(null, "ORG");
    Properties connProps = kvMap2Properties(kvParams, false);
    String uri = kvParams.get("uri");

    try (Connection con = DriverManager.getConnection(uri, connProps);
        Statement statement = con.createStatement()) {
      statement.execute("select 1");
    }
  }

  private class ConcurrentConnections implements Runnable {

    ConcurrentConnections() {}

    @Override
    public void run() {
      try (Connection con = getConnection();
          Statement statement = con.createStatement()) {
        statement.executeQuery("select * from bigTable");

      } catch (SQLException ex) {
        ex.printStackTrace();
      }
    }
  }
}
