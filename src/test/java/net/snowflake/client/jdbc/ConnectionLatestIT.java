/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static net.snowflake.client.jdbc.ConnectionIT.INVALID_CONNECTION_INFO_CODE;
import static net.snowflake.client.jdbc.ConnectionIT.WAIT_FOR_TELEMETRY_REPORT_IN_MILLISECS;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.*;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import net.snowflake.client.ConditionalIgnoreRule;
import net.snowflake.client.RunningOnGithubAction;
import net.snowflake.client.category.TestCategoryConnection;
import net.snowflake.client.core.QueryStatus;
import net.snowflake.client.jdbc.telemetryOOB.TelemetryService;
import net.snowflake.common.core.SqlState;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

/**
 * Connection integration tests for the latest JDBC driver. This doesn't work for the oldest
 * supported driver. Revisit this tests whenever bumping up the oldest supported driver to examine
 * if the tests still is not applicable. If it is applicable, move tests to ConnectionIT so that
 * both the latest and oldest supported driver run the tests.
 */
@Category(TestCategoryConnection.class)
public class ConnectionLatestIT extends BaseJDBCTest {
  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

  private boolean defaultState;

  @Before
  public void setUp() {
    TelemetryService service = TelemetryService.getInstance();
    service.updateContextForIT(getConnectionParameters());
    defaultState = service.isEnabled();
    service.setNumOfRetryToTriggerTelemetry(3);
    TelemetryService.enable();
  }

  @After
  public void tearDown() throws InterruptedException {
    TelemetryService service = TelemetryService.getInstance();
    // wait 5 seconds while the service is flushing
    TimeUnit.SECONDS.sleep(5);

    if (defaultState) {
      TelemetryService.enable();
    } else {
      TelemetryService.disable();
    }
    service.resetNumOfRetryToTriggerTelemetry();
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
    QueryStatus status = null;
    for (int retry = 0; retry < 5; ++retry) {
      Thread.sleep(100);
      status = rs1.unwrap(SnowflakeResultSet.class).getStatus();
      // Sometimes 100 millis is too short for GS to get query status with provided queryID, in
      // which case we will get NO_DATA.
      if (status != QueryStatus.NO_DATA) {
        break;
      }
    }
    // Query should take 60 seconds so should be running
    assertEquals(QueryStatus.RUNNING, status);
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
    // when GS response is slow, allow up to 1 second of retries to get final query status
    int counter = 0;
    while ((status == QueryStatus.NO_DATA || status == QueryStatus.RUNNING) && counter < 10) {
      Thread.sleep(100);
      status = rs1.unwrap(SnowflakeResultSet.class).getStatus();
    }
    // If GS response is too slow to return data, do nothing to avoid flaky test failure. If
    // response has returned,
    // assert it is the error message that we are expecting.
    if (status != QueryStatus.NO_DATA) {
      assertEquals(QueryStatus.FAILED_WITH_ERROR, status);
      assertEquals(2003, status.getErrorCode());
    }
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
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
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
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
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
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
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
}
