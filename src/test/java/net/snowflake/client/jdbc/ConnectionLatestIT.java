/*
 * Copyright (c) 2012-2022 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static net.snowflake.client.core.SessionUtil.CLIENT_SESSION_KEEP_ALIVE_HEARTBEAT_FREQUENCY;
import static net.snowflake.client.jdbc.ConnectionIT.*;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.AnyOf.anyOf;
import static org.junit.Assert.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.time.Duration;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import net.snowflake.client.ConditionalIgnoreRule;
import net.snowflake.client.RunningOnGithubAction;
import net.snowflake.client.TestUtil;
import net.snowflake.client.category.TestCategoryConnection;
import net.snowflake.client.core.*;
import net.snowflake.client.jdbc.telemetryOOB.TelemetryService;
import net.snowflake.common.core.ClientAuthnDTO;
import net.snowflake.common.core.ClientAuthnParameter;
import net.snowflake.common.core.SqlState;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
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
  public void testDisableQueryContextCache() throws SQLException {
    // Disable QCC via prop
    Properties props = new Properties();
    props.put("disableQueryContextCache", "true");
    Connection con = getConnection(props);
    Statement statement = con.createStatement();
    statement.execute("select 1");
    SFSession session = con.unwrap(SnowflakeConnectionV1.class).getSfSession();
    // if QCC disable, this should be null
    assertNull(session.getQueryContextDTO());
    con.close();
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
    SFSession session = connection.unwrap(SnowflakeConnectionV1.class).getSfSession();
    assertEquals(1800, session.getHeartbeatFrequency());
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

    SFSession session = connection.unwrap(SnowflakeConnectionV1.class).getSfSession();
    assertEquals(900, session.getHeartbeatFrequency());
  }

  /**
   * Test that PUT/GET statements can return query IDs. If there is a group of files
   * uploaded/downloaded, the getResultSet() function will return the query ID of the last PUT or
   * GET statement in the batch.
   *
   * @throws Throwable
   */
  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void putGetStatementsHaveQueryID() throws Throwable {
    Connection con = getConnection();
    Statement statement = con.createStatement();
    String sourceFilePath = getFullPathFileInResource(TEST_DATA_FILE);
    File destFolder = tmpFolder.newFolder();
    String destFolderCanonicalPath = destFolder.getCanonicalPath();
    statement.execute("CREATE OR REPLACE STAGE testPutGet_stage");
    SnowflakeStatement snowflakeStatement = statement.unwrap(SnowflakeStatement.class);
    String createStageQueryId = snowflakeStatement.getQueryID();
    TestUtil.assertValidQueryId(createStageQueryId);
    String putStatement = "PUT file://" + sourceFilePath + " @testPutGet_stage";
    ResultSet resultSet = snowflakeStatement.executeAsyncQuery(putStatement);
    String statementPutQueryId = snowflakeStatement.getQueryID();
    TestUtil.assertValidQueryId(statementPutQueryId);
    assertNotEquals(
        "create query id is override by put query id", createStageQueryId, statementPutQueryId);
    String resultSetPutQueryId = resultSet.unwrap(SnowflakeResultSet.class).getQueryID();
    TestUtil.assertValidQueryId(resultSetPutQueryId);
    assertEquals(resultSetPutQueryId, statementPutQueryId);
    resultSet =
        snowflakeStatement.executeAsyncQuery(
            "GET @testPutGet_stage 'file://" + destFolderCanonicalPath + "' parallel=8");
    String statementGetQueryId = snowflakeStatement.getQueryID();
    String resultSetGetQueryId = resultSet.unwrap(SnowflakeResultSet.class).getQueryID();
    TestUtil.assertValidQueryId(resultSetGetQueryId);
    assertNotEquals(
        "put and get query id should be different", resultSetGetQueryId, resultSetPutQueryId);
    assertEquals(resultSetGetQueryId, statementGetQueryId);
  }

  /** Added in > 3.14.4 */
  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void putGetStatementsHaveQueryIDEvenWhenFail() throws Throwable {
    try (Connection con = getConnection();
        Statement statement = con.createStatement()) {
      String sourceFilePath = getFullPathFileInResource(TEST_DATA_FILE);
      File destFolder = tmpFolder.newFolder();
      String destFolderCanonicalPath = destFolder.getCanonicalPath();
      SnowflakeStatement snowflakeStatement = statement.unwrap(SnowflakeStatement.class);
      try {
        statement.executeQuery("PUT file://" + sourceFilePath + " @not_existing_state");
        fail("PUT statement should fail");
      } catch (SnowflakeSQLException e) {
        TestUtil.assertValidQueryId(snowflakeStatement.getQueryID());
        assertEquals(snowflakeStatement.getQueryID(), e.getQueryId());
      }
      String putQueryId = snowflakeStatement.getQueryID();
      try {
        statement.executeQuery(
            "GET @not_existing_state 'file://" + destFolderCanonicalPath + "' parallel=8");
        fail("GET statement should fail");
      } catch (SnowflakeSQLException e) {
        TestUtil.assertValidQueryId(snowflakeStatement.getQueryID());
        assertEquals(snowflakeStatement.getQueryID(), e.getQueryId());
      }
      String getQueryId = snowflakeStatement.getQueryID();
      assertNotEquals("put and get query id should be different", putQueryId, getQueryId);
      String stageName = "stage_" + SnowflakeUtil.randomAlphaNumeric(10);
      statement.execute("CREATE OR REPLACE STAGE " + stageName);
      TestUtil.assertValidQueryId(snowflakeStatement.getQueryID());
      try {
        statement.executeQuery("PUT file://not_existing_file @" + stageName);
        fail("PUT statement should fail");
      } catch (SnowflakeSQLException e) {
        TestUtil.assertValidQueryId(snowflakeStatement.getQueryID());
        assertEquals(snowflakeStatement.getQueryID(), e.getQueryId());
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
            .executeAsyncQuery("CALL SYSTEM$WAIT(60, 'SECONDS')");
    // Retrieve query ID for part 2 of test, check status of query
    String queryID = rs1.unwrap(SnowflakeResultSet.class).getQueryID();
    QueryStatusV2 statusV2 = null;
    for (int retry = 0; retry < 5; ++retry) {
      Thread.sleep(100);
      statusV2 = rs1.unwrap(SnowflakeResultSet.class).getStatusV2();
      // Sometimes 100 millis is too short for GS to get query status with provided queryID, in
      // which case we will get NO_DATA.
      if (statusV2.getStatus() != QueryStatus.NO_DATA) {
        break;
      }
    }
    // Query should take 60 seconds so should be running
    assertEquals(QueryStatus.RUNNING, statusV2.getStatus());
    assertEquals(QueryStatus.RUNNING.name(), statusV2.getName());
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
    statusV2 = rs.unwrap(SnowflakeResultSet.class).getStatusV2();
    // Assert status of query is a success
    assertEquals(QueryStatus.SUCCESS, statusV2.getStatus());
    assertEquals("No error reported", statusV2.getErrorMessage());
    assertEquals(0, statusV2.getErrorCode());
    assertEquals(1, getSizeOfResultSet(rs));
    statement = con.createStatement();
    // Create another query that will not be successful (querying table that does not exist)
    rs1 =
        statement
            .unwrap(SnowflakeStatement.class)
            .executeAsyncQuery("select * from nonexistentTable");
    Thread.sleep(100);
    statusV2 = rs1.unwrap(SnowflakeResultSet.class).getStatusV2();
    // when GS response is slow, allow up to 1 second of retries to get final query status
    int counter = 0;
    while ((statusV2.getStatus() == QueryStatus.NO_DATA
            || statusV2.getStatus() == QueryStatus.RUNNING)
        && counter < 10) {
      counter++;
      Thread.sleep(100);
      statusV2 = rs1.unwrap(SnowflakeResultSet.class).getStatusV2();
    }
    // If GS response is too slow to return data, do nothing to avoid flaky test failure. If
    // response has returned,
    // assert it is the error message that we are expecting.
    if (statusV2.getStatus() != QueryStatus.NO_DATA) {
      assertEquals(QueryStatus.FAILED_WITH_ERROR, statusV2.getStatus());
      assertEquals(2003, statusV2.getErrorCode());
      assertEquals(
          "SQL compilation error:\n"
              + "Object 'NONEXISTENTTABLE' does not exist or not authorized.",
          statusV2.getErrorMessage());
    }
    statement.close();
    con.close();
  }

  @Test
  public void testGetErrorMessageFromAsyncQuery() throws SQLException {
    Connection con = getConnection();
    Statement statement = con.createStatement();
    // Create another query that will not be successful (querying table that does not exist)
    ResultSet rs1 = statement.unwrap(SnowflakeStatement.class).executeAsyncQuery("bad query!");
    try {
      rs1.next();
    } catch (SQLException ex) {
      assertEquals(
          "Status of query associated with resultSet is FAILED_WITH_ERROR. SQL compilation error:\n"
              + "syntax error line 1 at position 0 unexpected 'bad'. Results not generated.",
          ex.getMessage());
      assertEquals(
          "SQL compilation error:\n" + "syntax error line 1 at position 0 unexpected 'bad'.",
          rs1.unwrap(SnowflakeResultSet.class).getQueryErrorMessage());
    }
    rs1 = statement.unwrap(SnowflakeStatement.class).executeAsyncQuery("select 1");
    rs1.next();
    // Assert there is no error message when query is successful
    assertEquals("No error reported", rs1.unwrap(SnowflakeResultSet.class).getQueryErrorMessage());
    rs1.close();
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

  /**
   * Tests that error message and error code are reset after an error. This test is not reliable as
   * it uses sleep() call. It works locally but failed with PR.
   *
   * @throws SQLException
   * @throws InterruptedException
   */
  // @Test
  public void testQueryStatusErrorMessageAndErrorCode() throws SQLException, InterruptedException {
    // open connection and run asynchronous query
    Connection con = getConnection();
    Statement statement = con.createStatement();
    statement.execute("create or replace table testTable(colA string, colB boolean)");
    statement.execute("insert into testTable values ('test', true)");
    ResultSet rs1 =
        statement.unwrap(SnowflakeStatement.class).executeAsyncQuery("select * from testTable");
    QueryStatus status = rs1.unwrap(SnowflakeResultSet.class).getStatus();
    // Set the error message and error code so we can confirm they are reset when getStatus() is
    // called.
    status.setErrorMessage(QueryStatus.FAILED_WITH_ERROR.toString());
    status.setErrorCode(2003);
    Thread.sleep(300);
    status = rs1.unwrap(SnowflakeResultSet.class).getStatus();
    // Assert status of query is a success
    assertEquals(QueryStatus.SUCCESS, status);
    assertEquals("No error reported", status.getErrorMessage());
    assertEquals(0, status.getErrorCode());
    statement.execute("drop table if exists testTable");
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
      QueryStatus.NO_DATA,
      QueryStatus.BLOCKED
    };

    QueryStatus[] otherStatuses = {
      QueryStatus.ABORTED,
      QueryStatus.ABORTING,
      QueryStatus.SUCCESS,
      QueryStatus.FAILED_WITH_ERROR,
      QueryStatus.FAILED_WITH_INCIDENT,
      QueryStatus.DISCONNECTED,
      QueryStatus.RESTARTED
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
    // Adding authenticator type for code coverage purposes
    properties.put("authenticator", ClientAuthnDTO.AuthenticatorType.SNOWFLAKE.toString());
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
        assertThat(
            "Communication error",
            e.getErrorCode(),
            anyOf(equalTo(INVALID_CONNECTION_INFO_CODE), equalTo(BAD_REQUEST_GS_CODE)));

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
    // Adding authenticator type for code coverage purposes
    properties.put("authenticator", ClientAuthnDTO.AuthenticatorType.SNOWFLAKE.toString());
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
      assertThat("Login time out not taking effective", conEnd - connStart < 300000);

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
      assertThat("Login time out not taking effective", conEnd - connStart < 300000);
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
    properties.put("authenticator", ClientAuthnDTO.AuthenticatorType.SNOWFLAKE_JWT.toString());
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

  // Wait for the async query finish
  private void waitForAsyncQueryDone(Connection connection, String queryID) throws Exception {
    SFSession session = connection.unwrap(SnowflakeConnectionV1.class).getSfSession();
    QueryStatus qs = session.getQueryStatus(queryID);
    while (QueryStatus.isStillRunning(qs)) {
      Thread.sleep(1000);
      qs = session.getQueryStatus(queryID);
    }
  }

  @Test
  public void testGetChildQueryIdsForAsyncSingleStatement() throws Exception {
    Connection connection = getConnection();
    Connection connection2 = getConnection();

    Statement statement = connection.createStatement();
    String query0 = "create or replace temporary table test_multi (cola int);";
    String query1 = "insert into test_multi VALUES (111), (222);";
    String query2 = "select cola from test_multi order by cola asc";

    // Get ResultSet for first statement
    ResultSet rs = statement.unwrap(SnowflakeStatement.class).executeAsyncQuery(query0);
    String queryID = rs.unwrap(SnowflakeResultSet.class).getQueryID();
    waitForAsyncQueryDone(connection, queryID);
    String[] queryIDs = connection.unwrap(SnowflakeConnectionV1.class).getChildQueryIds(queryID);
    assert (queryIDs.length == 1);
    rs = connection.unwrap(SnowflakeConnection.class).createResultSet(queryIDs[0]);
    assertTrue(rs.next());
    assertEquals(rs.getString(1), "Table TEST_MULTI successfully created.");
    assertFalse(rs.next());

    // Get ResultSet for second statement in another connection
    rs = statement.unwrap(SnowflakeStatement.class).executeAsyncQuery(query1);
    queryID = rs.unwrap(SnowflakeResultSet.class).getQueryID();
    waitForAsyncQueryDone(connection2, queryID);
    queryIDs = connection2.unwrap(SnowflakeConnectionV1.class).getChildQueryIds(queryID);
    assert (queryIDs.length == 1);
    rs = connection2.unwrap(SnowflakeConnection.class).createResultSet(queryIDs[0]);
    assertTrue(rs.next());
    assertEquals(rs.getInt(1), 2); // insert 2 rows
    assertFalse(rs.next());

    // Get ResultSet for third statement in another connection
    rs = statement.unwrap(SnowflakeStatement.class).executeAsyncQuery(query2);
    queryID = rs.unwrap(SnowflakeResultSet.class).getQueryID();
    waitForAsyncQueryDone(connection2, queryID);
    queryIDs = connection2.unwrap(SnowflakeConnectionV1.class).getChildQueryIds(queryID);
    assert (queryIDs.length == 1);
    rs = connection2.unwrap(SnowflakeConnection.class).createResultSet(queryIDs[0]);
    assertTrue(rs.next());
    assertEquals(rs.getInt(1), 111);
    assertTrue(rs.next());
    assertEquals(rs.getInt(1), 222);
    assertFalse(rs.next());

    connection.close();
    connection2.close();
  }

  @Test
  public void testGetChildQueryIdsForAsyncMultiStatement() throws Exception {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();
    String multiStmtQuery =
        "create or replace temporary table test_multi (cola int);"
            + "insert into test_multi VALUES (111), (222);"
            + "select cola from test_multi order by cola asc";

    statement.unwrap(SnowflakeStatement.class).setParameter("MULTI_STATEMENT_COUNT", 3);
    ResultSet rs = statement.unwrap(SnowflakeStatement.class).executeAsyncQuery(multiStmtQuery);
    String queryID = rs.unwrap(SnowflakeResultSet.class).getQueryID();
    statement.close();
    connection.close();

    // Get the child query IDs in a new connection
    connection = getConnection();
    waitForAsyncQueryDone(connection, queryID);
    String[] queryIDs = connection.unwrap(SnowflakeConnectionV1.class).getChildQueryIds(queryID);
    assert (queryIDs.length == 3);

    // First statement ResultSet
    rs = connection.unwrap(SnowflakeConnection.class).createResultSet(queryIDs[0]);
    assertTrue(rs.next());
    assertEquals(rs.getString(1), "Table TEST_MULTI successfully created.");
    assertFalse(rs.next());

    // Second statement ResultSet
    rs = connection.unwrap(SnowflakeConnection.class).createResultSet(queryIDs[1]);
    assertTrue(rs.next());
    assertEquals(rs.getInt(1), 2);
    assertFalse(rs.next());

    // Third statement ResultSet
    rs = connection.unwrap(SnowflakeConnection.class).createResultSet(queryIDs[2]);
    assertTrue(rs.next());
    assertEquals(rs.getInt(1), 111);
    assertTrue(rs.next());
    assertEquals(rs.getInt(1), 222);
    assertFalse(rs.next());

    connection.close();
  }

  @Test
  public void testGetChildQueryIdsNegativeTestQueryIsRunning() throws Exception {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();
    String multiStmtQuery = "select 1; call system$wait(10); select 2";

    statement.unwrap(SnowflakeStatement.class).setParameter("MULTI_STATEMENT_COUNT", 3);
    ResultSet rs = statement.unwrap(SnowflakeStatement.class).executeAsyncQuery(multiStmtQuery);
    String queryID = rs.unwrap(SnowflakeResultSet.class).getQueryID();

    try {
      connection.unwrap(SnowflakeConnectionV1.class).getChildQueryIds(queryID);
      fail("The getChildQueryIds() should fail because query is running");
    } catch (SQLException ex) {
      String msg = ex.getMessage();
      if (!msg.contains("Status of query associated with resultSet is")
          || !msg.contains("Results not generated.")) {
        ex.printStackTrace();
        QueryStatus qs =
            connection.unwrap(SnowflakeConnectionV1.class).getSfSession().getQueryStatus(queryID);
        fail("Don't get expected message, query Status: " + qs + " actual message is: " + msg);
      }
    } finally {
      connection.createStatement().execute("select system$cancel_query('" + queryID + "')");
      statement.close();
      connection.close();
    }
  }

  @Test
  public void testGetChildQueryIdsNegativeTestQueryFailed() throws Exception {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();
    String multiStmtQuery = "select 1; select to_date('not_date'); select 2";

    statement.unwrap(SnowflakeStatement.class).setParameter("MULTI_STATEMENT_COUNT", 3);
    ResultSet rs = statement.unwrap(SnowflakeStatement.class).executeAsyncQuery(multiStmtQuery);
    String queryID = rs.unwrap(SnowflakeResultSet.class).getQueryID();

    try {
      waitForAsyncQueryDone(connection, queryID);
      connection.unwrap(SnowflakeConnectionV1.class).getChildQueryIds(queryID);
      fail("The getChildQueryIds() should fail because the query fails");
    } catch (SQLException ex) {
      assertTrue(
          ex.getMessage()
              .contains(
                  "Uncaught Execution of multiple statements failed on statement \"select"
                      + " to_date('not_date')\""));
    } finally {
      statement.close();
      connection.close();
    }
  }

  /**
   * See SNOW-496117 for more details. Don't run on github because the github testing deployment is
   * likely not having the test account we used here.
   */
  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testAuthenticatorEndpointWithDashInAccountName() throws Exception {
    Map<String, String> params = getConnectionParameters();
    String serverUrl =
        String.format(
            "%s://%s:%s",
            params.get("ssl").equals("on") ? "https" : "http",
            params.get("host"),
            params.get("port"));

    HttpPost postRequest =
        new HttpPost(
            new URIBuilder(serverUrl).setPath(SessionUtil.SF_PATH_AUTHENTICATOR_REQUEST).build());

    Map<String, Object> data =
        Collections.singletonMap(ClientAuthnParameter.ACCOUNT_NAME.name(), "snowhouse-local");
    ClientAuthnDTO authnData = new ClientAuthnDTO();
    authnData.setData(data);

    ObjectMapper mapper = ObjectMapperFactory.getObjectMapper();
    String json = mapper.writeValueAsString(authnData);

    StringEntity input = new StringEntity(json, StandardCharsets.UTF_8);
    input.setContentType("application/json");
    postRequest.setEntity(input);
    postRequest.addHeader("accept", "application/json");

    String theString =
        HttpUtil.executeGeneralRequest(postRequest, 60, 0, 0, 0, new HttpClientSettingsKey(null));

    JsonNode jsonNode = mapper.readTree(theString);
    assertEquals(
        "{\"data\":null,\"code\":null,\"message\":null,\"success\":true}", jsonNode.toString());
  }

  @Test
  public void testReadOnly() throws Throwable {
    try (Connection connection = getConnection()) {

      connection.setReadOnly(true);
      assertEquals(connection.isReadOnly(), false);

      connection.setReadOnly(false);
      connection.createStatement().execute("create or replace table readonly_test(c1 int)");
      assertFalse(connection.isReadOnly());
      connection.createStatement().execute("drop table if exists readonly_test");
    }
  }

  @Test
  public void testDownloadStreamWithFileNotFoundException() throws SQLException {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();
    statement.execute("CREATE OR REPLACE TEMP STAGE testDownloadStream_stage");
    long startDownloadTime = System.currentTimeMillis();
    try {
      connection
          .unwrap(SnowflakeConnection.class)
          .downloadStream("@testDownloadStream_stage", "/fileNotExist.gz", true);
    } catch (SQLException ex) {
      assertThat(ex.getErrorCode(), is(ErrorCode.S3_OPERATION_ERROR.getMessageCode()));
    }
    long endDownloadTime = System.currentTimeMillis();
    // S3Client retries some exception for a default timeout of 5 minutes
    // Check that 404 was not retried
    assertTrue(endDownloadTime - startDownloadTime < 400000);
  }

  @Test
  public void testIsAsyncSession() throws SQLException, InterruptedException {
    // Run a query that takes > 4 seconds to complete
    try (Connection con = getConnection();
        Statement statement = con.createStatement();
        ResultSet rs =
            statement
                .unwrap(SnowflakeStatement.class)
                .executeAsyncQuery("select count(*) from table(generator(timeLimit => 4))")) {
      // Assert that activeAsyncQueries is non-empty with running query. Session is async and not
      // safe to close
      long start = System.currentTimeMillis();
      SnowflakeConnectionV1 snowflakeConnection = con.unwrap(SnowflakeConnectionV1.class);
      assertTrue(snowflakeConnection.getSfSession().isAsyncSession());
      assertFalse(snowflakeConnection.getSfSession().isSafeToClose());
      // ensure that query is finished
      assertTrue(rs.next());
      // ensure query took > 4 seconds
      assertTrue(
          Duration.ofMillis(System.currentTimeMillis() - start).compareTo(Duration.ofSeconds(4))
              > 0);
      // Assert that there are no longer any queries running.
      // First, assert session is safe to close. This iterates through active queries, fetches their
      // status, and removes them from the activeQueriesMap if they are no longer active.
      assertTrue(snowflakeConnection.getSfSession().isSafeToClose());
      // Next, assert session is no longer async (just fetches size of activeQueriesMap with no
      // other action)
      assertFalse(snowflakeConnection.getSfSession().isAsyncSession());
    }
  }
}
