/*
 * Copyright (c) 2012-2022 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static net.snowflake.client.core.SessionUtil.CLIENT_SESSION_KEEP_ALIVE_HEARTBEAT_FREQUENCY;
import static net.snowflake.client.jdbc.ConnectionIT.BAD_REQUEST_GS_CODE;
import static net.snowflake.client.jdbc.ConnectionIT.INVALID_CONNECTION_INFO_CODE;
import static net.snowflake.client.jdbc.ConnectionIT.WAIT_FOR_TELEMETRY_REPORT_IN_MILLISECS;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.AnyOf.anyOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import net.snowflake.client.ConditionalIgnoreRule;
import net.snowflake.client.RunningNotOnAWS;
import net.snowflake.client.RunningOnGithubAction;
import net.snowflake.client.TestUtil;
import net.snowflake.client.category.TestCategoryConnection;
import net.snowflake.client.core.HttpClientSettingsKey;
import net.snowflake.client.core.HttpUtil;
import net.snowflake.client.core.ObjectMapperFactory;
import net.snowflake.client.core.QueryStatus;
import net.snowflake.client.core.SFSession;
import net.snowflake.client.core.SFSessionProperty;
import net.snowflake.client.core.SecurityUtil;
import net.snowflake.client.core.SessionUtil;
import net.snowflake.client.jdbc.telemetryOOB.TelemetryService;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.common.core.ClientAuthnDTO;
import net.snowflake.common.core.ClientAuthnParameter;
import net.snowflake.common.core.SqlState;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
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
  private static final SFLogger logger = SFLoggerFactory.getLogger(ConnectionLatestIT.class);

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
    System.clearProperty(SecurityUtil.ENABLE_BOUNCYCASTLE_PROVIDER_JVM);
  }

  @Test
  public void testDisableQueryContextCache() throws SQLException {
    // Disable QCC via prop
    Properties props = new Properties();
    props.put("disableQueryContextCache", "true");
    try (Connection con = getConnection(props);
        Statement statement = con.createStatement()) {
      statement.execute("select 1");
      SFSession session = con.unwrap(SnowflakeConnectionV1.class).getSfSession();
      // if QCC disable, this should be null
      assertNull(session.getQueryContextDTO());
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
    try (Connection connection = getConnection(paramProperties)) {
      for (Enumeration<?> enums = paramProperties.propertyNames(); enums.hasMoreElements(); ) {
        String key = (String) enums.nextElement();
        try (ResultSet rs =
            connection
                .createStatement()
                .executeQuery(String.format("show parameters like '%s'", key))) {
          assertTrue(rs.next());
          String value = rs.getString("value");

          assertThat(key, value, equalTo(paramProperties.get(key).toString()));
        }
      }
      SFSession session = connection.unwrap(SnowflakeConnectionV1.class).getSfSession();
      assertEquals(1800, session.getHeartbeatFrequency());
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
    try (Connection connection = getConnection(paramProperties)) {
      for (Enumeration<?> enums = paramProperties.propertyNames(); enums.hasMoreElements(); ) {
        String key = (String) enums.nextElement();
        try (ResultSet rs =
            connection
                .createStatement()
                .executeQuery(String.format("show parameters like '%s'", key))) {
          assertTrue(rs.next());
          String value = rs.getString("value");

          assertThat(key, value, equalTo("900"));
        }
      }

      SFSession session = connection.unwrap(SnowflakeConnectionV1.class).getSfSession();
      assertEquals(900, session.getHeartbeatFrequency());
    }
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
    try (Connection con = getConnection();
        Statement statement = con.createStatement()) {
      String sourceFilePath = getFullPathFileInResource(TEST_DATA_FILE);
      File destFolder = tmpFolder.newFolder();
      String destFolderCanonicalPath = destFolder.getCanonicalPath();
      statement.execute("CREATE OR REPLACE STAGE testPutGet_stage");
      SnowflakeStatement snowflakeStatement = statement.unwrap(SnowflakeStatement.class);
      String createStageQueryId = snowflakeStatement.getQueryID();
      TestUtil.assertValidQueryId(createStageQueryId);
      String putStatement = "PUT file://" + sourceFilePath + " @testPutGet_stage";
      String resultSetPutQueryId = "";
      try (ResultSet resultSet = snowflakeStatement.executeAsyncQuery(putStatement)) {
        String statementPutQueryId = snowflakeStatement.getQueryID();
        TestUtil.assertValidQueryId(statementPutQueryId);
        assertNotEquals(
            "create query id is override by put query id", createStageQueryId, statementPutQueryId);
        resultSetPutQueryId = resultSet.unwrap(SnowflakeResultSet.class).getQueryID();
        TestUtil.assertValidQueryId(resultSetPutQueryId);
        assertEquals(resultSetPutQueryId, statementPutQueryId);
      }
      try (ResultSet resultSet =
          snowflakeStatement.executeAsyncQuery(
              "GET @testPutGet_stage 'file://" + destFolderCanonicalPath + "' parallel=8")) {
        String statementGetQueryId = snowflakeStatement.getQueryID();
        String resultSetGetQueryId = resultSet.unwrap(SnowflakeResultSet.class).getQueryID();
        TestUtil.assertValidQueryId(resultSetGetQueryId);
        assertNotEquals(
            "put and get query id should be different", resultSetGetQueryId, resultSetPutQueryId);
        assertEquals(resultSetGetQueryId, statementGetQueryId);
      }
    }
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
    String queryID = null;
    QueryStatusV2 statusV2 = null;
    try (Connection con = getConnection();
        Statement statement = con.createStatement();
        ResultSet rs1 =
            statement
                .unwrap(SnowflakeStatement.class)
                .executeAsyncQuery("CALL SYSTEM$WAIT(60, 'SECONDS')")) {
      // Retrieve query ID for part 2 of test, check status of query
      queryID = rs1.unwrap(SnowflakeResultSet.class).getQueryID();

      SnowflakeResultSet sfrs = rs1.unwrap(SnowflakeResultSet.class);
      await()
          .atMost(Duration.ofSeconds(5))
          .until(() -> sfrs.getStatusV2().getStatus(), not(equalTo(QueryStatus.NO_DATA)));
      statusV2 = sfrs.getStatusV2();
      // Query should take 60 seconds so should be running
      assertEquals(QueryStatus.RUNNING, statusV2.getStatus());
      assertEquals(QueryStatus.RUNNING.name(), statusV2.getName());
      // close connection and wait for 1 minute while query finishes running
    }

    Thread.sleep(1000 * 70);
    // Create a new connection and new instance of a resultSet using query ID
    try (Connection con = getConnection()) {
      try (ResultSet rs =
          con.unwrap(SnowflakeConnection.class).createResultSet("Totally invalid query ID")) {
        fail("Query ID should be rejected");
      } catch (SQLException e) {
        assertEquals(SqlState.INVALID_PARAMETER_VALUE, e.getSQLState());
      }
      try (ResultSet rs = con.unwrap(SnowflakeConnection.class).createResultSet(queryID)) {
        statusV2 = rs.unwrap(SnowflakeResultSet.class).getStatusV2();
        // Assert status of query is a success
        assertEquals(QueryStatus.SUCCESS, statusV2.getStatus());
        assertEquals("No error reported", statusV2.getErrorMessage());
        assertEquals(0, statusV2.getErrorCode());
        assertEquals(1, getSizeOfResultSet(rs));
        try (Statement statement = con.createStatement();
            // Create another query that will not be successful (querying table that does not exist)
            ResultSet rs1 =
                statement
                    .unwrap(SnowflakeStatement.class)
                    .executeAsyncQuery("select * from nonexistentTable")) {
          Thread.sleep(100);
          statusV2 = rs1.unwrap(SnowflakeResultSet.class).getStatusV2();
          // when GS response is slow, allow up to 1 second of retries to get final query status
          SnowflakeResultSet sfrs1 = rs1.unwrap(SnowflakeResultSet.class);
          await()
              .atMost(Duration.ofSeconds(10))
              .until(
                  () -> {
                    QueryStatus qs = sfrs1.getStatusV2().getStatus();
                    return !(qs == QueryStatus.NO_DATA || qs == QueryStatus.RUNNING);
                  });
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
        }
      }
    }
  }

  @Test
  public void testGetErrorMessageFromAsyncQuery() throws SQLException {
    try (Connection con = getConnection();
        Statement statement = con.createStatement()) {
      // Create another query that will not be successful (querying table that does not exist)
      try (ResultSet rs1 =
          statement.unwrap(SnowflakeStatement.class).executeAsyncQuery("bad query!")) {
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
      }
      try (ResultSet rs2 =
          statement.unwrap(SnowflakeStatement.class).executeAsyncQuery("select 1")) {
        rs2.next();
        // Assert there is no error message when query is successful
        assertEquals(
            "No error reported", rs2.unwrap(SnowflakeResultSet.class).getQueryErrorMessage());
      }
    }
  }

  @Test
  public void testAsyncAndSynchronousQueries() throws SQLException {
    try (Connection con = getConnection();
        Statement statement = con.createStatement()) {
      // execute some statements that you want to be synchronous
      try {
        statement.execute("alter session set CLIENT_TIMESTAMP_TYPE_MAPPING=TIMESTAMP_TZ");
        statement.execute("create or replace table smallTable (colA string, colB int)");
        statement.execute("create or replace table uselessTable (colA string, colB int)");
        statement.execute("insert into smallTable values ('row1', 1), ('row2', 2), ('row3', 3)");
        statement.execute("insert into uselessTable values ('row1', 1), ('row2', 2), ('row3', 3)");
        // Select from uselessTable asynchronously; drop it synchronously afterwards
        try (ResultSet rs =
                statement
                    .unwrap(SnowflakeStatement.class)
                    .executeAsyncQuery("select * from smallTable");
            // execute a query that you don't want to wait for
            ResultSet rs1 =
                statement
                    .unwrap(SnowflakeStatement.class)
                    .executeAsyncQuery("select * from uselessTable");
            // Drop the table that was queried asynchronously. Should not drop until after async
            // query
            // finishes, because this
            // query IS synchronous
            ResultSet rs2 = statement.executeQuery("drop table uselessTable")) {
          while (rs2.next()) {
            assertEquals("USELESSTABLE successfully dropped.", rs2.getString(1));
          }
          // able to successfully fetch results in spite of table being dropped
          assertEquals(3, getSizeOfResultSet(rs1));
          statement.execute("alter session set CLIENT_TIMESTAMP_TYPE_MAPPING=TIMESTAMP_LTZ");

          // come back to the asynchronously executed result set after finishing other things
          assertTrue(rs.next());
          assertEquals(rs.getString(1), "row1");
          assertEquals(rs.getInt(2), 1);
          assertTrue(rs.next());
          assertEquals(rs.getString(1), "row2");
          assertEquals(rs.getInt(2), 2);
          assertTrue(rs.next());
          assertEquals(rs.getString(1), "row3");
          assertEquals(rs.getInt(2), 3);
        }
      } finally {
        statement.execute("drop table smallTable");
      }
    }
  }

  /** Can be used in > 3.14.4 (when {@link QueryStatusV2} was added) */
  @Test
  public void testQueryStatusErrorMessageAndErrorCodeChangeOnAsyncQuery() throws SQLException {
    try (Connection con = getConnection();
        Statement statement = con.createStatement();
        ResultSet rs1 =
            statement
                .unwrap(SnowflakeStatement.class)
                .executeAsyncQuery("select count(*) from table(generator(timeLimit => 2))")) {
      SnowflakeResultSet sfResultSet = rs1.unwrap(SnowflakeResultSet.class);
      // status should change state to RUNNING and then to SUCCESS
      await()
          .atMost(Duration.ofSeconds(10))
          .until(() -> sfResultSet.getStatusV2().getStatus(), equalTo(QueryStatus.RUNNING));

      // it may take more time to finish the test when running in parallel in CI builds
      await()
          .atMost(Duration.ofSeconds(360))
          .until(() -> sfResultSet.getStatusV2().getStatus(), equalTo(QueryStatus.SUCCESS));
    }
  }

  @Test
  public void testPreparedStatementAsyncQuery() throws SQLException {
    try (Connection con = getConnection();
        Statement statement = con.createStatement()) {
      try {
        statement.execute("create or replace table testTable(colA string, colB boolean)");
        try (PreparedStatement prepStatement =
            con.prepareStatement("insert into testTable values (?,?)")) {
          prepStatement.setInt(1, 33);
          prepStatement.setBoolean(2, true);
          // call executeAsyncQuery
          try (ResultSet rs =
              prepStatement.unwrap(SnowflakePreparedStatement.class).executeAsyncQuery()) {
            // Get access to results by calling next() function
            // next () will block until results are ready
            assertTrue(rs.next());
            // the resultSet consists of a single row, single column containing the number of rows
            // that have
            // been updated by the insert
            // the number of updated rows in testTable is 1 so 1 is returned
            assertEquals(rs.getString(1), "1");
          }
        }
      } finally {
        statement.execute("drop table testTable");
      }
    }
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
    try (Connection con = getConnection();
        Statement statement = con.createStatement();
        ResultSet rs =
            statement
                .unwrap(SnowflakeStatement.class)
                .executeAsyncQuery("select count(*) from table(generator(timeLimit => 5))")) {
      Thread.sleep(100);
      QueryStatus status = rs.unwrap(SnowflakeResultSet.class).getStatus();
      // Since warehouse has just been restarted, warehouse should still be booting
      assertEquals(QueryStatus.RESUMING_WAREHOUSE, status);

      // now try to get QUEUED status
      try (ResultSet rs1 =
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
                  .executeAsyncQuery("select count(*) from table(generator(timeLimit => 60))")) {
        // Retrieve query ID for part 2 of test, check status of query
        Thread.sleep(100);
        status = rs4.unwrap(SnowflakeResultSet.class).getStatus();
        // Since 4 queries were started at once, status is most likely QUEUED
        assertEquals(QueryStatus.QUEUED, status);
      }
    }
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
    try (Connection con = getConnection();
        Statement statement = con.createStatement()) {
      statement.execute("use role accountadmin");
      String pathfile = getFullPathFileInResource("encrypted_rsa_key.pub");
      String pubKey = new String(Files.readAllBytes(Paths.get(pathfile)));
      pubKey = pubKey.replace("-----BEGIN PUBLIC KEY-----", "");
      pubKey = pubKey.replace("-----END PUBLIC KEY-----", "");
      statement.execute(
          String.format("alter user %s set rsa_public_key='%s'", params.get("user"), pubKey));
    }

    try (Connection con = ds.getConnection();
        Statement statement = con.createStatement();
        ResultSet resultSet = statement.executeQuery("select 1")) {
      resultSet.next();
      assertThat("select 1", resultSet.getInt(1), equalTo(1));
    }
    File serializedFile = tmpFolder.newFile("serializedStuff.ser");
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
      try (Connection con = ds2.getConnection();
          Statement statement = con.createStatement()) {
        ResultSet resultSet = statement.executeQuery("select 1");
        resultSet.next();
        assertThat("select 1", resultSet.getInt(1), equalTo(1));
      }

      // clean up
      try (Connection connection = getConnection()) {
        Statement statement = connection.createStatement();
        statement.execute("use role accountadmin");
        statement.execute(String.format("alter user %s unset rsa_public_key", params.get("user")));
      }
    }
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testPrivateKeyInConnectionString() throws SQLException, IOException {
    Map<String, String> parameters = getConnectionParameters();
    String testUser = parameters.get("user");
    String pathfile = null;
    String pubKey = null;
    // Test with non-password-protected private key file (.pem)
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("use role accountadmin");
      pathfile = getFullPathFileInResource("rsa_key.pub");
      pubKey = new String(Files.readAllBytes(Paths.get(pathfile)));
      pubKey = pubKey.replace("-----BEGIN PUBLIC KEY-----", "");
      pubKey = pubKey.replace("-----END PUBLIC KEY-----", "");
      statement.execute(String.format("alter user %s set rsa_public_key='%s'", testUser, pubKey));
    }

    // PKCS #8
    String privateKeyLocation = getFullPathFileInResource("rsa_key.p8");
    String uri = parameters.get("uri") + "/?private_key_file=" + privateKeyLocation;
    Properties properties = new Properties();
    properties.put("account", parameters.get("account"));
    properties.put("user", testUser);
    properties.put("ssl", parameters.get("ssl"));
    properties.put("port", parameters.get("port"));
    try (Connection connection = DriverManager.getConnection(uri, properties)) {}

    // PKCS #1
    privateKeyLocation = getFullPathFileInResource("rsa_key.pem");
    uri = parameters.get("uri") + "/?private_key_file=" + privateKeyLocation;
    properties = new Properties();
    properties.put("account", parameters.get("account"));
    properties.put("user", testUser);
    properties.put("ssl", parameters.get("ssl"));
    properties.put("port", parameters.get("port"));
    properties.put("authenticator", ClientAuthnDTO.AuthenticatorType.SNOWFLAKE_JWT.toString());
    try (Connection connection = DriverManager.getConnection(uri, properties)) {}

    // test with password-protected private key file (.p8)
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("use role accountadmin");
      pathfile = getFullPathFileInResource("encrypted_rsa_key.pub");
      pubKey = new String(Files.readAllBytes(Paths.get(pathfile)));
      pubKey = pubKey.replace("-----BEGIN PUBLIC KEY-----", "");
      pubKey = pubKey.replace("-----END PUBLIC KEY-----", "");
      statement.execute(String.format("alter user %s set rsa_public_key='%s'", testUser, pubKey));
    }

    privateKeyLocation = getFullPathFileInResource("encrypted_rsa_key.p8");
    uri =
        parameters.get("uri")
            + "/?private_key_file_pwd=test&private_key_file="
            + privateKeyLocation;

    try (Connection connection = DriverManager.getConnection(uri, properties)) {}
    // test with incorrect password for private key
    uri =
        parameters.get("uri")
            + "/?private_key_file_pwd=wrong_password&private_key_file="
            + privateKeyLocation;

    try (Connection connection = DriverManager.getConnection(uri, properties)) {
      fail();
    } catch (SQLException e) {
      assertEquals(
          (int) ErrorCode.INVALID_OR_UNSUPPORTED_PRIVATE_KEY.getMessageCode(), e.getErrorCode());
    }

    // test with invalid public/private key combo (using 1st public key with 2nd private key)
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("use role accountadmin");
      pathfile = getFullPathFileInResource("rsa_key.pub");
      pubKey = new String(Files.readAllBytes(Paths.get(pathfile)));
      pubKey = pubKey.replace("-----BEGIN PUBLIC KEY-----", "");
      pubKey = pubKey.replace("-----END PUBLIC KEY-----", "");
      statement.execute(String.format("alter user %s set rsa_public_key='%s'", testUser, pubKey));
    }

    privateKeyLocation = getFullPathFileInResource("encrypted_rsa_key.p8");
    uri =
        parameters.get("uri")
            + "/?private_key_file_pwd=test&private_key_file="
            + privateKeyLocation;
    try (Connection connection = DriverManager.getConnection(uri, properties)) {
      fail();
    } catch (SQLException e) {
      assertEquals(390144, e.getErrorCode());
    }

    // test with invalid private key
    privateKeyLocation = getFullPathFileInResource("invalid_private_key.pem");
    uri = parameters.get("uri") + "/?private_key_file=" + privateKeyLocation;
    try (Connection connection = DriverManager.getConnection(uri, properties)) {
      fail();
    } catch (SQLException e) {
      assertEquals(
          (int) ErrorCode.INVALID_OR_UNSUPPORTED_PRIVATE_KEY.getMessageCode(), e.getErrorCode());
    }

    // clean up
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("use role accountadmin");
      statement.execute(String.format("alter user %s unset rsa_public_key", testUser));
    }
  }

  // This will only work with JDBC driver versions higher than 3.15.1
  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testPrivateKeyInConnectionStringWithBouncyCastle() throws SQLException, IOException {
    System.setProperty(SecurityUtil.ENABLE_BOUNCYCASTLE_PROVIDER_JVM, "true");
    testPrivateKeyInConnectionString();
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testBasicDataSourceSerialization() throws Exception {
    // test with username/password authentication
    // set up DataSource object and ensure connection works
    Map<String, String> params = getConnectionParameters();
    SnowflakeBasicDataSource ds = new SnowflakeBasicDataSource();
    SnowflakeBasicDataSource ds2 = null;
    ds.setServerName(params.get("host"));
    ds.setSsl("on".equals(params.get("ssl")));
    ds.setAccount(params.get("account"));
    ds.setPortNumber(Integer.parseInt(params.get("port")));
    ds.setUser(params.get("user"));
    ds.setPassword(params.get("password"));
    try (Connection con = ds.getConnection();
        Statement statement = con.createStatement();
        ResultSet resultSet = statement.executeQuery("select 1")) {
      resultSet.next();
      assertThat("select 1", resultSet.getInt(1), equalTo(1));
      con.close();
      File serializedFile = tmpFolder.newFile("serializedStuff.ser");
      // serialize datasource object into a file
      try (FileOutputStream outputFile = new FileOutputStream(serializedFile);
          ObjectOutputStream out = new ObjectOutputStream(outputFile)) {
        out.writeObject(ds);
      }
      // deserialize into datasource object again
      try (FileInputStream inputFile = new FileInputStream(serializedFile);
          ObjectInputStream in = new ObjectInputStream(inputFile)) {
        ds2 = (SnowflakeBasicDataSource) in.readObject();
      }
    }

    // test connection a second time
    try (Connection con = ds2.getConnection();
        Statement statement = con.createStatement();
        ResultSet resultSet = statement.executeQuery("select 1")) {
      resultSet.next();
      assertThat("select 1", resultSet.getInt(1), equalTo(1));
    }
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
    String queryID = null;
    String[] queryIDs = null;
    try (Connection connection = getConnection();
        Connection connection2 = getConnection();
        Statement statement = connection.createStatement()) {
      String query0 = "create or replace temporary table test_multi (cola int);";
      String query1 = "insert into test_multi VALUES (111), (222);";
      String query2 = "select cola from test_multi order by cola asc";

      // Get ResultSet for first statement
      try (ResultSet rs = statement.unwrap(SnowflakeStatement.class).executeAsyncQuery(query0)) {
        queryID = rs.unwrap(SnowflakeResultSet.class).getQueryID();
        waitForAsyncQueryDone(connection, queryID);
        queryIDs = connection.unwrap(SnowflakeConnectionV1.class).getChildQueryIds(queryID);
        assertEquals(queryIDs.length, 1);
      }

      try (ResultSet rs =
          connection.unwrap(SnowflakeConnection.class).createResultSet(queryIDs[0])) {
        assertTrue(rs.next());
        assertEquals(rs.getString(1), "Table TEST_MULTI successfully created.");
        assertFalse(rs.next());
      }

      // Get ResultSet for second statement in another connection
      try (ResultSet rs = statement.unwrap(SnowflakeStatement.class).executeAsyncQuery(query1)) {
        queryID = rs.unwrap(SnowflakeResultSet.class).getQueryID();
        waitForAsyncQueryDone(connection2, queryID);
        queryIDs = connection2.unwrap(SnowflakeConnectionV1.class).getChildQueryIds(queryID);
        assertEquals(queryIDs.length, 1);
      }

      try (ResultSet rs =
          connection2.unwrap(SnowflakeConnection.class).createResultSet(queryIDs[0])) {
        assertTrue(rs.next());
        assertEquals(rs.getInt(1), 2); // insert 2 rows
        assertFalse(rs.next());
      }

      // Get ResultSet for third statement in another connection
      try (ResultSet rs = statement.unwrap(SnowflakeStatement.class).executeAsyncQuery(query2)) {
        queryID = rs.unwrap(SnowflakeResultSet.class).getQueryID();
        waitForAsyncQueryDone(connection2, queryID);
        queryIDs = connection2.unwrap(SnowflakeConnectionV1.class).getChildQueryIds(queryID);
        assertEquals(queryIDs.length, 1);
      }

      try (ResultSet rs =
          connection2.unwrap(SnowflakeConnection.class).createResultSet(queryIDs[0])) {
        assertTrue(rs.next());
        assertEquals(rs.getInt(1), 111);
        assertTrue(rs.next());
        assertEquals(rs.getInt(1), 222);
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void testGetChildQueryIdsForAsyncMultiStatement() throws Exception {
    String queryID = null;
    String[] queryIDs = null;
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      String multiStmtQuery =
          "create or replace temporary table test_multi (cola int);"
              + "insert into test_multi VALUES (111), (222);"
              + "select cola from test_multi order by cola asc";

      statement.unwrap(SnowflakeStatement.class).setParameter("MULTI_STATEMENT_COUNT", 3);
      try (ResultSet rs =
          statement.unwrap(SnowflakeStatement.class).executeAsyncQuery(multiStmtQuery)) {
        queryID = rs.unwrap(SnowflakeResultSet.class).getQueryID();
      }
    }
    // Get the child query IDs in a new connection
    try (Connection connection = getConnection()) {
      waitForAsyncQueryDone(connection, queryID);
      queryIDs = connection.unwrap(SnowflakeConnectionV1.class).getChildQueryIds(queryID);
      assertEquals(queryIDs.length, 3);

      // First statement ResultSet
      try (ResultSet rs =
          connection.unwrap(SnowflakeConnection.class).createResultSet(queryIDs[0])) {
        assertTrue(rs.next());
        assertEquals(rs.getString(1), "Table TEST_MULTI successfully created.");
        assertFalse(rs.next());
      }

      // Second statement ResultSet
      try (ResultSet rs =
          connection.unwrap(SnowflakeConnection.class).createResultSet(queryIDs[1])) {
        assertTrue(rs.next());
        assertEquals(rs.getInt(1), 2);
        assertFalse(rs.next());
      }

      // Third statement ResultSet
      try (ResultSet rs =
          connection.unwrap(SnowflakeConnection.class).createResultSet(queryIDs[2])) {
        assertTrue(rs.next());
        assertEquals(rs.getInt(1), 111);
        assertTrue(rs.next());
        assertEquals(rs.getInt(1), 222);
        assertFalse(rs.next());
      }
    }
  }

  @Test
  public void testGetChildQueryIdsNegativeTestQueryIsRunning() throws Exception {
    String queryID = null;
    try (Connection connection = getConnection()) {
      try (Statement statement = connection.createStatement()) {
        String multiStmtQuery = "select 1; call system$wait(10); select 2";

        statement.unwrap(SnowflakeStatement.class).setParameter("MULTI_STATEMENT_COUNT", 3);
        try (ResultSet rs =
            statement.unwrap(SnowflakeStatement.class).executeAsyncQuery(multiStmtQuery)) {
          queryID = rs.unwrap(SnowflakeResultSet.class).getQueryID();
        }
        try {
          connection.unwrap(SnowflakeConnectionV1.class).getChildQueryIds(queryID);
          fail("The getChildQueryIds() should fail because query is running");
        } catch (SQLException ex) {
          String msg = ex.getMessage();
          if (!msg.contains("Status of query associated with resultSet is")
              || !msg.contains("Results not generated.")) {
            ex.printStackTrace();
            QueryStatus qs =
                connection
                    .unwrap(SnowflakeConnectionV1.class)
                    .getSfSession()
                    .getQueryStatus(queryID);
            fail("Don't get expected message, query Status: " + qs + " actual message is: " + msg);
          }
        }
      } finally {
        try (Statement statement = connection.createStatement()) {
          statement.execute("select system$cancel_query('" + queryID + "')");
        }
      }
    }
  }

  @Test
  public void testGetChildQueryIdsNegativeTestQueryFailed() throws Exception {
    String queryID = null;
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      String multiStmtQuery = "select 1; select to_date('not_date'); select 2";

      statement.unwrap(SnowflakeStatement.class).setParameter("MULTI_STATEMENT_COUNT", 3);
      try (ResultSet rs =
          statement.unwrap(SnowflakeStatement.class).executeAsyncQuery(multiStmtQuery)) {
        queryID = rs.unwrap(SnowflakeResultSet.class).getQueryID();
      }
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
      }
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
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {

      connection.setReadOnly(true);
      assertEquals(connection.isReadOnly(), false);

      connection.setReadOnly(false);
      try {
        statement.execute("create or replace table readonly_test(c1 int)");
        assertFalse(connection.isReadOnly());
      } finally {
        statement.execute("drop table if exists readonly_test");
      }
    }
  }

  /**
   * Test case for the method testDownloadStreamWithFileNotFoundException. This test verifies that a
   * SQLException is thrown when attempting to download a file that does not exist. It verifies that
   * the error code is ErrorCode.S3_OPERATION_ERROR so only runs on AWS.
   */
  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningNotOnAWS.class)
  public void testDownloadStreamWithFileNotFoundException() throws SQLException {
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
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

  private Boolean isPbes2KeySupported() throws SQLException, IOException, SecurityException {

    final String privateKeyFileNameEnv = "SNOWFLAKE_PRIVATE_KEY_FILENAME";
    final String publicKeyFileNameEnv = "SNOWFLAKE_PUBLIC_KEY_FILENAME";
    final String passphraseEnv = "SNOWFLAKE_TEST_PASSPHRASE";

    String privateKeyFile = System.getenv(privateKeyFileNameEnv);
    String publicKeyFile = System.getenv(publicKeyFileNameEnv);
    String passphrase = System.getenv(passphraseEnv);

    assertNotNull(
        privateKeyFileNameEnv
            + " environment variable can't be empty. "
            + "Please provide the filename for your private key located in the resource folder",
        passphrase);

    assertNotNull(
        publicKeyFileNameEnv
            + " environment variable can't be empty. "
            + "Please provide the filename for your public key located in the resource folder",
        passphrase);

    assertNotNull(
        passphraseEnv + " environment variable is required to decrypt private key.", passphrase);
    Map<String, String> parameters = getConnectionParameters();
    String testUser = parameters.get("user");
    Properties properties = new Properties();
    properties.put("account", parameters.get("account"));
    properties.put("user", testUser);
    properties.put("ssl", parameters.get("ssl"));
    properties.put("port", parameters.get("port"));
    Connection connection = getConnection();
    Statement statement = connection.createStatement();
    statement.execute("use role accountadmin");
    String pathFile = getFullPathFileInResource(publicKeyFile);
    String pubKey = new String(Files.readAllBytes(Paths.get(pathFile)));
    pubKey = pubKey.replace("-----BEGIN PUBLIC KEY-----", "");
    pubKey = pubKey.replace("-----END PUBLIC KEY-----", "");
    pubKey = pubKey.replace("\n", "");
    statement.execute(String.format("alter user %s set rsa_public_key='%s'", testUser, pubKey));
    connection.close();
    String privateKeyLocation = getFullPathFileInResource(privateKeyFile);
    String uri =
        parameters.get("uri")
            + "/?private_key_file_pwd="
            + passphrase
            + "&private_key_file="
            + privateKeyLocation;
    try (Connection conn = DriverManager.getConnection(uri, properties)) {
    } catch (SQLException e) {
      return false;
    }

    // clean up
    connection = getConnection();
    statement = connection.createStatement();
    statement.execute("use role accountadmin");
    statement.execute(String.format("alter user %s unset rsa_public_key", testUser));
    connection.close();
    return true;
  }

  /**
   * Added in > 3.15.1. This test is ignored and won't run until you explicitly uncomment the Ignore
   * annotation. You can only run this test locally, i.e.: mvn -DjenkinsIT
   * -DtestCategory=net.snowflake.client.category.TestCategoryConnection verify
   *
   * <p>In order to run the test successfully, you need to first do the following: 1.) Generate a
   * private and public key using OpenSSL v3: openssl genrsa 2048 | openssl pkcs8 -topk8 -v2 aes256
   * -inform PEM -out rsa-key-aes256.p8 openssl rsa -in rsa-key-aes256.p8 -pubout -out
   * rsa-key-aes256.pub 2.) Export the following environment variables: export
   * SNOWFLAKE_TEST_PASSPHRASE=your_key's_passphrase export
   * SNOWFLAKE_PRIVATE_KEY_FILENAME=rsa-key-aes256.p8 export
   * SNOWFLAKE_PUBLIC_KEY_FILENAME=rsa-key-aes256.pub 3.) Move the private and public keys to the
   * src/test/resources folder 4.) After the test is complete, remove the files from the resources
   * folder
   *
   * <p>We're testing if we can decrypt a private key generated using OpenSSL v3. This was failing
   * with an invalid private key error before SNOW-896618 added the ability to decrypt private keys
   * with the JVM argument {@value
   * net.snowflake.client.core.SecurityUtil#ENABLE_BOUNCYCASTLE_PROVIDER_JVM}
   *
   * @throws SQLException
   * @throws IOException
   */
  @Test
  @Ignore
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testPbes2Support() throws SQLException, IOException {
    System.clearProperty(SecurityUtil.ENABLE_BOUNCYCASTLE_PROVIDER_JVM);
    boolean pbes2Supported = isPbes2KeySupported();

    // The expectation is that this is going to fail (meaning the test passes when pbes2Supported is
    // false) when we use the default JDK security providers without Bouncy Castle added.
    // If the time comes when the JDK's default providers finally support PBES2 then this test will
    // fail letting us know that we don't need the Bouncy Castle dependency anymore for private key
    // decryption.
    String failureMessage =
        "The failure means that the JDK version can decrypt a private key generated by OpenSSL v3 and "
            + "BouncyCastle shouldn't be needed anymore";
    assertFalse(failureMessage, pbes2Supported);

    // The expectation is that this is going to pass once we add Bouncy Castle in the list of
    // providers
    System.setProperty(SecurityUtil.ENABLE_BOUNCYCASTLE_PROVIDER_JVM, "true");
    pbes2Supported = isPbes2KeySupported();
    failureMessage =
        "Bouncy Castle Provider should have been loaded with the -D"
            + SecurityUtil.ENABLE_BOUNCYCASTLE_PROVIDER_JVM
            + "JVM argument and this should have decrypted the private key generated by OpenSSL v3";
    assertTrue(failureMessage, pbes2Supported);
  }

  // Test for regenerating okta one-time token for versions > 3.15.1
  @Test
  @Ignore
  public void testDataSourceOktaGenerates429StatusCode() throws Exception {
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
    ds.setAuthenticator("<okta address>");
    Runnable r =
        () -> {
          try {
            ds.getConnection();
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        };
    List<Thread> threadList = new ArrayList<>();
    for (int i = 0;
        i < 30;
        ++i) { // https://docs.snowflake.com/en/user-guide/admin-security-fed-auth-use#http-429-errors
      threadList.add(new Thread(r));
    }
    threadList.forEach(Thread::start);
    for (Thread thread : threadList) {
      thread.join();
    }
  }

  /** Test added in JDBC driver version > 3.16.1 */
  @Test
  public void testDataSourceSetters() {
    Map<String, String> params = getConnectionParameters();
    SnowflakeBasicDataSource ds = new SnowflakeBasicDataSource();

    ds.setTracing("all");
    ds.setApplication("application_name");
    ds.setAccount(params.get("account"));
    ds.setAuthenticator("snowflake");
    ds.setArrowTreatDecimalAsInt(true);
    ds.setAllowUnderscoresInHost(true);
    ds.setClientConfigFile("/some/path/file.json");
    ds.setDisableGcsDefaultCredentials(false);
    ds.setDisableSamlURLCheck(false);
    ds.setDisableSocksProxy(false);
    ds.setEnablePatternSearch(true);
    ds.setDatabaseName("DB_NAME");
    ds.setEnablePutGet(false);
    ds.setMaxHttpRetries(5);
    ds.setNetworkTimeout(10);
    ds.setOcspFailOpen(false);
    ds.setProxyHost("proxyHost.com");
    ds.setProxyPort(8080);
    ds.setProxyProtocol("http");
    ds.setProxyUser("proxyUser");
    ds.setProxyPassword("proxyPassword");
    ds.setPutGetMaxRetries(3);
    ds.setStringsQuotedForColumnDef(true);
    ds.setEnableDiagnostics(true);
    ds.setDiagnosticsAllowlistFile("/some/path/allowlist.json");

    Properties props = ds.getProperties();
    assertEquals(params.get("account"), props.get("account"));
    assertEquals("snowflake", props.get("authenticator"));
    assertEquals("all", props.get("tracing"));
    assertEquals("application_name", props.get(SFSessionProperty.APPLICATION.getPropertyKey()));
    assertEquals("snowflake", props.get(SFSessionProperty.AUTHENTICATOR.getPropertyKey()));
    assertEquals(
        "true", props.get(SFSessionProperty.JDBC_ARROW_TREAT_DECIMAL_AS_INT.getPropertyKey()));
    assertEquals("true", props.get(SFSessionProperty.ALLOW_UNDERSCORES_IN_HOST.getPropertyKey()));
    assertEquals(
        "/some/path/file.json", props.get(SFSessionProperty.CLIENT_CONFIG_FILE.getPropertyKey()));
    assertEquals(
        "false", props.get(SFSessionProperty.DISABLE_GCS_DEFAULT_CREDENTIALS.getPropertyKey()));
    assertEquals("false", props.get(SFSessionProperty.DISABLE_SAML_URL_CHECK.getPropertyKey()));
    assertEquals("false", props.get(SFSessionProperty.DISABLE_SOCKS_PROXY.getPropertyKey()));
    assertEquals("true", props.get(SFSessionProperty.ENABLE_PATTERN_SEARCH.getPropertyKey()));
    assertEquals("DB_NAME", props.get(SFSessionProperty.DATABASE.getPropertyKey()));
    assertEquals("false", props.get(SFSessionProperty.ENABLE_PUT_GET.getPropertyKey()));
    assertEquals("5", props.get(SFSessionProperty.MAX_HTTP_RETRIES.getPropertyKey()));
    assertEquals("10", props.get(SFSessionProperty.NETWORK_TIMEOUT.getPropertyKey()));
    assertEquals("false", props.get(SFSessionProperty.OCSP_FAIL_OPEN.getPropertyKey()));
    assertEquals("proxyHost.com", props.get(SFSessionProperty.PROXY_HOST.getPropertyKey()));
    assertEquals("8080", props.get(SFSessionProperty.PROXY_PORT.getPropertyKey()));
    assertEquals("http", props.get(SFSessionProperty.PROXY_PROTOCOL.getPropertyKey()));
    assertEquals("proxyUser", props.get(SFSessionProperty.PROXY_USER.getPropertyKey()));
    assertEquals("proxyPassword", props.get(SFSessionProperty.PROXY_PASSWORD.getPropertyKey()));
    assertEquals("3", props.get(SFSessionProperty.PUT_GET_MAX_RETRIES.getPropertyKey()));
    assertEquals("true", props.get(SFSessionProperty.STRINGS_QUOTED.getPropertyKey()));
    assertEquals("true", props.get(SFSessionProperty.ENABLE_DIAGNOSTICS.getPropertyKey()));
    assertEquals(
        "/some/path/allowlist.json",
        props.get(SFSessionProperty.DIAGNOSTICS_ALLOWLIST_FILE.getPropertyKey()));

    ds.setOauthToken("a_token");
    assertEquals("OAUTH", props.get(SFSessionProperty.AUTHENTICATOR.getPropertyKey()));
    assertEquals("a_token", props.get(SFSessionProperty.TOKEN.getPropertyKey()));

    ds.setPasscodeInPassword(true);
    assertEquals("true", props.get(SFSessionProperty.PASSCODE_IN_PASSWORD.getPropertyKey()));
    assertEquals(
        "USERNAME_PASSWORD_MFA", props.get(SFSessionProperty.AUTHENTICATOR.getPropertyKey()));

    ds.setPrivateKeyFile("key.p8", "pwd");
    assertEquals("key.p8", props.get(SFSessionProperty.PRIVATE_KEY_FILE.getPropertyKey()));
    assertEquals("pwd", props.get(SFSessionProperty.PRIVATE_KEY_FILE_PWD.getPropertyKey()));
    assertEquals("SNOWFLAKE_JWT", props.get(SFSessionProperty.AUTHENTICATOR.getPropertyKey()));

    ds.setPasscodeInPassword(false);
    ds.setPasscode("a_passcode");
    assertEquals("false", props.get(SFSessionProperty.PASSCODE_IN_PASSWORD.getPropertyKey()));
    assertEquals(
        "USERNAME_PASSWORD_MFA", props.get(SFSessionProperty.AUTHENTICATOR.getPropertyKey()));
    assertEquals("a_passcode", props.get(SFSessionProperty.PASSCODE.getPropertyKey()));
  }
  /**
   * SNOW-1465374: For TIMESTAMP_LTZ we were returning timestamps without timezone when scale was
   * set e.g. to 6 in Arrow format The problem wasn't visible when calling getString, but was
   * visible when we called toString on passed getTimestamp since we returned {@link
   * java.sql.Timestamp}, not {@link SnowflakeTimestampWithTimezone}
   *
   * <p>Timestamps before 1582-10-05 are always returned as {@link java.sql.Timestamp}, not {@link
   * SnowflakeTimestampWithTimezone} {SnowflakeTimestampWithTimezone}
   *
   * <p>Added in > 3.16.1
   */
  @Test
  public void shouldGetDifferentTimestampLtzConsistentBetweenFormats() throws Exception {
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      statement.executeUpdate(
          "create or replace table DATETIMETZ_TYPE(timestamp_tzcol timestamp_ltz, timestamp_tzpcol timestamp_ltz(6), timestamptzcol timestampltz, timestampwtzcol timestamp with local time zone);");
      Arrays.asList(
              "insert into DATETIMETZ_TYPE values('9999-12-31 23:59:59.999999999','9999-12-31 23:59:59.999999','9999-12-31 23:59:59.999999999','9999-12-31 23:59:59.999999999');",
              "insert into DATETIMETZ_TYPE values('1582-01-01 00:00:00.000000001','1582-01-01 00:00:00.000001','1582-01-01 00:00:00.000000001','1582-01-01 00:00:00.000000001');",
              "insert into DATETIMETZ_TYPE values('2000-06-18 18:29:30.123456789 +0100','2000-06-18 18:29:30.123456 +0100','2000-06-18 18:29:30.123456789 +0100','2000-06-18 18:29:30.123456789 +0100');",
              "insert into DATETIMETZ_TYPE values(current_timestamp(),current_timestamp(),current_timestamp(),current_timestamp());",
              "insert into DATETIMETZ_TYPE values('2000-06-18 18:29:30.12345 -0530','2000-06-18 18:29:30.123 -0530','2000-06-18 18:29:30.123456 -0530','2000-06-18 18:29:30.123 -0530');",
              "insert into DATETIMETZ_TYPE values('2000-06-18 18:29:30','2000-06-18 18:29:30','2000-06-18 18:29:30','2000-06-18 18:29:30');",
              "insert into DATETIMETZ_TYPE values('1582-10-04 00:00:00.000000001','1582-10-04 00:00:00.000001','1582-10-04 00:00:00.000000001','1582-10-04 00:00:00.000000001');",
              "insert into DATETIMETZ_TYPE values('1582-10-05 00:00:00.000000001','1582-10-05 00:00:00.000001','1582-10-05 00:00:00.000000001','1582-10-05 00:00:00.000000001');",
              "insert into DATETIMETZ_TYPE values('1583-10-05 00:00:00.000000001','1583-10-05 00:00:00.000001','1583-10-05 00:00:00.000000001','1583-10-05 00:00:00.000000001');")
          .forEach(
              insert -> {
                try {
                  statement.executeUpdate(insert);
                } catch (SQLException e) {
                  throw new RuntimeException(e);
                }
              });
      try (ResultSet arrowResultSet = statement.executeQuery("select * from DATETIMETZ_TYPE")) {
        try (Connection jsonConnection = getConnection();
            Statement jsonStatement = jsonConnection.createStatement()) {
          jsonStatement.execute("alter session set JDBC_QUERY_RESULT_FORMAT=JSON");
          try (ResultSet jsonResultSet =
              jsonStatement.executeQuery("select * from DATETIMETZ_TYPE")) {
            int rowIdx = 0;
            while (arrowResultSet.next()) {
              logger.debug("Checking row " + rowIdx);
              assertTrue(jsonResultSet.next());
              for (int column = 1; column <= 4; ++column) {
                logger.trace(
                    "JSON row[{}],column[{}] as string '{}', timestamp string '{}', as timestamp numeric '{}', tz offset={}, timestamp class {}",
                    rowIdx,
                    column,
                    jsonResultSet.getString(column),
                    jsonResultSet.getTimestamp(column),
                    jsonResultSet.getTimestamp(column).getTime(),
                    jsonResultSet.getTimestamp(column).getTimezoneOffset(),
                    jsonResultSet.getTimestamp(column).getClass());
                logger.trace(
                    "ARROW row[{}],column[{}] as string '{}', timestamp string '{}', as timestamp numeric '{}', tz offset={}, timestamp class {}",
                    rowIdx,
                    column,
                    arrowResultSet.getString(column),
                    arrowResultSet.getTimestamp(column),
                    arrowResultSet.getTimestamp(column).getTime(),
                    arrowResultSet.getTimestamp(column).getTimezoneOffset(),
                    arrowResultSet.getTimestamp(column).getClass());
                assertEquals(
                    "Expecting that string representation are the same for row "
                        + rowIdx
                        + " and column "
                        + column,
                    jsonResultSet.getString(column),
                    arrowResultSet.getString(column));
                assertEquals(
                    "Expecting that string representation (via toString) are the same for row "
                        + rowIdx
                        + " and column "
                        + column,
                    jsonResultSet.getTimestamp(column).toString(),
                    arrowResultSet.getTimestamp(column).toString());
                assertEquals(
                    "Expecting that timestamps are the same for row "
                        + rowIdx
                        + " and column "
                        + column,
                    jsonResultSet.getTimestamp(column),
                    arrowResultSet.getTimestamp(column));
              }
              rowIdx++;
            }
          }
        }
      }
    }
  }
}
