package net.snowflake.client.jdbc;

import static net.snowflake.client.AssumptionUtils.isRunningOnGithubActions;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Logger;
import net.snowflake.client.AbstractDriverIT;
import net.snowflake.client.annotations.DontRunOnGithubActions;
import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** This test assumes that GS has been set up */
@Tag(TestTags.OTHERS)
public class HeartbeatIT extends AbstractDriverIT {
  private static Logger logger = Logger.getLogger(HeartbeatIT.class.getName());

  /**
   * set up
   *
   * <p>change the master token validity to 10 seconds change the session token validity to 5
   * seconds change the SESSION_RECORD_ACCESS_INTERVAL_SECS to 1 second
   */
  @BeforeAll
  public static void setUpClass() throws Exception {
    if (!isRunningOnGithubActions()) {
      try (Connection connection = getSnowflakeAdminConnection();
          Statement statement = connection.createStatement()) {
        statement.execute(
            "alter system set"
                + " master_token_validity=60"
                + ",session_token_validity=20"
                + ",SESSION_RECORD_ACCESS_INTERVAL_SECS=1");
      }
    }
  }

  /**
   * Reset master_token_validity, session_token_validity, SESSION_RECORD_ACCESS_INTERVAL_SECS to
   * default.
   */
  @AfterAll
  public static void tearDownClass() throws Exception {
    if (!isRunningOnGithubActions()) {
      try (Connection connection = getSnowflakeAdminConnection();
          Statement statement = connection.createStatement()) {
        statement.execute(
            "alter system set"
                + " master_token_validity=default"
                + ",session_token_validity=default"
                + ",SESSION_RECORD_ACCESS_INTERVAL_SECS=default");
      }
    }
  }

  /**
   * create a new connection with or without keep alive session and submit a query that will take
   * longer than the master token validity.
   *
   * @param useKeepAliveSession Enables/disables client session keep alive
   * @param queryIdx The query index
   * @throws SQLException Will be thrown if any of the driver calls fail
   */
  protected void submitQuery(boolean useKeepAliveSession, int queryIdx)
      throws SQLException, InterruptedException {
    ResultSetMetaData resultSetMetaData;

    Properties sessionParams = new Properties();
    sessionParams.put(
        "CLIENT_SESSION_KEEP_ALIVE",
        useKeepAliveSession ? Boolean.TRUE.toString() : Boolean.FALSE.toString());

    try (Connection connection = getConnection("s3testaccount", sessionParams);
        Statement statement = connection.createStatement()) {

      Thread.sleep(61000); // sleep 61 seconds
      try (ResultSet resultSet = statement.executeQuery("SELECT 1")) {
        resultSetMetaData = resultSet.getMetaData();

        // assert column count
        assertEquals(1, resultSetMetaData.getColumnCount());

        // assert we get 1 row
        assertTrue(resultSet.next());

        logger.fine("Query " + queryIdx + " passed ");
      }
    }
  }

  /**
   * Test heartbeat by starting 10 threads. Each get a connection and wait for a time longer than
   * master token validity and issue a query to make sure the query succeeds.
   */
  @Test
  @DontRunOnGithubActions
  public void testSuccess() throws Exception {
    int concurrency = 10;
    ExecutorService executorService = Executors.newFixedThreadPool(10);
    List<Future<?>> futures = new ArrayList<>();
    // create 10 threads, each open a connection and submit a query
    // after sleeping 15 seconds
    for (int idx = 0; idx < concurrency; idx++) {
      logger.fine("open a new connection and submit query " + idx);
      final int queryIdx = idx;
      futures.add(
          executorService.submit(
              () -> {
                try {
                  submitQuery(true, queryIdx);
                } catch (SQLException | InterruptedException e) {
                  throw new IllegalStateException("task interrupted", e);
                }
              }));
    }
    executorService.shutdown();
    for (int idx = 0; idx < concurrency; idx++) {
      futures.get(idx).get();
    }
  }

  /**
   * Test no heartbeat by starting 1 thread. It gets a connection and wait for a time longer than
   * master token validity and issue a query to make sure the query fails.
   */
  @Test
  @DontRunOnGithubActions
  public void testFailure() throws Exception {
    ExecutorService executorService = Executors.newFixedThreadPool(1);
    Future<?> future =
        executorService.submit(
            () -> {
              try {
                submitQuery(false, 0);
              } catch (SQLException e) {
                throw new RuntimeSQLException("SQLException", e);
              } catch (InterruptedException e) {
                throw new IllegalStateException("task interrupted", e);
              }
            });
    executorService.shutdown();
    ExecutionException ex = assertThrows(ExecutionException.class, future::get);
    Throwable rootCause = ex.getCause();
    assertThat("Runtime Exception", rootCause, instanceOf(RuntimeSQLException.class));

    rootCause = rootCause.getCause();

    assertThat("Root cause class", rootCause, instanceOf(SnowflakeSQLException.class));
    assertThat("Error code", ((SnowflakeSQLException) rootCause).getErrorCode(), equalTo(390114));
  }

  class RuntimeSQLException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    RuntimeSQLException(String message, SQLException e) {
      super(message, e);
    }
  }
}
