/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import net.snowflake.client.AbstractDriverIT;
import net.snowflake.client.ConditionalIgnoreRule;
import net.snowflake.client.RunningOnGithubAction;
import net.snowflake.client.category.TestCategoryOthers;
import net.snowflake.client.core.QueryStatus;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Logger;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.*;

/** This test assumes that GS has been set up */
@Category(TestCategoryOthers.class)
public class HeartbeatIT extends AbstractDriverIT {
  private static Logger logger = Logger.getLogger(HeartbeatIT.class.getName());

  /**
   * set up
   *
   * <p>change the master token validity to 10 seconds change the session token validity to 5
   * seconds change the SESSION_RECORD_ACCESS_INTERVAL_SECS to 1 second
   */
  @BeforeClass
  public static void setUpClass() throws Exception {
    if (!RunningOnGithubAction.isRunningOnGithubAction()) {
      Connection connection = getSnowflakeAdminConnection();
      connection
          .createStatement()
          .execute(
              "alter system set"
                  + " master_token_validity=60"
                  + ",session_token_validity=20"
                  + ",SESSION_RECORD_ACCESS_INTERVAL_SECS=1");
      connection.close();
    }
  }

  /**
   * Reset master_token_validity, session_token_validity, SESSION_RECORD_ACCESS_INTERVAL_SECS to
   * default.
   */
  @AfterClass
  public static void tearDownClass() throws Exception {
    if (!RunningOnGithubAction.isRunningOnGithubAction()) {
      Connection connection = getSnowflakeAdminConnection();
      connection
          .createStatement()
          .execute(
              "alter system set"
                  + " master_token_validity=default"
                  + ",session_token_validity=default"
                  + ",SESSION_RECORD_ACCESS_INTERVAL_SECS=default");
      connection.close();
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
  private void submitQuery(boolean useKeepAliveSession, int queryIdx)
      throws SQLException, InterruptedException {
    Connection connection = null;
    Statement statement = null;
    ResultSet resultSet = null;
    ResultSetMetaData resultSetMetaData;

    try {
      Properties sessionParams = new Properties();
      sessionParams.put(
          "CLIENT_SESSION_KEEP_ALIVE",
          useKeepAliveSession ? Boolean.TRUE.toString() : Boolean.FALSE.toString());

      connection = getConnection(sessionParams);
      statement = connection.createStatement();

      Thread.sleep(61000); // sleep 61 seconds
      resultSet = statement.executeQuery("SELECT 1");
      resultSetMetaData = resultSet.getMetaData();

      // assert column count
      assertEquals(1, resultSetMetaData.getColumnCount());

      // assert we get 1 row
      assertTrue(resultSet.next());

      logger.fine("Query " + queryIdx + " passed ");
      statement.close();
    } finally {
      closeSQLObjects(resultSet, statement, connection);
    }
  }

  /**
   * create a new connection with or without keep alive sessionsubmit an asynchronous query that
   * will take longer than the master token validity.
   *
   * @param useKeepAliveSession Enables/disables client session keep alive
   * @param queryIdx The query index
   * @throws SQLException Will be thrown if any of the driver calls fail
   */
  private void submitAsyncQuery(boolean useKeepAliveSession, int queryIdx)
      throws SQLException, InterruptedException {
    Connection connection = null;
    ResultSet resultSet = null;
    try {
      Properties sessionParams = new Properties();
      sessionParams.put(
          "CLIENT_SESSION_KEEP_ALIVE",
          useKeepAliveSession ? Boolean.TRUE.toString() : Boolean.FALSE.toString());

      connection = getConnection(sessionParams);

      Statement stmt = connection.createStatement();
      // Query will take 30 seconds to run, but ResultSet will be returned immediately
      resultSet =
          stmt.unwrap(SnowflakeStatement.class)
              .executeAsyncQuery("SELECT count(*) FROM TABLE(generator(timeLimit => 30))");
      Thread.sleep(61000); // sleep 61 seconds to await original session expiration time
      QueryStatus qs = resultSet.unwrap(SnowflakeResultSet.class).getStatus();
      // Ensure query succeeded
      assertEquals(QueryStatus.SUCCESS, qs);

      // assert we get 1 row
      assertTrue(resultSet.next());
      assertFalse(resultSet.next());

      logger.fine("Query " + queryIdx + " passed ");
    } finally {
      resultSet.close();
      connection.close();
    }
  }

  /**
   * Test heartbeat by starting 10 threads. Each get a connection and wait for a time longer than
   * master token validity and issue a query to make sure the query succeeds.
   */
  private void testSuccess(boolean isAsync) throws Exception {
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
                  if (isAsync) {
                    submitAsyncQuery(true, queryIdx);
                  } else {
                    submitQuery(true, queryIdx);
                  }
                } catch (SQLException | InterruptedException e) {
                  throw new IllegalStateException("task interrupted", e);
                }
              }));
    }
    executorService.shutdown();
    for (int idx = 0; idx < concurrency; idx++) futures.get(idx).get();
  }

  /**
   * Test no heartbeat by starting 1 thread. It gets a connection and wait for a time longer than
   * master token validity and issue a query to make sure the query fails.
   */
  private void testFailure(boolean isAsync) throws Exception {
    ExecutorService executorService = Executors.newFixedThreadPool(1);

    try {
      Future<?> future =
          executorService.submit(
              () -> {
                try {
                  if (isAsync) {
                    submitAsyncQuery(false, 0);
                  } else {
                    submitQuery(false, 0);
                  }
                } catch (SQLException e) {
                  throw new RuntimeSQLException("SQLException", e);
                } catch (InterruptedException e) {
                  throw new IllegalStateException("task interrupted", e);
                }
              });
      executorService.shutdown();
      future.get();
      fail("should fail and raise an exception");
    } catch (ExecutionException ex) {
      Throwable rootCause = ex.getCause();
      assertThat("Runtime Exception", rootCause, instanceOf(RuntimeSQLException.class));

      rootCause = rootCause.getCause();

      assertThat("Root cause class", rootCause, instanceOf(SnowflakeSQLException.class));
      assertThat("Error code", ((SnowflakeSQLException) rootCause).getErrorCode(), equalTo(390114));
    }
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testSynchronousQuerySuccess() throws Exception {
    testSuccess(false);
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testAsynchronousQuerySuccess() throws Exception {
    testSuccess(true);
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testSynchronousQueryFailure() throws Exception {
    testFailure(false);
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testAsynchronousQueryFailure() throws Exception {
    testFailure(true);
  }

  class RuntimeSQLException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    RuntimeSQLException(String message, SQLException e) {
      super(message, e);
    }
  }
}
