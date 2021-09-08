package net.snowflake.client.jdbc;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.logging.Logger;
import net.snowflake.client.ConditionalIgnoreRule;
import net.snowflake.client.RunningOnGithubAction;
import net.snowflake.client.core.QueryStatus;
import org.junit.Test;

/**
 * Test class for using heartbeat with asynchronous querying. This is a "Latest" class because old
 * driver versions do not contain the asynchronous querying API.
 */
public class HeartbeatAsyncLatestIT extends HeartbeatIT {
  private static Logger logger = Logger.getLogger(HeartbeatAsyncLatestIT.class.getName());

  /**
   * create a new connection with or without keep alive parameter and submit an asynchronous query.
   * The async query will not fail if the session token expires, but the functions fetching the
   * query results require a valid session token.
   *
   * @param useKeepAliveSession Enables/disables client session keep alive
   * @param queryIdx The query index
   * @throws SQLException Will be thrown if any of the driver calls fail
   */
  @Override
  protected void submitQuery(boolean useKeepAliveSession, int queryIdx)
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
      // Query will take 5 seconds to run, but ResultSet will be returned immediately
      resultSet =
          stmt.unwrap(SnowflakeStatement.class)
              .executeAsyncQuery("SELECT count(*) FROM TABLE(generator(timeLimit => 5))");
      Thread.sleep(61000); // sleep 61 seconds to await original session expiration time
      QueryStatus qs = resultSet.unwrap(SnowflakeResultSet.class).getStatus();
      int retry = 0;
      int MAX_RETRY = 20;
      // Ensure query succeeded. Avoid flaky test failure by waiting until query is complete to
      // assert the query status is a success.
      while (QueryStatus.isStillRunning(qs) && retry < MAX_RETRY) {
        Thread.sleep(3000);
        retry++;
        qs = resultSet.unwrap(SnowflakeResultSet.class).getStatus();
      }
      // Query should succeed eventually. Assert this is the case.
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

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testAsynchronousQuerySuccess() throws Exception {
    testSuccess();
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testAsynchronousQueryFailure() throws Exception {
    testFailure();
  }

  /** Test that isValid() function returns false when session is expired */
  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testIsValidWithInvalidSession() throws Exception {
    Connection connection = null;
    try {
      connection = getConnection();
      // assert that connection starts out valid
      assertTrue(connection.isValid(5));
      Thread.sleep(61000); // sleep 61 seconds to await session expiration time
      // assert that connection is no longer valid after session has expired
      assertFalse(connection.isValid(5));
    } finally {
      connection.close();
    }
  }
}
