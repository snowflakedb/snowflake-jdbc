package net.snowflake.client.jdbc;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Properties;
import java.util.logging.Logger;
import net.snowflake.client.annotations.DontRunOnGithubActions;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.core.QueryStatus;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Test class for using heartbeat with asynchronous querying. This is a "Latest" class because old
 * driver versions do not contain the asynchronous querying API.
 */
@Tag(TestTags.OTHERS)
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
    Properties sessionParams = new Properties();
    sessionParams.put(
        "CLIENT_SESSION_KEEP_ALIVE",
        useKeepAliveSession ? Boolean.TRUE.toString() : Boolean.FALSE.toString());

    try (Connection connection = getConnection("s3testaccount", sessionParams);
        Statement stmt = connection.createStatement();
        // Query will take 5 seconds to run, but ResultSet will be returned immediately
        ResultSet resultSet =
            stmt.unwrap(SnowflakeStatement.class)
                .executeAsyncQuery("SELECT count(*) FROM TABLE(generator(timeLimit => 5))")) {
      Thread.sleep(61000); // sleep 61 seconds to await original session expiration time
      QueryStatus qs = resultSet.unwrap(SnowflakeResultSet.class).getStatus();
      // Ensure query succeeded. Avoid flaky test failure by waiting until query is complete to
      // assert the query status is a success.
      SnowflakeResultSet rs = resultSet.unwrap(SnowflakeResultSet.class);
      await()
          .atMost(Duration.ofSeconds(60))
          .until(() -> !QueryStatus.isStillRunning(rs.getStatus()));
      // Query should succeed eventually. Assert this is the case.
      assertEquals(QueryStatus.SUCCESS, qs);

      // assert we get 1 row
      assertTrue(resultSet.next());
      assertFalse(resultSet.next());
      logger.fine("Query " + queryIdx + " passed ");
    }
  }

  /** Test that isValid() function returns false when session is expired */
  @Test
  @DontRunOnGithubActions
  public void testIsValidWithInvalidSession() throws Exception {
    try (Connection connection = getConnection("s3testaccount")) {
      // assert that connection starts out valid
      assertTrue(connection.isValid(5));
      Thread.sleep(61000); // sleep 61 seconds to await session expiration time
      // assert that connection is no longer valid after session has expired
      assertFalse(connection.isValid(5));
    }
  }
}
