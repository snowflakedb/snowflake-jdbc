package net.snowflake.client.internal.jdbc;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Properties;
import net.snowflake.client.AbstractDriverIT;
import net.snowflake.client.annotations.DontRunOnGithubActions;
import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Integration test for HeartbeatRegistry with multiple sessions having different heartbeat
 * intervals.
 *
 * <p>This test verifies that the bug fix works: sessions with short intervals are not expired when
 * sessions with long intervals are added.
 */
@Tag(TestTags.OTHERS)
public class HeartbeatMultiSessionIT extends AbstractDriverIT {

  /**
   * Set up test environment with short token validity.
   *
   * <p>Sets master token validity to 60 seconds and session token validity to 20 seconds to make
   * tests run faster.
   */
  private void setupShortTokenValidity() throws Exception {
    try (Connection connection = getSnowflakeAdminConnection()) {
      try (Statement statement = connection.createStatement()) {
        statement.execute(
            "alter system set"
                + " master_token_validity=60"
                + ",session_token_validity=20"
                + ",SESSION_RECORD_ACCESS_INTERVAL_SECS=1");
      }
    }
  }

  /** Reset master_token_validity, session_token_validity to default. */
  private void restoreDefaultTokenValidity() throws Exception {
    try (Connection connection = getSnowflakeAdminConnection()) {
      try (Statement statement = connection.createStatement()) {
        statement.execute(
            "alter system set"
                + " master_token_validity=default"
                + ",session_token_validity=default"
                + ",SESSION_RECORD_ACCESS_INTERVAL_SECS=default");
      }
    }
  }

  /**
   * Test that multiple sessions with different heartbeat intervals work independently.
   *
   * <p>This tests the critical bug fix: a session with short heartbeat interval should not expire
   * when a session with long heartbeat interval is added.
   */
  @Test
  @DontRunOnGithubActions
  public void testMultipleSessionsDifferentHeartbeatIntervals() throws Exception {
    try {
      setupShortTokenValidity();
      // Connection 1: Short heartbeat (5 seconds)
      Properties props1 = new Properties();
      props1.put("CLIENT_SESSION_KEEP_ALIVE", "true");
      props1.put("CLIENT_SESSION_KEEP_ALIVE_HEARTBEAT_FREQUENCY", "5");

      // Connection 2: Long heartbeat (30 seconds)
      Properties props2 = new Properties();
      props2.put("CLIENT_SESSION_KEEP_ALIVE", "true");
      props2.put("CLIENT_SESSION_KEEP_ALIVE_HEARTBEAT_FREQUENCY", "30");

      try (Connection conn1 = getConnection("s3testaccount", props1);
          Connection conn2 = getConnection("s3testaccount", props2)) {

        // Verify both connections are initially valid
        assertTrue(conn1.isValid(5), "Connection 1 should be valid initially");
        assertTrue(conn2.isValid(5), "Connection 2 should be valid initially");

        // Sleep 65 seconds (past master token validity of 60s)
        Thread.sleep(65000);

        // Both connections should still be valid because:
        // - Connection 1 got heartbeated every 5 seconds
        // - Connection 2 got heartbeated every 30 seconds
        // The bug would have caused connection 1 to expire because its
        // heartbeat would have been delayed to 30 seconds.
        assertTrue(conn1.isValid(5), "Connection 1 should still be valid after 65 seconds");
        assertTrue(conn2.isValid(5), "Connection 2 should still be valid after 65 seconds");
      }
    } finally {
      restoreDefaultTokenValidity();
    }
  }

  /**
   * Test that multiple sessions with the same interval work correctly.
   *
   * <p>This verifies that multiple sessions with the same heartbeat interval can coexist and remain
   * valid. Sessions with identical intervals should share a thread internally, but this is verified
   * by unit tests rather than integration tests.
   */
  @Test
  @DontRunOnGithubActions
  public void testMultipleSessionsSameInterval_ShareThread() throws Exception {
    try {
      setupShortTokenValidity();
      Properties props = new Properties();
      props.put("CLIENT_SESSION_KEEP_ALIVE", "true");
      props.put("CLIENT_SESSION_KEEP_ALIVE_HEARTBEAT_FREQUENCY", "10");

      try (Connection conn1 = getConnection("s3testaccount", props);
          Connection conn2 = getConnection("s3testaccount", props);
          Connection conn3 = getConnection("s3testaccount", props)) {

        // All connections should be valid
        assertTrue(conn1.isValid(5));
        assertTrue(conn2.isValid(5));
        assertTrue(conn3.isValid(5));

        // Sleep to let heartbeats run
        Thread.sleep(65000);

        // All should still be valid
        assertTrue(conn1.isValid(5), "Connection 1 should still be valid");
        assertTrue(conn2.isValid(5), "Connection 2 should still be valid");
        assertTrue(conn3.isValid(5), "Connection 3 should still be valid");
      }
    } finally {
      restoreDefaultTokenValidity();
    }
  }
}
