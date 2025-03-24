package net.snowflake.client.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.spy;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import net.snowflake.client.annotations.DontRunOnGithubActions;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.core.ExecTimeTelemetryData;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@Tag(TestTags.STATEMENT)
public class PreparedStatementLargeUpdateLatestIT extends BaseJDBCTest {

  /**
   * Test that large update larger than MAX_INT returns correctly
   *
   * @throws Throwable
   */
  @Test
  @DontRunOnGithubActions
  public void testLargeUpdate() throws Throwable {
    try (Connection con = getConnection();
        Statement statement = con.createStatement()) {
      try {
        long expectedUpdateRows = (long) Integer.MAX_VALUE + 10L;
        statement.execute("create or replace table  test_large_update(c1 boolean)");
        try (PreparedStatement st =
                con.prepareStatement(
                    "insert into test_large_update select true from table(generator(rowcount=>"
                        + expectedUpdateRows
                        + "))");
            PreparedStatement spyp = spy(st)) {
          // Mock internal method which returns rowcount
          Mockito.doReturn(expectedUpdateRows)
              .when((SnowflakePreparedStatementV1) spyp)
              .executeUpdateInternal(
                  Mockito.any(String.class),
                  Mockito.any(Map.class),
                  Mockito.any(boolean.class),
                  Mockito.any(ExecTimeTelemetryData.class));
          long updatedRows = spyp.executeLargeUpdate();
          assertEquals(expectedUpdateRows, updatedRows);
        }
      } finally {
        statement.execute("drop table if exists test_large_update");
      }
    }
  }

  /**
   * Test that when a batch size larger than MAX_INT is returned, the result is returned as a long.
   *
   * @throws SQLException
   */
  @Test
  @DontRunOnGithubActions
  public void testExecuteLargeBatchOverIntMax() throws SQLException {
    try (Connection connection = getConnection();
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("create or replace table over_int_table (val string, id int)");
        try (PreparedStatement pstmt =
                connection.prepareStatement("UPDATE over_int_table SET ID=200");
            PreparedStatement spyp = spy(pstmt)) {
          long numRows = Integer.MAX_VALUE + 10L;
          // Mock internal method which returns rowcount
          Mockito.doReturn(numRows)
              .when((SnowflakePreparedStatementV1) spyp)
              .executeUpdateInternal(
                  Mockito.any(String.class),
                  Mockito.any(Map.class),
                  Mockito.any(boolean.class),
                  Mockito.any(ExecTimeTelemetryData.class));
          pstmt.addBatch();
          long[] queryResult = spyp.executeLargeBatch();
          assertEquals(1, queryResult.length);
          assertEquals(numRows, queryResult[0]);
        }
      } finally {
        statement.execute("drop table if exists over_int_table");
      }
    }
  }
}
