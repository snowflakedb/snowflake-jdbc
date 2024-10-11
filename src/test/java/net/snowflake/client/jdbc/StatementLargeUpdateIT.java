package net.snowflake.client.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.Statement;
import org.junit.jupiter.api.Test;

/** Large update test. No JSON/ARROW specific test case is required. */
// @Category(TestCategoryStatement.class)
public class StatementLargeUpdateIT extends BaseJDBCTest {
  @Test
  public void testLargeUpdate() throws Throwable {
    try (Connection con = getConnection();
        Statement statement = con.createStatement()) {
      long expectedUpdateRows = (long) Integer.MAX_VALUE + 10L;
      try {
        statement.execute("create or replace table test_large_update(c1 boolean)");
        long updatedRows =
            statement.executeLargeUpdate(
                "insert into test_large_update select true from table(generator(rowcount=>"
                    + expectedUpdateRows
                    + "))");
        assertEquals(expectedUpdateRows, updatedRows);
        assertEquals(expectedUpdateRows, statement.getLargeUpdateCount());
      } finally {
        statement.execute("drop table if exists test_large_update");
      }
    }
  }
}
