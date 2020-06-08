package net.snowflake.client.jdbc;

import net.snowflake.client.category.TestCategoryStatement;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;

/**
 * Large update test. No JSON/ARROW specific test case is required.
 */
@Category(TestCategoryStatement.class)
public class StatementLargeUpdateIT extends BaseJDBCTest
{
  @Test
  public void testLargeUpdate() throws Throwable
  {
    try (Connection con = getConnection())
    {
      long expectedUpdateRows = (long) Integer.MAX_VALUE + 10L;
      con.createStatement().execute("create or replace table test_large_update(c1 boolean)");
      Statement st = con.createStatement();
      long updatedRows = st.executeLargeUpdate(
          "insert into test_large_update select true from table(generator(rowcount=>" + expectedUpdateRows + "))");
      assertEquals(expectedUpdateRows, updatedRows);
      assertEquals(expectedUpdateRows, st.getLargeUpdateCount());
      con.createStatement().execute("drop table if exists test_large_update");
    }
  }
}
