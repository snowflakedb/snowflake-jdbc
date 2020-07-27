/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.jdbc;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.PreparedStatement;
import net.snowflake.client.category.TestCategoryStatement;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(TestCategoryStatement.class)
public class PreparedStatementLargeUpdateIt extends BaseJDBCTest {
  @Test
  public void testLargeUpdate() throws Throwable {
    try (Connection con = getConnection()) {
      long expectedUpdateRows = (long) Integer.MAX_VALUE + 10L;
      con.createStatement().execute("create or replace table test_large_update(c1 boolean)");
      PreparedStatement st =
          con.prepareStatement(
              "insert into test_large_update select true from table(generator(rowcount=>"
                  + expectedUpdateRows
                  + "))");
      long updatedRows = st.executeLargeUpdate();
      assertEquals(expectedUpdateRows, updatedRows);
      assertEquals(expectedUpdateRows, st.getLargeUpdateCount());
      con.createStatement().execute("drop table if exists test_large_update");
    }
  }
}
