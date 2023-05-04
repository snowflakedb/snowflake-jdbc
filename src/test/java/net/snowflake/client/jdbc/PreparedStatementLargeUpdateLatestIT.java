/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.jdbc;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.spy;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import net.snowflake.client.category.TestCategoryStatement;
import net.snowflake.client.core.ExecTimeTelemetryData;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category(TestCategoryStatement.class)
public class PreparedStatementLargeUpdateLatestIT extends BaseJDBCTest {

  /**
   * Test that large update larger than MAX_INT returns correctly
   *
   * @throws Throwable
   */
  @Test
  public void testLargeUpdate() throws Throwable {
    try (Connection con = getConnection()) {
      long expectedUpdateRows = (long) Integer.MAX_VALUE + 10L;
      con.createStatement().execute("create or replace table  test_large_update(c1 boolean)");
      PreparedStatement st =
          con.prepareStatement(
              "insert into test_large_update select true from table(generator(rowcount=>"
                  + expectedUpdateRows
                  + "))");
      PreparedStatement spyp = spy(st);
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
      con.createStatement().execute("drop table if exists test_large_update");
    }
  }

  /**
   * Test that when a batch size larger than MAX_INT is returned, the result is returned as a long.
   *
   * @throws SQLException
   */
  @Test
  public void testExecuteLargeBatchOverIntMax() throws SQLException {
    Connection connection = getConnection();
    PreparedStatement pstmt = connection.prepareStatement("UPDATE over_int SET ID=200");
    PreparedStatement spyp = spy(pstmt);
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
}
