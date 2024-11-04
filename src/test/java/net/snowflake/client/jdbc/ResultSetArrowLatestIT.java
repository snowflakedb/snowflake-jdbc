/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import net.snowflake.client.category.TestCategoryArrow;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * ResultSet integration tests for the latest JDBC driver. This doesn't work for the oldest
 * supported driver. Drop this file when ResultSetLatestIT is dropped.
 */
@Category(TestCategoryArrow.class)
public class ResultSetArrowLatestIT extends ResultSetLatestIT {
  public ResultSetArrowLatestIT() {
    super("arrow");
  }

  // TODO move to parent - for now we only want to test ARROW
  @Test
  public void testChunkedResultSetHasExpectedNumberOfRows() throws SQLException {
    int rowsToGet = 8295305;
    int count = 0;
    int seq8 = -1;
    Map<Integer, Integer> seq8Usages = new HashMap<>();
    try (Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(
                "select randstr(abs(mod(random(), 20)) + 1 ,random()), seq8() as s from table(generator(rowcount=>"
                    + rowsToGet
                    + ")) where 1=1 and s > -1 "
                    + "order by s")) {
      while (resultSet.next()) {
        int newSeq8 = resultSet.getInt(2);
        //        System.out.println(newSeq8 + ": " + resultSet.getString(1));
        count++;
        assertTrue(seq8 < newSeq8);
        seq8 = newSeq8;
        seq8Usages.put(newSeq8, seq8Usages.getOrDefault(newSeq8, 0));
      }
      assertEquals(rowsToGet, count);
      assertEquals(rowsToGet, seq8Usages.size());
    }
  }
}
