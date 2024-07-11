/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.jdbc;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class GCPLargeResult extends BaseJDBCTest {
  private final String queryResultFormat;

  @Parameterized.Parameters(name = "format={0}")
  public static Object[][] data() {
    return new Object[][] {{"JSON"}, {"ARROW"}};
  }

  public GCPLargeResult(String queryResultFormat) {
    this.queryResultFormat = queryResultFormat;
  }

  Connection init() throws SQLException {
    Connection conn = BaseJDBCTest.getConnection("gcpaccount");
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("alter session set jdbc_query_result_format = '" + queryResultFormat + "'");
    }
    return conn;
  }

  @Test
  public void testLargeResultSetGCP() throws Throwable {
    try (Connection con = init()) {
      PreparedStatement stmt =
          con.prepareStatement(
              "select seq8(), randstr(1000, random()) from table(generator(rowcount=>1000))");
      stmt.setMaxRows(999);
      ResultSet rset = stmt.executeQuery();
      int cnt = 0;
      while (rset.next()) {
        ++cnt;
      }
      assertEquals(cnt, 999);
    }
  }
}
