/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import net.snowflake.client.providers.SimpleFormatProvider;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

public class GCPLargeResult extends BaseJDBCTest {

  Connection init(String queryResultFormat) throws SQLException {
    Connection conn = BaseJDBCTest.getConnection("gcpaccount");
    System.out.println("Connected");
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("alter session set jdbc_query_result_format = '" + queryResultFormat + "'");
    }
    return conn;
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleFormatProvider.class)
  public void testLargeResultSetGCP(String queryResultFormat) throws Throwable {
    try (Connection con = init(queryResultFormat);
        PreparedStatement stmt =
            con.prepareStatement(
                "select seq8(), randstr(1000, random()) from table(generator(rowcount=>1000))")) {
      stmt.setMaxRows(999);
      try (ResultSet rset = stmt.executeQuery()) {
        int cnt = 0;
        while (rset.next()) {
          ++cnt;
        }
        assertEquals(cnt, 999);
      }
    }
  }
}
