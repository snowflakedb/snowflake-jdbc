/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
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

  // Test setting new connection property jdbc_arrow_treat_decimal_as_int=false. Connection property
  // introduced after version 3.15.0.
  @Test
  public void testGetObjectForArrowResultFormatJDBCArrowDecimalAsIntFalse() throws SQLException {
    Properties properties = new Properties();
    properties.put("jdbc_arrow_treat_decimal_as_int", false);
    try (Connection con = init(properties);
        Statement stmt = con.createStatement()) {
      stmt.execute(createTableSql);
      stmt.execute(insertStmt);

      // Test with jdbc_arrow_treat_decimal_as_int=false and JDBC_TREAT_DECIMAL_AS_INT=true
      try (ResultSet rs = stmt.executeQuery(selectQuery)) {
        while (rs.next()) {
          assertEquals(rs.getObject(1).getClass().toString(), "class java.lang.Long");
          assertEquals(rs.getObject(2).getClass().toString(), "class java.math.BigDecimal");
          assertEquals(rs.getObject(3).getClass().toString(), "class java.lang.Long");
          assertEquals(rs.getObject(4).getClass().toString(), "class java.lang.Long");
        }
      }

      // Test with jdbc_arrow_treat_decimal_as_int=false and JDBC_TREAT_DECIMAL_AS_INT=false
      stmt.execute(setJdbcTreatDecimalAsIntFalse);
      try (ResultSet rs = stmt.executeQuery(selectQuery)) {
        while (rs.next()) {
          assertEquals(rs.getObject(1).getClass().toString(), "class java.math.BigDecimal");
          assertEquals(rs.getObject(2).getClass().toString(), "class java.math.BigDecimal");
          assertEquals(rs.getObject(3).getClass().toString(), "class java.math.BigDecimal");
          assertEquals(rs.getObject(4).getClass().toString(), "class java.math.BigDecimal");
        }
      }
    }
  }

  // Test default setting of new connection property jdbc_arrow_treat_decimal_as_int=true.
  // Connection property introduced after version 3.15.0.
  @Test
  public void testGetObjectForArrowResultFormatJDBCArrowDecimalAsIntTrue() throws SQLException {
    try (Connection con = init();
        Statement stmt = con.createStatement()) {
      stmt.execute(createTableSql);
      stmt.execute(insertStmt);

      // Test with jdbc_arrow_treat_decimal_as_int=true and JDBC_TREAT_DECIMAL_AS_INT=true
      try (ResultSet rs = stmt.executeQuery(selectQuery)) {
        while (rs.next()) {
          assertEquals(rs.getObject(1).getClass().toString(), "class java.lang.Long");
          assertEquals(rs.getObject(2).getClass().toString(), "class java.math.BigDecimal");
          assertEquals(rs.getObject(3).getClass().toString(), "class java.lang.Long");
          assertEquals(rs.getObject(4).getClass().toString(), "class java.lang.Long");
        }
      }

      // Test with jdbc_arrow_treat_decimal_as_int=true and JDBC_TREAT_DECIMAL_AS_INT=false
      stmt.execute(setJdbcTreatDecimalAsIntFalse);
      try (ResultSet rs = stmt.executeQuery(selectQuery)) {
        while (rs.next()) {
          assertEquals(rs.getObject(1).getClass().toString(), "class java.lang.Long");
          assertEquals(rs.getObject(2).getClass().toString(), "class java.math.BigDecimal");
          assertEquals(rs.getObject(3).getClass().toString(), "class java.lang.Long");
          assertEquals(rs.getObject(4).getClass().toString(), "class java.lang.Long");
        }
      }
    }
  }
}
