/*
 * Copyright (c) 2012-2024 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc.structuredtypes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import net.snowflake.client.ThrowingConsumer;
import net.snowflake.client.jdbc.BaseJDBCTest;
import net.snowflake.client.jdbc.ResultSetFormatType;

abstract class ResultSetStructuredTypesBaseIT extends BaseJDBCTest {

  protected final ResultSetFormatType queryResultFormat;

  public ResultSetStructuredTypesBaseIT(ResultSetFormatType queryResultFormat) {
    this.queryResultFormat = queryResultFormat;
  }

  protected Connection init() throws SQLException {
    return initConnection(this.queryResultFormat);
  }

  protected static Connection initConnection(ResultSetFormatType queryResultFormat)
      throws SQLException {
    Connection conn = BaseJDBCTest.getConnection(BaseJDBCTest.DONT_INJECT_SOCKET_TIMEOUT);
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("alter session set USE_CACHED_RESULT = false;");
      stmt.execute("alter session set ENABLE_STRUCTURED_TYPES_IN_CLIENT_RESPONSE = true");
      stmt.execute("alter session set IGNORE_CLIENT_VESRION_IN_STRUCTURED_TYPES_RESPONSE = true");
      stmt.execute("ALTER SESSION SET TIMEZONE = 'Europe/Warsaw'");
      stmt.execute(
          "alter session set jdbc_query_result_format = '"
              + queryResultFormat.sessionParameterTypeValue
              + "'");
      if (queryResultFormat == ResultSetFormatType.NATIVE_ARROW) {
        stmt.execute("alter session set ENABLE_STRUCTURED_TYPES_NATIVE_ARROW_FORMAT = true");
        stmt.execute("alter session set FORCE_ENABLE_STRUCTURED_TYPES_NATIVE_ARROW_FORMAT = true");
      }
    }
    return conn;
  }

  protected void assertGetStringAndGetBytesAreCompatible(ResultSet resultSet, String expected)
      throws SQLException {
    String result = resultSet.getString(1);
    assertEqualsIgnoringWhitespace(expected, result);
    String resultFromBytes = new String(resultSet.getBytes(1));
    assertEqualsIgnoringWhitespace(expected, resultFromBytes);
  }

  protected void assertEqualsIgnoringWhitespace(String expected, String actual) {
    assertEquals(expected.replaceAll("\\s+", ""), actual.replaceAll("\\s+", ""));
  }

  protected void withFirstRow(String sqlText, ThrowingConsumer<ResultSet, SQLException> consumer)
      throws SQLException {
    try (Connection connection = init();
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(sqlText); ) {
      assertTrue(rs.next());
      consumer.accept(rs);
    }
  }

  protected void withFirstRow(
      Connection connection, String sqlText, ThrowingConsumer<ResultSet, SQLException> consumer)
      throws SQLException {
    try (Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(sqlText); ) {
      assertTrue(rs.next());
      consumer.accept(rs);
    }
  }
}
