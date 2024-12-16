package net.snowflake.client.jdbc.structuredtypes;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import net.snowflake.client.TestUtil;
import net.snowflake.client.ThrowingConsumer;
import net.snowflake.client.jdbc.BaseJDBCTest;
import net.snowflake.client.jdbc.ResultSetFormatType;

abstract class StructuredTypesGetStringBaseIT extends BaseJDBCTest {
  public StructuredTypesGetStringBaseIT() {}

  protected Connection init(ResultSetFormatType queryResultFormat) throws SQLException {
    return initConnection(queryResultFormat);
  }

  protected static Connection initConnection(ResultSetFormatType queryResultFormat)
      throws SQLException {
    Connection conn = BaseJDBCTest.getConnection(BaseJDBCTest.DONT_INJECT_SOCKET_TIMEOUT);
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("alter session set USE_CACHED_RESULT = false");
      stmt.execute("alter session set ENABLE_STRUCTURED_TYPES_IN_CLIENT_RESPONSE = true");
      stmt.execute("alter session set IGNORE_CLIENT_VESRION_IN_STRUCTURED_TYPES_RESPONSE = true");
      stmt.execute("ALTER SESSION SET TIMEZONE = 'Europe/Warsaw'");
      stmt.execute(
          "alter session set "
              + "TIMESTAMP_TYPE_MAPPING='TIMESTAMP_LTZ',"
              + "TIMESTAMP_OUTPUT_FORMAT='DY, DD MON YYYY HH24:MI:SS TZHTZM',"
              + "TIMESTAMP_TZ_OUTPUT_FORMAT='DY, DD MON YYYY HH24:MI:SS TZHTZM',"
              + "TIMESTAMP_LTZ_OUTPUT_FORMAT='DY, DD MON YYYY HH24:MI:SS TZHTZM',"
              + "TIMESTAMP_NTZ_OUTPUT_FORMAT='DY, DD MON YYYY HH24:MI:SS TZHTZM'");
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

  protected void assertResultSetIsCompatible(ResultSet resultSet, String expected)
      throws SQLException {
    // Test getString
    String result = resultSet.getString(1);
    TestUtil.assertEqualsIgnoringWhitespace(expected, result);

    // Test getObject
    result = resultSet.getObject(1, String.class);
    String resultCasted = (String) resultSet.getObject(1);
    TestUtil.assertEqualsIgnoringWhitespace(expected, result);
    TestUtil.assertEqualsIgnoringWhitespace(expected, resultCasted);

    // Test getBytes
    TestUtil.assertEqualsIgnoringWhitespace(
        expected, new String(resultSet.getBytes(1), StandardCharsets.UTF_8));
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
