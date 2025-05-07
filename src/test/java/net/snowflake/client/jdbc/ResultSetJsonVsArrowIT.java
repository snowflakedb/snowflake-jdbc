package net.snowflake.client.jdbc;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Collectors;
import net.snowflake.client.annotations.DontRunOnGithubActions;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.providers.SimpleResultFormatProvider;
import org.apache.arrow.vector.BigIntVector;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

/** Completely compare json and arrow resultSet behaviors */
@Tag(TestTags.ARROW)
public class ResultSetJsonVsArrowIT extends BaseJDBCTest {

  public Connection init(String queryResultFormat) throws SQLException {
    Connection conn = getConnection(BaseJDBCTest.DONT_INJECT_SOCKET_TIMEOUT);
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("alter session set jdbc_query_result_format = '" + queryResultFormat + "'");
    }
    return conn;
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testGSResult(String queryResultFormat) throws SQLException {
    try (Connection con = init(queryResultFormat);
        Statement statement = con.createStatement();
        ResultSet rs =
            statement.executeQuery(
                "select 1, 128, 65500, 10000000000000, "
                    + "1000000000000000000000000000000000000, NULL, "
                    + "current_timestamp, current_timestamp(0), current_timestamp(5),"
                    + "current_date, current_time, current_time(0), current_time(5);")) {
      assertTrue(rs.next());
      assertEquals((byte) 1, rs.getByte(1));
      assertEquals((short) 128, rs.getShort(2));
      assertEquals(65500, rs.getInt(3));
      assertEquals(10000000000000l, rs.getLong(4));
      assertEquals(new BigDecimal("1000000000000000000000000000000000000"), rs.getBigDecimal(5));
      assertNull(rs.getString(6));
      assertNotNull(rs.getTimestamp(7));
      assertNotNull(rs.getTimestamp(8));
      assertNotNull(rs.getTimestamp(9));

      assertNotNull(rs.getDate(10));
      assertNotNull(rs.getTime(11));
      assertNotNull(rs.getTime(12));
      assertNotNull(rs.getTime(13));
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testGSResultReal(String queryResultFormat) throws SQLException {
    try (Connection con = init(queryResultFormat);
        Statement statement = con.createStatement()) {
      try {
        statement.execute("create or replace table t (a real)");
        statement.execute("insert into t values (123.456)");
        try (ResultSet rs = statement.executeQuery("select * from t;")) {
          assertTrue(rs.next());
          assertEquals(123.456, rs.getFloat(1), 0.001);
        }
      } finally {
        statement.execute("drop table if exists t");
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testGSResultScan(String queryResultFormat) throws SQLException {
    String queryId = null;
    try (Connection con = init(queryResultFormat);
        Statement statement = con.createStatement()) {
      try {
        statement.execute("create or replace table t (a text)");
        statement.execute("insert into t values ('test')");
        try (ResultSet rs = statement.executeQuery("select count(*) from t;")) {
          assertTrue(rs.next());
          assertEquals(1, rs.getInt(1));
          queryId = rs.unwrap(SnowflakeResultSet.class).getQueryID();
        }
        try (ResultSet rs =
            statement.executeQuery("select * from table(result_scan('" + queryId + "'))")) {
          assertTrue(rs.next());
          assertEquals(1, rs.getInt(1));
        }
      } finally {
        statement.execute("drop table if exists t");
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testGSResultForEmptyAndSmallTable(String queryResultFormat) throws SQLException {
    try (Connection con = init(queryResultFormat);
        Statement statement = con.createStatement()) {
      try {
        statement.execute("create or replace table t (a int)");
        try (ResultSet rs = statement.executeQuery("select * from t;")) {
          assertFalse(rs.next());
        }
        statement.execute("insert into t values (1)");
        try (ResultSet rs = statement.executeQuery("select * from t;")) {
          assertTrue(rs.next());
          assertEquals(1, rs.getInt(1));
        }
      } finally {
        statement.execute("drop table if exists t");
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testSNOW89737(String queryResultFormat) throws SQLException {
    try (Connection con = init(queryResultFormat);
        Statement statement = con.createStatement()) {
      try {
        statement.execute(
            "create or replace table test_types(c1 number, c2 integer, c3 float, c4 varchar, c5 char, c6 "
                + "binary, c7 boolean, c8 date, c9 datetime, c10 time, c11 timestamp_ltz, c12 timestamp_tz, c13 "
                + "variant, c14 object, c15 array)");
        statement.execute(
            "insert into test_types values (null, null, null, null, null, null, null, null, null, null, "
                + "null, null, null, null, null)");
        statement.execute(
            "insert into test_types (c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12) values(5, 5, 5.0,"
                + "'hello', 'h', '48454C4C4F', true, '1994-12-27', "
                + "'1994-12-27 05:05:05', '05:05:05', '1994-12-27 05:05:05 +00:05', '1994-12-27 05:05:05')");
        statement.execute(
            "insert into test_types(c13) select parse_json(' { \"key1\\x00\":\"value1\" } ')");
        statement.execute(
            "insert into test_types(c14) select parse_json(' { \"key1\\x00\":\"value1\" } ')");
        statement.execute(
            "insert into test_types(c15) select parse_json('{\"fruits\" : [\"apples\", \"pears\", "
                + "\"oranges\"]}')");
        ResultSet resultSet = statement.executeQuery("select * from test_types");
        // test first row of result set against all "get" methods
        assertTrue(resultSet.next());
        // test getString method against all other data types
        assertEquals(null, resultSet.getString(1));
        assertEquals(null, resultSet.getString(2));
        assertEquals(null, resultSet.getString(3));
        assertEquals(null, resultSet.getString(4));
        assertEquals(null, resultSet.getString(5));
        assertEquals(null, resultSet.getString(6));
        assertEquals(null, resultSet.getString(7));
        assertEquals(null, resultSet.getString(8));
        assertEquals(null, resultSet.getString(9));
        assertEquals(null, resultSet.getString(10));
        assertEquals(null, resultSet.getString(11));
        assertEquals(null, resultSet.getString(12));
        assertEquals(null, resultSet.getString(13));
        assertEquals(null, resultSet.getString(14));
        assertEquals(null, resultSet.getString(15));
      } finally {
        statement.execute("drop table if exists t");
      }
    }
  }

  /**
   * Note: Arrow format does not include space and \n in the string values
   *
   * @throws SQLException
   */
  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testSemiStructuredData(String queryResultFormat) throws SQLException {
    try (Connection con = init(queryResultFormat);
        Statement statement = con.createStatement();
        ResultSet rs =
            statement.executeQuery(
                "select array_construct(10, 20, 30), "
                    + "array_construct(null, 'hello', 3::double, 4, 5), "
                    + "array_construct(), "
                    + "object_construct('a',1,'b','BBBB', 'c',null),"
                    + "object_construct('Key_One', parse_json('NULL'), 'Key_Two', null, 'Key_Three', 'null'),"
                    + "to_variant(3.2),"
                    + "parse_json('{ \"a\": null}'),"
                    + " 100::variant;")) {
      while (rs.next()) {
        assertEquals("[\n" + "  10,\n" + "  20,\n" + "  30\n" + "]", rs.getString(1));
        assertEquals(
            "[\n"
                + "  undefined,\n"
                + "  \"hello\",\n"
                + "  3.000000000000000e+00,\n"
                + "  4,\n"
                + "  5\n"
                + "]",
            rs.getString(2));
        assertEquals("{\n" + "  \"a\": 1,\n" + "  \"b\": \"BBBB\"\n" + "}", rs.getString(4));
        assertEquals(
            "{\n" + "  \"Key_One\": null,\n" + "  \"Key_Three\": \"null\"\n" + "}",
            rs.getString(5));
        assertEquals("{\n" + "  \"a\": null\n" + "}", rs.getString(7));
        assertEquals("[]", rs.getString(3));
        assertEquals("3.2", rs.getString(6));
        assertEquals("100", rs.getString(8));
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testStructuredTypes(String queryResultFormat) throws SQLException {
    try (Connection con = init(queryResultFormat);
        Statement stmt = con.createStatement()) {
      stmt.execute("alter session set feature_structured_types = 'ENABLED';");

      try (ResultSet rs =
          stmt.executeQuery(
              "select array_construct(10, 20, 30)::array(int), "
                  + "object_construct_keep_null('a', 1, 'b', 'BBBB', 'c', null)::object(a int, b varchar, c int), "
                  + "object_construct_keep_null('k1', 'v1', 'k2', null)::map(varchar, varchar);")) {
        while (rs.next()) {
          assertEquals("[\n" + "  10,\n" + "  20,\n" + "  30\n" + "]", rs.getString(1));
          assertEquals(
              "{\n" + "  \"a\": 1,\n" + "  \"b\": \"BBBB\",\n" + "  \"c\": null\n" + "}",
              rs.getString(2));
          assertEquals("{\n" + "  \"k1\": \"v1\",\n" + "  \"k2\": null\n" + "}", rs.getString(3));
        }
      }
    }
  }

  private Connection init(String queryResultFormat, String table, String column, String values)
      throws SQLException {
    Connection con = init(queryResultFormat);
    try (Statement statement = con.createStatement()) {
      statement.execute("create or replace table " + table + " " + column);
      statement.execute("insert into " + table + " values " + values);
    }
    return con;
  }

  private boolean isJSON(String queryResultFormat) {
    return queryResultFormat.equalsIgnoreCase("json");
  }

  /**
   * compare behaviors (json vs arrow)
   *
   * <p>VALUE_IS_NULL Yes No -----------------------------------------------------------------------
   * getColumnType BIGINT 0 getInt same 0 getShort same 0 getLong same 0 getString same null
   * getFloat same 0 getDouble same 0 getBigDecimal same null getObject same null getByte same 0
   * getBytes INTERNAL_ERROR vs SUCCESS null
   * -------------------------------------------------------------------------
   *
   * @throws SQLException
   */
  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testTinyInt(String queryResultFormat) throws SQLException {
    int[] cases = {0, 1, -1, 127, -128};
    String table = "test_arrow_tiny_int";
    String column = "(a int)";
    String values = "(" + StringUtils.join(ArrayUtils.toObject(cases), "),(") + "), (NULL)";
    try (Connection con = init(queryResultFormat, table, column, values);
        Statement statement = con.createStatement();
        ResultSet rs = statement.executeQuery("select * from " + table)) {
      try {
        double delta = 0.1;
        int columnType = rs.getMetaData().getColumnType(1);
        assertEquals(Types.BIGINT, columnType);
        for (int i = 0; i < cases.length; i++) {
          assertTrue(rs.next());
          assertEquals(cases[i], rs.getInt(1));
          assertEquals((short) cases[i], rs.getShort(1));
          assertEquals((long) cases[i], rs.getLong(1));
          assertEquals((Integer.toString(cases[i])), rs.getString(1));
          assertEquals((float) cases[i], rs.getFloat(1), delta);
          double val = cases[i];
          assertEquals(val, rs.getDouble(1), delta);
          assertEquals(new BigDecimal(Integer.toString(cases[i])), rs.getBigDecimal(1));
          assertEquals(rs.getLong(1), rs.getObject(1));
          assertEquals(cases[i], rs.getByte(1));

          byte[] bytes = new byte[1];
          bytes[0] = (byte) cases[i];
          assertArrayEquals(bytes, rs.getBytes(1));
        }
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));
        assertEquals((short) 0, rs.getShort(1));
        assertEquals((long) 0, rs.getLong(1));
        assertNull(rs.getString(1));
        assertEquals((float) 0, rs.getFloat(1), delta);
        double val = 0;
        assertEquals(val, rs.getDouble(1), delta);
        assertNull(rs.getBigDecimal(1));
        assertNull(rs.getObject(1));
        assertEquals(0, rs.getByte(1));
        assertNull(rs.getBytes(1));
        assertTrue(rs.wasNull());
      } finally {
        statement.execute("drop table if exists " + table);
      }
    }
  }

  /**
   * compare behaviors (json vs arrow)
   *
   * <p>VALUE_IS_NULL Yes No
   * ------------------------------------------------------------------------------------------------
   * getColumnType DECIMAL 0 getInt java.NumberFormatException vs SQLException.INVALID_VALUE_CONVERT
   * 0 getShort java.NumberFormatException vs SQLException.INVALID_VALUE_CONVERT 0 getLong same 0
   * getString same null getFloat same 0 getDouble same 0 getBigDecimal same null getObject same
   * null getByte INTERNAL_ERROR vs SUCCESS 0 getBytes INTERNAL_ERROR vs SUCCESS null
   * --------------------------------------------------------------------------------------------------
   *
   * @throws SQLException
   */
  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testScaledTinyInt(String queryResultFormat) throws SQLException {
    float[] cases = {0.0f, 0.11f, -0.11f, 1.27f, -1.28f};
    String table = "test_arrow_tiny_int";
    String column = "(a number(3,2))";
    String values = "(" + StringUtils.join(ArrayUtils.toObject(cases), "),(") + "), (null)";
    try (Connection con = init(queryResultFormat, table, column, values);
        Statement statement = con.createStatement();
        ResultSet rs = con.createStatement().executeQuery("select * from test_arrow_tiny_int")) {
      try {
        double delta = 0.001;
        int columnType = rs.getMetaData().getColumnType(1);
        assertEquals(Types.DECIMAL, columnType);

        for (int i = 0; i < cases.length; i++) {
          assertTrue(rs.next());
          SQLException se = assertThrows(SQLException.class, () -> rs.getInt(1));
          assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
          assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());

          se = assertThrows(SQLException.class, () -> rs.getShort(1));
          assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
          assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());

          se = assertThrows(SQLException.class, () -> rs.getLong(1));
          assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
          assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());

          assertEquals((String.format("%.2f", cases[i])), rs.getString(1));
          assertEquals(cases[i], rs.getFloat(1), delta);
          double val = cases[i];
          assertEquals(val, rs.getDouble(1), delta);
          assertEquals(new BigDecimal(rs.getString(1)), rs.getBigDecimal(1));
          assertEquals(rs.getBigDecimal(1), rs.getObject(1));
          if (isJSON(queryResultFormat)) {
            Exception e = assertThrows(Exception.class, () -> rs.getByte(1));
            // Note: not caught by SQLException!
            assertTrue(e.toString().contains("NumberFormatException"));
          } else {
            assertEquals(((byte) (cases[i] * 100)), rs.getByte(1));
          }

          if (!isJSON(queryResultFormat)) {
            byte[] bytes = new byte[1];
            bytes[0] = rs.getByte(1);
            assertArrayEquals(bytes, rs.getBytes(1));
          }
        }

        // null value
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));
        assertEquals((short) 0, rs.getShort(1));
        assertEquals((long) 0, rs.getLong(1));
        assertNull(rs.getString(1));
        assertEquals((float) 0, rs.getFloat(1), delta);
        double val = 0;
        assertEquals(val, rs.getDouble(1), delta);
        assertNull(rs.getBigDecimal(1));
        assertNull(rs.getObject(1));
        assertEquals(0, rs.getByte(1));
        assertNull(rs.getBytes(1));
        assertTrue(rs.wasNull());
      } finally {
        statement.execute("drop table if exists " + table);
      }
    }
  }

  /**
   * compare behaviors (json vs arrow)
   *
   * <p>VALUE_IS_NULL Yes No -----------------------------------------------------------------------
   * getColumnType BIGINT 0 getInt same 0 getShort same 0 getLong same 0 getString same null
   * getFloat same 0 getDouble same 0 getBigDecimal same null getObject same null getByte
   * INTERNAL_ERROR vs INVALID_VALUE_CONVERT 0 getBytes INTERNAL_ERROR vs SUCCESS null
   * -------------------------------------------------------------------------
   *
   * @throws SQLException
   */
  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testSmallInt(String queryResultFormat) throws SQLException {
    short[] cases = {0, 1, -1, 127, -128, 128, -129, 32767, -32768};
    String table = "test_arrow_small_int";
    String column = "(a int)";
    String values = "(" + StringUtils.join(ArrayUtils.toObject(cases), "),(") + "), (NULL)";
    try (Connection con = init(queryResultFormat, table, column, values);
        Statement statement = con.createStatement();
        ResultSet rs = statement.executeQuery("select * from " + table)) {
      try {
        double delta = 0.1;
        int columnType = rs.getMetaData().getColumnType(1);
        assertEquals(Types.BIGINT, columnType);
        for (int i = 0; i < cases.length; i++) {
          assertTrue(rs.next());
          assertEquals(cases[i], rs.getInt(1));
          assertEquals(cases[i], rs.getShort(1));
          assertEquals((long) cases[i], rs.getLong(1));
          assertEquals((Integer.toString(cases[i])), rs.getString(1));
          assertEquals((float) cases[i], rs.getFloat(1), delta);
          double val = cases[i];
          assertEquals(val, rs.getDouble(1), delta);
          assertEquals(new BigDecimal(Integer.toString(cases[i])), rs.getBigDecimal(1));
          assertEquals(rs.getLong(1), rs.getObject(1));
          if (cases[i] <= 127 && cases[i] >= -128) {
            assertEquals(cases[i], rs.getByte(1));
          } else {
            Exception e = assertThrows(Exception.class, () -> rs.getByte(1));
            if (isJSON(queryResultFormat)) {
              // Note: not caught by SQLException!
              assertTrue(e.toString().contains("NumberFormatException"));
            } else {
              SQLException se = assertInstanceOf(SQLException.class, e);
              assertEquals(
                  (int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
              assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());
            }
          }
          ByteBuffer bb = ByteBuffer.allocate(2);
          bb.putShort(cases[i]);
          if (isJSON(queryResultFormat)) {
            byte[] res = rs.getBytes(1);
            for (int j = res.length - 1; j >= 0; j--) {
              assertEquals(bb.array()[2 - res.length + j], res[j]);
            }
          } else {
            assertArrayEquals(bb.array(), rs.getBytes(1));
          }
        }
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));
        assertEquals((short) 0, rs.getShort(1));
        assertEquals((long) 0, rs.getLong(1));
        assertNull(rs.getString(1));
        assertEquals((float) 0, rs.getFloat(1), delta);
        double val = 0;
        assertEquals(val, rs.getDouble(1), delta);
        assertNull(rs.getBigDecimal(1));
        assertNull(rs.getObject(1));
        assertEquals(0, rs.getByte(1));
        assertNull(rs.getBytes(1));
        assertTrue(rs.wasNull());
      } finally {
        statement.execute("drop table if exists " + table);
      }
    }
  }

  /**
   * compare behaviors (json vs arrow)
   *
   * <p>VALUE_IS_NULL Yes No
   * ----------------------------------------------------------------------------- getColumnType
   * DECIMAL 0 getInt NumberFormatException vs INVALID_VALUE_CONVERT 0 getShort
   * NumberFormatException vs INVALID_VALUE_CONVERT 0 getLong same 0 getString same null getFloat
   * same 0 getDouble same 0 getBigDecimal same null getObject same null getByte
   * NumberFormatException vs INVALID_VALUE_CONVERT 0 getBytes INTERNAL_ERROR vs SUCCESS null
   * -------------------------------------------------------------------------------
   *
   * @throws SQLException
   */
  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testScaledSmallInt(String queryResultFormat) throws SQLException {
    float[] cases = {0, 2.0f, -2.0f, 32.767f, -32.768f};
    short[] shortCompact = {0, 2000, -2000, 32767, -32768};
    String table = "test_arrow_small_int";
    String column = "(a number(5,3))";
    String values = "(" + StringUtils.join(ArrayUtils.toObject(cases), "),(") + "), (null)";
    try (Connection con = init(queryResultFormat, table, column, values);
        Statement statement = con.createStatement();
        ResultSet rs = con.createStatement().executeQuery("select * from test_arrow_small_int")) {
      try {
        double delta = 0.0001;
        int columnType = rs.getMetaData().getColumnType(1);
        assertEquals(Types.DECIMAL, columnType);

        for (int i = 0; i < cases.length; i++) {
          assertTrue(rs.next());
          SQLException se = assertThrows(SQLException.class, () -> rs.getInt(1));
          assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
          assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());

          se = assertThrows(SQLException.class, () -> rs.getShort(1));
          assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
          assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());

          se = assertThrows(SQLException.class, () -> rs.getLong(1));
          assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
          assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());

          assertEquals((String.format("%.3f", cases[i])), rs.getString(1));
          assertEquals(cases[i], rs.getFloat(1), delta);
          double val = cases[i];
          assertEquals(val, rs.getDouble(1), delta);
          assertEquals(new BigDecimal(rs.getString(1)), rs.getBigDecimal(1));
          assertEquals(rs.getBigDecimal(1), rs.getObject(1));
          Exception e = assertThrows(Exception.class, () -> rs.getByte(1));
          if (isJSON(queryResultFormat)) {
            // Note: not caught by SQLException!
            assertTrue(e.toString().contains("NumberFormatException"));
          } else {
            se = assertInstanceOf(SQLException.class, e);
            assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
            assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());
          }

          ByteBuffer byteBuffer = ByteBuffer.allocate(2);
          byteBuffer.putShort(shortCompact[i]);
          if (isJSON(queryResultFormat)) {
            se = assertThrows(SQLException.class, () -> rs.getBytes(1));
            assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
            assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());
          } else {
            assertArrayEquals(byteBuffer.array(), rs.getBytes(1));
          }
        }

        // null value
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));
        assertEquals((short) 0, rs.getShort(1));
        assertEquals((long) 0, rs.getLong(1));
        assertNull(rs.getString(1));
        assertEquals((float) 0, rs.getFloat(1), delta);
        double val = 0;
        assertEquals(val, rs.getDouble(1), delta);
        assertNull(rs.getBigDecimal(1));
        assertEquals(null, rs.getObject(1));
        assertEquals(0, rs.getByte(1));
        assertNull(rs.getBytes(1));
        assertTrue(rs.wasNull());
      } finally {
        statement.execute("drop table if exists " + table);
      }
    }
  }

  /**
   * compare behaviors (json vs arrow)
   *
   * <p>VALUE_IS_NULL Yes No
   * -------------------------------------------------------------------------- getColumnType BIGINT
   * 0 getInt same 0 getShort same 0 getLong same 0 getString same null getFloat same 0 getDouble
   * same 0 getBigDecimal same null getObject same null getByte INTERNAL_ERROR vs
   * INVALID_VALUE_CONVERT 0 getBytes INTERNAL_ERROR OR vs SUCCESS null Return wrong result
   * -------------------------------------------------------------------------
   *
   * @throws SQLException
   */
  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testInt(String queryResultFormat) throws SQLException {
    int[] cases = {
      0, 1, -1, 127, -128, 128, -129, 32767, -32768, 32768, -32769, 2147483647, -2147483648
    };
    String table = "test_arrow_int";
    String column = "(a int)";
    String values = "(" + StringUtils.join(ArrayUtils.toObject(cases), "),(") + "), (NULL)";
    try (Connection con = init(queryResultFormat, table, column, values);
        Statement statement = con.createStatement();
        ResultSet rs = con.createStatement().executeQuery("select * from " + table)) {
      try {
        double delta = 0.1;
        int columnType = rs.getMetaData().getColumnType(1);
        assertEquals(Types.BIGINT, columnType);
        for (int i = 0; i < cases.length; i++) {
          assertTrue(rs.next());
          assertEquals(cases[i], rs.getInt(1));
          if (cases[i] >= Short.MIN_VALUE && cases[i] <= Short.MAX_VALUE) {
            assertEquals((short) cases[i], rs.getShort(1));
          } else {
            SQLException se = assertThrows(SQLException.class, () -> rs.getShort(1));
            assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
            assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());
          }
          assertEquals((long) cases[i], rs.getLong(1));
          assertEquals((Integer.toString(cases[i])), rs.getString(1));
          assertEquals((float) cases[i], rs.getFloat(1), delta);
          double val = cases[i];
          assertEquals(val, rs.getDouble(1), delta);
          assertEquals(new BigDecimal(Integer.toString(cases[i])), rs.getBigDecimal(1));
          assertEquals(rs.getLong(1), rs.getObject(1));
          if (cases[i] <= 127 && cases[i] >= -128) {
            assertEquals(cases[i], rs.getByte(1));
          } else {
            Exception e = assertThrows(Exception.class, () -> rs.getByte(1));
            if (isJSON(queryResultFormat)) {
              // Note: not caught by SQLException!
              assertTrue(e.toString().contains("NumberFormatException"));
            } else {
              SQLException se = assertInstanceOf(SQLException.class, e);
              assertEquals(
                  (int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
              assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());
            }
          }
          ByteBuffer bb = ByteBuffer.allocate(4);
          bb.putInt(cases[i]);
          if (isJSON(queryResultFormat)) {
            byte[] res = rs.getBytes(1);
            for (int j = res.length - 1; j >= 0; j--) {
              assertEquals(bb.array()[4 - res.length + j], res[j]);
            }
          } else {
            assertArrayEquals(bb.array(), rs.getBytes(1));
          }
        }
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));
        assertEquals((short) 0, rs.getShort(1));
        assertEquals((long) 0, rs.getLong(1));
        assertNull(rs.getString(1));
        assertEquals((float) 0, rs.getFloat(1), delta);
        double val = 0;
        assertEquals(val, rs.getDouble(1), delta);
        assertNull(rs.getBigDecimal(1));
        assertNull(rs.getObject(1));
        assertEquals(0, rs.getByte(1));
        assertNull(rs.getBytes(1));
        assertTrue(rs.wasNull());
      } finally {
        statement.execute("drop table if exists " + table);
      }
    }
  }

  /**
   * compare behaviors (json vs arrow)
   *
   * <p>VALUE_IS_NULL Yes No
   * ----------------------------------------------------------------------------- getColumnType
   * DECIMAL 0 getInt NumberFormatException vs INVALID_VALUE_CONVERT 0 getShort
   * NumberFormatException vs INVALID_VALUE_CONVERT 0 getLong same 0 getString same null getFloat
   * same 0 getDouble same 0 getBigDecimal same null getObject same null getByte
   * NumberFormatException vs INVALID_VALUE_CONVERT 0 getBytes INTERNAL_ERROR vs SUCCESS null
   * -------------------------------------------------------------------------------
   *
   * @throws SQLException
   */
  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testScaledInt(String queryResultFormat) throws SQLException {
    int scale = 9;
    int[] intCompacts = {0, 123456789, -123456789, 2147483647, -2147483647};
    List<BigDecimal> caseList =
        Arrays.stream(intCompacts)
            .mapToObj(x -> BigDecimal.valueOf(x, scale))
            .collect(Collectors.toList());

    BigDecimal[] cases = caseList.stream().toArray(BigDecimal[]::new);

    String table = "test_arrow_int";

    String column = String.format("(a number(10,%d))", scale);
    String values = "(" + StringUtils.join(cases, "),(") + "), (null)";
    try (Connection con = init(queryResultFormat, table, column, values);
        Statement statement = con.createStatement();
        ResultSet rs = con.createStatement().executeQuery("select * from test_arrow_int")) {
      try {
        double delta = 0.0000000001;
        int columnType = rs.getMetaData().getColumnType(1);
        assertEquals(Types.DECIMAL, columnType);

        for (int i = 0; i < cases.length; i++) {
          assertTrue(rs.next());
          SQLException se = assertThrows(SQLException.class, () -> rs.getInt(1));
          assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
          assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());

          se = assertThrows(SQLException.class, () -> rs.getShort(1));
          assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
          assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());

          se = assertThrows(SQLException.class, () -> rs.getLong(1));
          assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
          assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());

          assertEquals(cases[i].toPlainString(), rs.getString(1));
          assertEquals(Float.parseFloat(cases[i].toString()), rs.getFloat(1), delta);
          double val = Double.parseDouble(cases[i].toString());
          assertEquals(val, rs.getDouble(1), delta);
          assertEquals(new BigDecimal(rs.getString(1)), rs.getBigDecimal(1));
          assertEquals(rs.getBigDecimal(1), rs.getObject(1));

          Exception e = assertThrows(Exception.class, () -> rs.getByte(1));
          if (isJSON(queryResultFormat)) {
            // Note: not caught by SQLException!
            assertTrue(e.toString().contains("NumberFormatException"));
          } else {
            se = assertInstanceOf(SQLException.class, e);
            assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
            assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());
          }

          if (isJSON(queryResultFormat)) {
            se = assertThrows(SQLException.class, () -> rs.getBytes(1));
            assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
            assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());
          } else {
            ByteBuffer byteBuffer = ByteBuffer.allocate(4);
            byteBuffer.putInt(intCompacts[i]);
            assertArrayEquals(byteBuffer.array(), rs.getBytes(1));
          }
        }

        // null value
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));
        assertEquals((short) 0, rs.getShort(1));
        assertEquals((long) 0, rs.getLong(1));
        assertNull(rs.getString(1));
        assertEquals((float) 0, rs.getFloat(1), delta);
        double val = 0;
        assertEquals(val, rs.getDouble(1), delta);
        assertNull(rs.getBigDecimal(1));
        assertEquals(null, rs.getObject(1));
        assertEquals(0, rs.getByte(1));
        assertNull(rs.getBytes(1));
        assertTrue(rs.wasNull());
      } finally {
        statement.execute("drop table if exists " + table);
      }
    }
  }

  /**
   * compare behaviors (json vs arrow)
   *
   * <p>VALUE_IS_NULL Yes No
   * -------------------------------------------------------------------------- getColumnType BIGINT
   * 0 getInt same 0 getShort same 0 getLong same 0 getString same null getFloat same 0 getDouble
   * same 0 getBigDecimal same null getObject same null getByte INTERNAL_ERROR vs
   * INVALID_VALUE_CONVERT 0 getBytes INTERNAL_ERROR OR vs SUCCESS null Return wrong result
   * -------------------------------------------------------------------------
   *
   * @throws SQLException
   */
  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testBigInt(String queryResultFormat) throws SQLException {
    long[] cases = {
      0,
      1,
      -1,
      127,
      -128,
      128,
      -129,
      32767,
      -32768,
      32768,
      -32769,
      2147483647,
      -2147483648,
      2147483648l,
      -2147483649l,
      Long.MAX_VALUE,
      Long.MIN_VALUE
    };
    String table = "test_arrow_big_int";
    String column = "(a int)";
    String values = "(" + StringUtils.join(ArrayUtils.toObject(cases), "),(") + "), (NULL)";
    try (Connection con = init(queryResultFormat, table, column, values);
        Statement statement = con.createStatement();
        ResultSet rs = statement.executeQuery("select * from " + table)) {
      try {
        double delta = 0.1;
        int columnType = rs.getMetaData().getColumnType(1);
        assertEquals(Types.BIGINT, columnType);
        for (int i = 0; i < cases.length; i++) {
          assertTrue(rs.next());

          if (cases[i] >= Integer.MIN_VALUE && cases[i] <= Integer.MAX_VALUE) {
            assertEquals(cases[i], rs.getInt(1));
          } else {
            SQLException se = assertThrows(SQLException.class, () -> rs.getInt(1));
            assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
            assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());
          }

          if (cases[i] >= Short.MIN_VALUE && cases[i] <= Short.MAX_VALUE) {
            assertEquals((short) cases[i], rs.getShort(1));
          } else {
            SQLException se = assertThrows(SQLException.class, () -> rs.getShort(1));
            assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
            assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());
          }
          assertEquals(cases[i], rs.getLong(1));
          assertEquals((Long.toString(cases[i])), rs.getString(1));
          assertEquals((float) cases[i], rs.getFloat(1), delta);
          double val = cases[i];
          assertEquals(val, rs.getDouble(1), delta);
          assertEquals(new BigDecimal(Long.toString(cases[i])), rs.getBigDecimal(1));
          assertEquals(rs.getLong(1), rs.getObject(1));
          if (cases[i] <= 127 && cases[i] >= -128) {
            assertEquals(cases[i], rs.getByte(1));
          } else {
            Exception e = assertThrows(Exception.class, () -> rs.getByte(1));
            if (isJSON(queryResultFormat)) {
              // Note: not caught by SQLException!
              assertTrue(e.toString().contains("NumberFormatException"));
            } else {
              SQLException se = assertInstanceOf(SQLException.class, e);
              assertEquals(
                  (int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
              assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());
            }
          }
          ByteBuffer bb = ByteBuffer.allocate(8);
          bb.putLong(cases[i]);
          byte[] res = rs.getBytes(1);
          for (int j = res.length - 1; j >= 0; j--) {
            assertEquals(bb.array()[8 - res.length + j], res[j]);
          }
        }
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));
        assertEquals((short) 0, rs.getShort(1));
        assertEquals((long) 0, rs.getLong(1));
        assertNull(rs.getString(1));
        assertEquals((float) 0, rs.getFloat(1), delta);
        double val = 0;
        assertEquals(val, rs.getDouble(1), delta);
        assertNull(rs.getBigDecimal(1));
        assertEquals(null, rs.getObject(1));
        assertEquals(0, rs.getByte(1));
        assertNull(rs.getBytes(1));
        assertTrue(rs.wasNull());
      } finally {
        statement.execute("drop table if exists " + table);
      }
    }
  }

  /**
   * compare behaviors (json vs arrow)
   *
   * <p>VALUE_IS_NULL Yes No
   * ----------------------------------------------------------------------------- getColumnType
   * DECIMAL 0 getInt NumberFormatException vs INVALID_VALUE_CONVERT 0 getShort
   * NumberFormatException vs INVALID_VALUE_CONVERT 0 getLong same 0 getString same null getFloat
   * same 0 getDouble same 0 getBigDecimal same null getObject same null getByte
   * NumberFormatException vs INVALID_VALUE_CONVERT 0 getBytes INTERNAL_ERROR vs SUCCESS null
   * -------------------------------------------------------------------------------
   *
   * @throws SQLException
   */
  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testScaledBigInt(String queryResultFormat) throws SQLException {
    int scale = 18;
    long[] longCompacts = {
      0, 123456789, -123456789, 2147483647, -2147483647, Long.MIN_VALUE, Long.MAX_VALUE
    };
    List<BigDecimal> caseList =
        Arrays.stream(longCompacts)
            .mapToObj(x -> BigDecimal.valueOf(x, scale))
            .collect(Collectors.toList());

    BigDecimal[] cases = caseList.stream().toArray(BigDecimal[]::new);

    String table = "test_arrow_big_int";

    String column = String.format("(a number(38,%d))", scale);
    String values = "(" + StringUtils.join(cases, "),(") + "), (null)";
    try (Connection con = init(queryResultFormat, table, column, values);
        Statement statement = con.createStatement();
        ResultSet rs = statement.executeQuery("select * from " + table)) {
      try {
        double delta = 0.0000000000000000001;
        int columnType = rs.getMetaData().getColumnType(1);
        assertEquals(Types.DECIMAL, columnType);

        for (int i = 0; i < cases.length; i++) {
          assertTrue(rs.next());
          SQLException se = assertThrows(SQLException.class, () -> rs.getInt(1));
          assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
          assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());

          se = assertThrows(SQLException.class, () -> rs.getShort(1));
          assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
          assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());

          se = assertThrows(SQLException.class, () -> rs.getLong(1));
          assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
          assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());

          assertEquals(cases[i].toPlainString(), rs.getString(1));
          assertEquals(Float.parseFloat(cases[i].toString()), rs.getFloat(1), delta);
          double val = Double.parseDouble(cases[i].toString());
          assertEquals(val, rs.getDouble(1), delta);
          assertEquals(new BigDecimal(rs.getString(1)), rs.getBigDecimal(1));
          assertEquals(rs.getBigDecimal(1), rs.getObject(1));

          Exception e = assertThrows(Exception.class, () -> rs.getByte(1));
          if (isJSON(queryResultFormat)) {
            // Note: not caught by SQLException!
            assertTrue(e.toString().contains("NumberFormatException"));
          } else {
            se = assertInstanceOf(SQLException.class, e);
            assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
            assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());
          }

          if (isJSON(queryResultFormat)) {
            se = assertThrows(SQLException.class, () -> rs.getBytes(1));
            assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
            assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());
          } else {
            ByteBuffer byteBuffer = ByteBuffer.allocate(BigIntVector.TYPE_WIDTH);
            byteBuffer.putLong(longCompacts[i]);
            assertArrayEquals(byteBuffer.array(), rs.getBytes(1));
          }
        }

        // null value
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));
        assertEquals((short) 0, rs.getShort(1));
        assertEquals((long) 0, rs.getLong(1));
        assertNull(rs.getString(1));
        assertEquals((float) 0, rs.getFloat(1), delta);
        double val = 0;
        assertEquals(val, rs.getDouble(1), delta);
        assertNull(rs.getBigDecimal(1));
        assertEquals(null, rs.getObject(1));
        assertEquals(0, rs.getByte(1));
        assertNull(rs.getBytes(1));
        assertTrue(rs.wasNull());
      } finally {
        statement.execute("drop table if exists " + table);
      }
    }
  }

  /**
   * compare behaviors (json vs arrow)
   *
   * <p>VALUE_IS_NULL Yes No
   * ----------------------------------------------------------------------------- getColumnType
   * DECIMAL 0 getInt NumberFormatException vs INVALID_VALUE_CONVERT 0 getShort
   * NumberFormatException vs INVALID_VALUE_CONVERT 0 getLong same 0 getString same null getFloat
   * same 0 getDouble same 0 getBigDecimal same null getObject INTERNAL_ERROR vs SUCCESS null
   * getByte NumberFormatException vs INVALID_VALUE_CONVERT 0 getBytes INTERNAL_ERROR vs SUCCESS
   * null -------------------------------------------------------------------------------
   *
   * @throws SQLException
   */
  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testDecimalNoScale(String queryResultFormat) throws SQLException {
    int scale = 0;
    String[] longCompacts = {
      "10000000000000000000000000000000000000",
      "12345678901234567890123456789012345678", // pragma: allowlist secret
      "99999999999999999999999999999999999999"
    };
    List<BigDecimal> caseList =
        Arrays.stream(longCompacts).map(x -> new BigDecimal(x)).collect(Collectors.toList());

    BigDecimal[] cases = caseList.stream().toArray(BigDecimal[]::new);

    String table = "test_arrow_decimal";

    String column = String.format("(a number(38,%d))", scale);
    String values = "(" + StringUtils.join(cases, "),(") + "), (null)";
    try (Connection con = init(queryResultFormat, table, column, values);
        Statement statement = con.createStatement();
        ResultSet rs = statement.executeQuery("select * from " + table)) {
      try {
        double delta = 0.1;
        int columnType = rs.getMetaData().getColumnType(1);

        assertEquals(Types.BIGINT, columnType);

        for (int i = 0; i < cases.length; i++) {
          assertTrue(rs.next());

          SQLException se = assertThrows(SQLException.class, () -> rs.getInt(1));
          assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
          assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());

          se = assertThrows(SQLException.class, () -> rs.getShort(1));
          assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
          assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());

          se = assertThrows(SQLException.class, () -> rs.getLong(1));
          assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
          assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());

          assertEquals(cases[i].toPlainString(), rs.getString(1));
          assertEquals(Float.parseFloat(cases[i].toString()), rs.getFloat(1), delta);
          double val = Double.parseDouble(cases[i].toString());
          assertEquals(val, rs.getDouble(1), delta);
          assertEquals(new BigDecimal(rs.getString(1)), rs.getBigDecimal(1));
          assertEquals(rs.getBigDecimal(1), rs.getObject(1));

          Exception e = assertThrows(Exception.class, () -> rs.getByte(1));
          if (isJSON(queryResultFormat)) {
            // Note: not caught by SQLException!
            assertTrue(e.toString().contains("NumberFormatException"));
          } else {
            se = assertInstanceOf(SQLException.class, e);
            assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
            assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());
          }
          assertArrayEquals(cases[i].toBigInteger().toByteArray(), rs.getBytes(1));
        }

        // null value
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));
        assertEquals((short) 0, rs.getShort(1));
        assertEquals((long) 0, rs.getLong(1));
        assertNull(rs.getString(1));
        assertEquals((float) 0, rs.getFloat(1), delta);
        double val = 0;
        assertEquals(val, rs.getDouble(1), delta);
        assertNull(rs.getBigDecimal(1));
        assertEquals(null, rs.getObject(1));
        assertEquals(0, rs.getByte(1));
        assertNull(rs.getBytes(1));
        assertTrue(rs.wasNull());
      } finally {
        statement.execute("drop table if exists " + table);
      }
    }
  }

  /**
   * compare behaviors (json vs arrow)
   *
   * <p>VALUE_IS_NULL Yes No
   * ----------------------------------------------------------------------------- getColumnType
   * DECIMAL 0 getInt NumberFormatException vs INVALID_VALUE_CONVERT 0 getShort
   * NumberFormatException vs INVALID_VALUE_CONVERT 0 getLong same 0 getString same null getFloat
   * same 0 getDouble same 0 getBigDecimal same null getObject same null getByte
   * NumberFormatException vs INVALID_VALUE_CONVERT 0 getBytes INTERNAL_ERROR vs SUCCESS null
   * -------------------------------------------------------------------------------
   *
   * @throws SQLException
   */
  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testDecimalWithLargeScale(String queryResultFormat) throws SQLException {
    int scale = 37;
    String[] longCompacts = {
      "1.0000000000000000000000000000000000000",
      "1.2345678901234567890123456789012345678",
      "9.9999999999999999999999999999999999999"
    };
    List<BigDecimal> caseList =
        Arrays.stream(longCompacts).map(x -> new BigDecimal(x)).collect(Collectors.toList());

    BigDecimal[] cases = caseList.stream().toArray(BigDecimal[]::new);

    String table = "test_arrow_decimal";

    String column = String.format("(a number(38,%d))", scale);
    String values = "(" + StringUtils.join(cases, "),(") + "), (null)";
    try (Connection con = init(queryResultFormat, table, column, values);
        Statement statement = con.createStatement();
        ResultSet rs = statement.executeQuery("select * from " + table)) {
      try {
        double delta = 0.00000000000000000000000000000000000001;
        int columnType = rs.getMetaData().getColumnType(1);
        assertEquals(Types.DECIMAL, columnType);

        for (int i = 0; i < cases.length; i++) {
          assertTrue(rs.next());

          SQLException se = assertThrows(SQLException.class, () -> rs.getInt(1));
          assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
          assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());

          se = assertThrows(SQLException.class, () -> rs.getShort(1));
          assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
          assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());

          se = assertThrows(SQLException.class, () -> rs.getLong(1));
          assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
          assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());

          assertEquals(cases[i].toPlainString(), rs.getString(1));
          assertEquals(Float.parseFloat(cases[i].toString()), rs.getFloat(1), delta);
          double val = Double.parseDouble(cases[i].toString());
          assertEquals(val, rs.getDouble(1), delta);
          assertEquals(new BigDecimal(rs.getString(1)), rs.getBigDecimal(1));
          assertEquals(rs.getBigDecimal(1), rs.getObject(1));

          Exception e = assertThrows(Exception.class, () -> rs.getByte(1));
          if (isJSON(queryResultFormat)) {
            // Note: not caught by SQLException!
            assertTrue(e.toString().contains("NumberFormatException"));
          } else {
            se = assertInstanceOf(SQLException.class, e);
            assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
            assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());
          }

          if (isJSON(queryResultFormat)) {
            se = assertThrows(SQLException.class, () -> rs.getBytes(1));
            assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
            assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());
          } else {
            assertArrayEquals(cases[i].toBigInteger().toByteArray(), rs.getBytes(1));
          }
        }

        // null value
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));
        assertEquals((short) 0, rs.getShort(1));
        assertEquals((long) 0, rs.getLong(1));
        assertNull(rs.getString(1));
        assertEquals((float) 0, rs.getFloat(1), delta);
        double val = 0;
        assertEquals(val, rs.getDouble(1), delta);
        assertNull(rs.getBigDecimal(1));
        assertEquals(null, rs.getObject(1));
        assertEquals(0, rs.getByte(1));
        assertNull(rs.getBytes(1));
        assertTrue(rs.wasNull());
      } finally {
        statement.execute("drop table if exists " + table);
      }
    }
  }

  /**
   * compare behaviors (json vs arrow) decimal values stored in bigInt
   *
   * <p>VALUE_IS_NULL Yes No
   * ----------------------------------------------------------------------------- getColumnType
   * DECIMAL 0 getInt NumberFormatException vs INVALID_VALUE_CONVERT 0 getShort
   * NumberFormatException vs INVALID_VALUE_CONVERT 0 getLong same 0 getString same null getFloat
   * same 0 getDouble same 0 getBigDecimal same null getObject same null getByte
   * NumberFormatException vs INVALID_VALUE_CONVERT 0 getBytes INTERNAL_ERROR vs SUCCESS null
   * -------------------------------------------------------------------------------
   *
   * @throws SQLException
   */
  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testDecimal(String queryResultFormat) throws SQLException {
    int scale = 37;
    long[] longCompacts = {
      0, 123456789, -123456789, 2147483647, -2147483647, Long.MIN_VALUE, Long.MAX_VALUE
    };
    List<BigDecimal> caseList =
        Arrays.stream(longCompacts)
            .mapToObj(x -> BigDecimal.valueOf(x, scale))
            .collect(Collectors.toList());

    BigDecimal[] cases = caseList.stream().toArray(BigDecimal[]::new);

    String table = "test_arrow_big_int";

    String column = String.format("(a number(38,%d))", scale);
    String values = "(" + StringUtils.join(cases, "),(") + "), (null)";
    try (Connection con = init(queryResultFormat, table, column, values);
        Statement statement = con.createStatement();
        ResultSet rs = con.createStatement().executeQuery("select * from " + table)) {
      try {
        double delta = 0.00000000000000000000000000000000000001;
        ByteBuffer byteBuf = ByteBuffer.allocate(BigIntVector.TYPE_WIDTH);
        int columnType = rs.getMetaData().getColumnType(1);
        assertEquals(Types.DECIMAL, columnType);

        for (int i = 0; i < cases.length; i++) {
          assertTrue(rs.next());

          SQLException se = assertThrows(SQLException.class, () -> rs.getInt(1));
          assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
          assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());

          se = assertThrows(SQLException.class, () -> rs.getShort(1));
          assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
          assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());

          se = assertThrows(SQLException.class, () -> rs.getLong(1));
          assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
          assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());

          assertEquals(cases[i].toPlainString(), rs.getString(1));
          assertEquals(Float.parseFloat(cases[i].toString()), rs.getFloat(1), delta);
          double val = Double.parseDouble(cases[i].toString());
          assertEquals(val, rs.getDouble(1), delta);
          assertEquals(new BigDecimal(rs.getString(1)), rs.getBigDecimal(1));
          assertEquals(rs.getBigDecimal(1), rs.getObject(1));

          Exception e = assertThrows(Exception.class, () -> rs.getByte(1));
          if (isJSON(queryResultFormat)) {
            // Note: not caught by SQLException!
            assertTrue(e.toString().contains("NumberFormatException"));
          } else {
            se = assertInstanceOf(SQLException.class, e);
            assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
            assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());
          }

          if (isJSON(queryResultFormat)) {
            se = assertThrows(SQLException.class, () -> rs.getBytes(1));
            assertEquals((int) ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(), se.getErrorCode());
            assertEquals(ErrorCode.INVALID_VALUE_CONVERT.getSqlState(), se.getSQLState());
          } else {
            assertArrayEquals(byteBuf.putLong(0, longCompacts[i]).array(), rs.getBytes(1));
          }
        }

        // null value
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));
        assertEquals((short) 0, rs.getShort(1));
        assertEquals((long) 0, rs.getLong(1));
        assertNull(rs.getString(1));
        assertEquals((float) 0, rs.getFloat(1), delta);
        double val = 0;
        assertEquals(val, rs.getDouble(1), delta);
        assertNull(rs.getBigDecimal(1));
        assertEquals(null, rs.getObject(1));
        assertEquals(0, rs.getByte(1));
        assertNull(rs.getBytes(1));
        assertTrue(rs.wasNull());
      } finally {
        statement.execute("drop table if exists " + table);
      }
    }
  }

  /**
   * Arrow can make sure no precision loss for double values
   *
   * @throws SQLException
   */
  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testDoublePrecision(String queryResultFormat) throws SQLException {
    String[] cases = {
      // SNOW-31249
      "-86.6426540296895",
      "3.14159265359",
      // SNOW-76269
      "1.7976931348623157E308",
      "1.7E308",
      "1.7976931348623151E308",
      "-1.7976931348623151E308",
      "-1.7E308",
      "-1.7976931348623157E308"
    };

    // json results have been truncated to maxScale = 9
    String[] json_results = {
      "-86.64265403",
      "3.141592654",
      "Infinity",
      "1.7E308",
      "Infinity",
      "-Infinity",
      "-1.7E308",
      "-Infinity"
    };
    String table = "test_arrow_double";

    String column = "(a double)";
    String values = "(" + StringUtils.join(cases, "),(") + ")";
    try (Connection con = init(queryResultFormat, table, column, values);
        Statement statement = con.createStatement();
        ResultSet rs = statement.executeQuery("select * from " + table)) {
      try {
        int i = 0;
        if (isJSON(queryResultFormat)) {
          while (rs.next()) {
            assertEquals(json_results[i++], Double.toString(rs.getDouble(1)));
          }
        } else {
          // Arrow results has no precision loss
          while (rs.next()) {
            assertEquals(cases[i++], Double.toString(rs.getDouble(1)));
          }
        }
      } finally {
        statement.execute("drop table if exists " + table);
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testBoolean(String queryResultFormat) throws SQLException {
    String table = "test_arrow_boolean";
    String column = "(a boolean)";
    String values = "(true),(null),(false)";
    try (Connection conn = init(queryResultFormat, table, column, values);
        Statement statement = conn.createStatement();
        ResultSet rs = statement.executeQuery("select * from " + table)) {
      assertTrue(rs.next());
      assertTrue(rs.getBoolean(1));
      assertEquals("TRUE", rs.getString(1));
      assertTrue(rs.next());
      assertFalse(rs.getBoolean(1));
      assertTrue(rs.next());
      assertFalse(rs.getBoolean(1));
      assertEquals("FALSE", rs.getString(1));
      assertFalse(rs.next());
      statement.execute("drop table if exists " + table);
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testClientSideSorting(String queryResultFormat) throws SQLException {
    String table = "test_arrow_sort_on";
    String column = "( a int, b double, c string)";
    String values = "(1,2.0,'test'),(0,2.0, 'test'),(1,2.0,'abc')";
    try (Connection conn = init(queryResultFormat, table, column, values);
        Statement statement = conn.createStatement()) {
      try {
        // turn on sorting mode
        statement.execute("set-sf-property sort on");

        try (ResultSet rs = statement.executeQuery("select * from " + table)) {
          assertTrue(rs.next());
          assertEquals("0", rs.getString(1));
          assertTrue(rs.next());
          assertEquals("1", rs.getString(1));
          assertTrue(rs.next());
          assertEquals("test", rs.getString(3));
        }
      } finally {
        statement.execute("drop table if exists " + table);
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testClientSideSortingOnBatchedChunk(String queryResultFormat) throws SQLException {
    // in this test, the first chunk contains multiple batches when the format is Arrow
    String[] queries = {
      "set-sf-property sort on",
      "alter session set populate_change_tracking_columns = true;",
      "alter session set create_change_tracking_columns = true;",
      "alter session set enable_stream = true;",
      "alter session set qa_mode=true;  -- used for row_id",
      "create or replace schema stream_get_table_timestamp;",
      "create or replace  table T(id int);",
      "create stream S on table T;",
      "select system$stream_get_table_timestamp('S');",
      "select system$last_change_commit_time('T');",
      "insert into T values (1);",
      "insert into T values (2);",
      "insert into T values (3);",
    };

    try (Connection conn = init(queryResultFormat);
        Statement stat = conn.createStatement()) {
      try {
        for (String q : queries) {
          stat.execute(q);
        }

        try (ResultSet rs = stat.executeQuery("select * from S")) {
          assertTrue(rs.next());
          assertEquals(1, rs.getInt(1));
          assertTrue(rs.next());
          assertEquals(2, rs.getInt(1));
          assertTrue(rs.next());
          assertEquals(3, rs.getInt(1));
          assertFalse(rs.next());
        }
      } finally {
        stat.execute("drop stream S");
        stat.execute("drop table T");
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testTimestampNTZAreAllNulls(String queryResultFormat) throws SQLException {
    try (Connection con = init(queryResultFormat);
        Statement statement = con.createStatement()) {
      try {
        statement.executeQuery(
            "create or replace table test_null_ts_ntz (a timestampntz(9)) as select null from table(generator"
                + "(rowcount => 1000000)) v "
                + "order by 1;");
        try (ResultSet rs = statement.executeQuery("select * from test_null_ts_ntz")) {
          while (rs.next()) {
            rs.getObject(1);
          }
        }
      } finally {
        statement.executeQuery("drop table if exists test_null_ts_ntz");
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void TestArrowStringRoundTrip(String queryResultFormat) throws SQLException {
    String big_number = "11111111112222222222333333333344444444";
    try (Connection con = init(queryResultFormat);
        Statement st = con.createStatement()) {
      try {
        for (int i = 0; i < 38; i++) {
          StringBuilder to_insert = new StringBuilder(big_number);
          if (i != 0) {
            int insert_to = 38 - i;
            to_insert.insert(insert_to, ".");
          }
          st.execute("create or replace table test_arrow_string (a NUMBER(38, " + i + ") )");
          st.execute("insert into test_arrow_string values (" + to_insert + ")");
          try (ResultSet rs = st.executeQuery("select * from test_arrow_string")) {
            assertTrue(rs.next());
            assertEquals(to_insert.toString(), rs.getString(1));
          }
        }
      } finally {
        st.execute("drop table if exists test_arrow_string");
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void TestArrowFloatRoundTrip(String queryResultFormat) throws SQLException {
    float[] cases = {Float.MAX_VALUE, Float.MIN_VALUE};
    try (Connection con = init(queryResultFormat);
        Statement st = con.createStatement()) {
      try {
        for (float f : cases) {
          st.executeQuery("create or replace table test_arrow_float (a FLOAT)");
          st.executeQuery("insert into test_arrow_float values (" + f + ")");
          try (ResultSet rs = st.executeQuery("select * from test_arrow_float")) {
            assertTrue(rs.next());
            assertEquals(f, rs.getFloat(1), Float.MIN_VALUE);
          }
        }
      } finally {
        st.executeQuery("drop table if exists test_arrow_float");
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  @DontRunOnGithubActions
  public void TestTimestampNTZWithDLS(String queryResultFormat) throws SQLException {
    TimeZone origTz = TimeZone.getDefault();
    String[] timeZones = new String[] {"America/New_York", "America/Los_Angeles"};
    try (Connection con = init(queryResultFormat);
        Statement st = con.createStatement()) {
      for (String timeZone : timeZones) {
        TimeZone.setDefault(TimeZone.getTimeZone(timeZone));
        st.execute("alter session set JDBC_USE_SESSION_TIMEZONE=false");
        st.execute("alter session set JDBC_TREAT_TIMESTAMP_NTZ_AS_UTC=true");
        st.execute("alter session set TIMEZONE='" + timeZone + "'");
        st.execute(
            "create or replace table src_ts(col1 TIMESTAMP_NTZ, col2 TIMESTAMP_LTZ, col3 TIMESTAMP_TZ)");
        List<String> testTimestampNTZValues =
            Arrays.asList(
                // DLS start in 2018
                "2018-03-11 01:10:34.0123456",
                "2018-03-11 02:10:34.0",
                "2018-03-11 03:10:34.0",
                // DLS end in 2018
                "2018-11-04 01:10:34.0",
                "2018-11-04 02:10:34.0",
                "2018-11-04 03:10:34.0",
                // DLS start in 2020
                "2020-03-11 01:10:34.0",
                "2020-03-11 02:10:34.0",
                "2020-03-11 03:10:34.0",
                // DLS end in 2020
                "2020-11-01 01:10:34.0",
                "2020-11-01 02:10:34.0",
                "2020-11-01 03:10:34.0");

        List<String[]> testTimestampLTZValues =
            Arrays.asList(
                // DLS start in 2018
                new String[] {"2018-03-11 01:10:34.0123456", "2018-03-11 01:10:34.0123456"},
                new String[] {
                  "2018-03-11 02:10:34.0", "2018-03-11 01:10:34.0"
                }, // only this has an impact
                new String[] {"2018-03-11 03:10:34.0", "2018-03-11 03:10:34.0"},
                // DLS end in 2018
                new String[] {"2018-11-04 01:10:34.0", "2018-11-04 01:10:34.0"},
                new String[] {"2018-11-04 02:10:34.0", "2018-11-04 02:10:34.0"},
                new String[] {"2018-11-04 03:10:34.0", "2018-11-04 03:10:34.0"},
                // DLS start in 2020
                new String[] {"2020-03-11 01:10:34.0", "2020-03-11 01:10:34.0"},
                new String[] {"2020-03-11 02:10:34.0", "2020-03-11 02:10:34.0"},
                new String[] {"2020-03-11 03:10:34.0", "2020-03-11 03:10:34.0"},
                // DLS end in 2020
                new String[] {"2020-11-01 01:10:34.0", "2020-11-01 01:10:34.0"},
                new String[] {"2020-11-01 02:10:34.0", "2020-11-01 02:10:34.0"},
                new String[] {"2020-11-01 03:10:34.0", "2020-11-01 03:10:34.0"});
        List<String> testTimestampTZValues =
            Arrays.asList(
                // DLS start in 2018
                "2018-03-11 01:10:34.0 +0200",
                "2018-03-11 02:10:34.0 +0200",
                "2018-03-11 03:10:34.0 +0200",
                // DLS end in 2018
                "2018-11-04 01:10:34.0 +0200",
                "2018-11-04 02:10:34.0 +0200",
                "2018-11-04 03:10:34.0 +0200",
                // DLS start in 2020
                "2020-03-11 01:10:34.0 +0200",
                "2020-03-11 02:10:34.0 +0200",
                "2020-03-11 03:10:34.0 +0200",
                // DLS end in 2020
                "2020-11-01 01:10:34.0 +0200",
                "2020-11-01 02:10:34.0 +0200",
                "2020-11-01 03:10:34.0 +0200");

        for (int i = 0; i < testTimestampNTZValues.size(); i++) {
          st.execute(
              "insert into src_ts(col1,col2,col3) values('"
                  + testTimestampNTZValues.get(i)
                  + "', '"
                  + testTimestampLTZValues.get(i)[0]
                  + "', '"
                  + testTimestampTZValues.get(i)
                  + "')");
        }

        try (ResultSet resultSet = st.executeQuery("select col1, col2, col3 from src_ts")) {
          int j = 0;
          while (resultSet.next()) {
            Object data1 = resultSet.getObject(1);
            assertEquals(testTimestampNTZValues.get(j), data1.toString());

            Object data2 = resultSet.getObject(2);
            assertEquals(testTimestampLTZValues.get(j)[1], data2.toString());

            Object data3 = resultSet.getObject(3);
            assertThat(data3, instanceOf(Timestamp.class));
            assertEquals(
                parseTimestampTZ(testTimestampTZValues.get(j)).toEpochSecond(),
                ((Timestamp) data3).getTime() / 1000);
            j++;
          }
        }
      }
    } finally {
      TimeZone.setDefault(origTz);
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void TestTimestampNTZBinding(String queryResultFormat) throws SQLException {
    TimeZone origTz = TimeZone.getDefault();
    try (Connection con = init(queryResultFormat)) {
      TimeZone.setDefault(TimeZone.getTimeZone("PST"));
      try (Statement st = con.createStatement()) {
        st.execute("alter session set CLIENT_TIMESTAMP_TYPE_MAPPING=TIMESTAMP_NTZ");
        st.execute("alter session set JDBC_TREAT_TIMESTAMP_NTZ_AS_UTC=true");
        st.execute("create or replace table src_ts(col1 TIMESTAMP_NTZ)");
        try (PreparedStatement prepst = con.prepareStatement("insert into src_ts values(?)")) {
          Timestamp tz = Timestamp.valueOf("2018-03-11 01:10:34.0");
          prepst.setTimestamp(1, tz);
          prepst.execute();
        }
        try (ResultSet resultSet = st.executeQuery("SELECT COL1 FROM SRC_TS")) {
          Object data;
          int i = 1;
          while (resultSet.next()) {
            data = resultSet.getObject(i);
            System.out.println(data.toString());
          }
        }
      }
    }
    TimeZone.setDefault(origTz);
  }
}
