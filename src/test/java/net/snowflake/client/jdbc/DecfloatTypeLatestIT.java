package net.snowflake.client.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.providers.SimpleResultFormatProvider;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

@Tag(TestTags.STATEMENT)
public class DecfloatTypeLatestIT extends BaseJDBCTest {

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testDecfloatToBigDecimal(String queryResultFormat) throws SQLException {
    try (Connection con = getConnection()) {
      try (Statement stmt = createStatement(con, queryResultFormat)) {
        ResultSet rs = stmt.executeQuery("SELECT 123.456::DECFLOAT, NULL::DECFLOAT");
        assertTrue(rs.next());

        // Test non-null value
        BigDecimal value = rs.getBigDecimal(1);
        assertEquals(new BigDecimal("123.456"), value);

        // Test null value
        BigDecimal nullValue = rs.getBigDecimal(2);
        assertNull(nullValue);
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testDecfloatToDouble(String queryResultFormat) throws SQLException {
    try (Connection con = getConnection()) {
      try (Statement stmt = createStatement(con, queryResultFormat)) {
        ResultSet rs = stmt.executeQuery("SELECT 123.456::DECFLOAT, NULL::DECFLOAT");
        assertTrue(rs.next());

        // Test non-null value
        double value = rs.getDouble(1);
        assertEquals(123.456, value, 0.001);

        // Test null value (returns 0.0)
        double nullValue = rs.getDouble(2);
        assertEquals(0.0, nullValue, 0.001);
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testDecfloatToFloat(String queryResultFormat) throws SQLException {
    try (Connection con = getConnection()) {
      try (Statement stmt = createStatement(con, queryResultFormat)) {
        ResultSet rs = stmt.executeQuery("SELECT 123.456::DECFLOAT, NULL::DECFLOAT");
        assertTrue(rs.next());

        // Test non-null value
        float value = rs.getFloat(1);
        assertEquals(123.456f, value, 0.001f);

        // Test null value (returns 0.0)
        float nullValue = rs.getFloat(2);
        assertEquals(0.0f, nullValue, 0.001f);
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testDecfloatToString(String queryResultFormat) throws SQLException {
    try (Connection con = getConnection()) {
      try (Statement stmt = createStatement(con, queryResultFormat)) {
        ResultSet rs = stmt.executeQuery("SELECT 123.456::DECFLOAT, NULL::DECFLOAT");
        assertTrue(rs.next());

        // Test non-null value
        String value = rs.getString(1);
        assertEquals("123.456", value);

        // Test null value
        String nullValue = rs.getString(2);
        assertNull(nullValue);
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testDecfloatToObject(String queryResultFormat) throws SQLException {
    try (Connection con = getConnection()) {
      try (Statement stmt = createStatement(con, queryResultFormat)) {
        ResultSet rs = stmt.executeQuery("SELECT 123.456::DECFLOAT, NULL::DECFLOAT");
        assertTrue(rs.next());

        // Test non-null value
        Object value = rs.getObject(1);
        assertEquals(new BigDecimal("123.456"), value);

        // Test null value
        Object nullValue = rs.getObject(2);
        assertNull(nullValue);
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testDecfloatIntegerConversions(String queryResultFormat) throws SQLException {
    try (Connection con = getConnection()) {
      try (Statement stmt = createStatement(con, queryResultFormat)) {

        // Test regular integer values without fractions
        final ResultSet r = stmt.executeQuery("SELECT 123::DECFLOAT");
        assertTrue(r.next());

        assertEquals(123, r.getInt(1));
        assertEquals(123L, r.getLong(1));
        assertEquals((short) 123, r.getShort(1));

        // Test overflow scenarios with large values that fit in Long but overflow int/short
        final ResultSet rs =
            stmt.executeQuery("SELECT 2147483648::DECFLOAT"); // 2^31, exceeds int max value
        assertTrue(rs.next());

        // Long conversion should succeed
        assertEquals(2147483648L, rs.getLong(1));

        // Int conversion should throw exception (overflow)
        SnowflakeSQLException e =
            assertThrows(
                SnowflakeSQLException.class,
                () -> rs.getInt(1),
                "Expected SnowflakeSQLException for getInt overflow");
        assertEquals(
            ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(),
            e.getErrorCode(),
            "Expected error code INVALID_VALUE_CONVERT for getInt overflow");

        // Short conversion should throw exception (overflow)
        e =
            assertThrows(
                SnowflakeSQLException.class,
                () -> rs.getShort(1),
                "Expected SnowflakeSQLException for getShort overflow");
        assertEquals(
            ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(),
            e.getErrorCode(),
            "Expected error code INVALID_VALUE_CONVERT for getShort overflow");

        // Test overflow scenarios with values that exceed Long.MAX_VALUE
        final ResultSet rsLongOverflow =
            stmt.executeQuery(
                "SELECT 9223372036854775808::DECFLOAT"); // 2^63, exceeds long max value
        assertTrue(rsLongOverflow.next());

        // Long conversion should throw exception (overflow)
        e =
            assertThrows(
                SnowflakeSQLException.class,
                () -> rsLongOverflow.getLong(1),
                "Expected SnowflakeSQLException for getLong overflow");
        assertEquals(
            ErrorCode.INVALID_VALUE_CONVERT.getMessageCode(),
            e.getErrorCode(),
            "Expected error code INVALID_VALUE_CONVERT for getLong overflow");
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testDecfloatBigDecimalConversions(String queryResultFormat) throws SQLException {
    try (Connection con = getConnection()) {
      try (Statement stmt = createStatement(con, queryResultFormat)) {

        // Test various DECFLOAT values with precise BigDecimal conversions
        String[] testValues = {
          "0",
          "-1",
          "-1.5",
          "123.4567",
          "-1.2345e2",
          "1.23456e2",
          "-9.8765432099999998623226732747455716901E-250",
          "1.2345678901234567890123456789012345678e37"
        };

        BigDecimal[] expectedValues = {
          new BigDecimal("0"),
          new BigDecimal("-1"),
          new BigDecimal("-1.5"),
          new BigDecimal("123.4567"),
          new BigDecimal("-123.45"), // -1.2345e2 = -123.45 (decimal notation)
          new BigDecimal("123.456"), // 1.23456e2 = 123.456 (decimal notation)
          new BigDecimal(
              "-9.8765432099999998623226732747455716901E-250"), // Very small negative number with
          // precision
          new BigDecimal("1.2345678901234567890123456789012345678E+37") // Very large number
        };

        for (int i = 0; i < testValues.length; i++) {
          String query = "SELECT " + testValues[i] + "::DECFLOAT";
          ResultSet rs = stmt.executeQuery(query);
          assertTrue(rs.next(), "Failed to get result for: " + testValues[i]);

          BigDecimal actual = rs.getBigDecimal(1);
          BigDecimal expected = expectedValues[i];

          assertEquals(expected, actual, "Failed for value: " + testValues[i]);
        }
      }
    }
  }

  private Statement createStatement(Connection connection, String queryResultFormat)
      throws SQLException {
    Statement stmt = connection.createStatement();
    stmt.execute("ALTER SESSION SET JDBC_QUERY_RESULT_FORMAT = '" + queryResultFormat + "'");
    stmt.execute("ALTER SESSION SET FEATURE_DECFLOAT = enabled");
    stmt.execute("ALTER SESSION SET DECFLOAT_RESULT_COLUMN_TYPE = 2");
    return stmt;
  }
}
