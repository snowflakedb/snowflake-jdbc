package net.snowflake.client.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import net.snowflake.client.annotations.DontRunOnGithubActions;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.providers.SimpleResultFormatProvider;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

@Tag(TestTags.STATEMENT)
public class DecfloatTypeLatestIT extends BaseJDBCTest {

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testBasicDecfloatConversions(String queryResultFormat) throws SQLException {
    try (Connection con = getConnection()) {
      try (Statement stmt = createStatement(con, queryResultFormat)) {
        ResultSet rs = stmt.executeQuery("SELECT 123.456::DECFLOAT, NULL::DECFLOAT");
        assertTrue(rs.next());

        // Test BigDecimal conversions
        BigDecimal bigDecimalValue = rs.getBigDecimal(1);
        assertEquals(new BigDecimal("123.456"), bigDecimalValue);
        BigDecimal nullBigDecimal = rs.getBigDecimal(2);
        assertNull(nullBigDecimal);

        // Test Double conversions
        double doubleValue = rs.getDouble(1);
        assertEquals(123.456, doubleValue, 0.001);
        double nullDouble = rs.getDouble(2);
        assertEquals(0.0, nullDouble, 0.001);

        // Test Float conversions
        float floatValue = rs.getFloat(1);
        assertEquals(123.456f, floatValue, 0.001f);
        float nullFloat = rs.getFloat(2);
        assertEquals(0.0f, nullFloat, 0.001f);

        // Test String conversions
        String stringValue = rs.getString(1);
        assertEquals("123.456", stringValue);
        String nullString = rs.getString(2);
        assertNull(nullString);

        // Test Object conversions
        Object objectValue = rs.getObject(1);
        assertEquals(new BigDecimal("123.456"), objectValue);
        Object nullObject = rs.getObject(2);
        assertNull(nullObject);
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
          new BigDecimal("-123.45"),
          new BigDecimal("123.456"),
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

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testDecfloatBindingBasicTypes(String queryResultFormat) throws SQLException {
    try (Connection con = getConnection()) {
      try (Statement ignored = createStatement(con, queryResultFormat)) {
        try (PreparedStatement ps =
            con.prepareStatement(
                "SELECT ?::DECFLOAT, ?::DECFLOAT, ?::DECFLOAT, ?::DECFLOAT, ?::DECFLOAT, ?::DECFLOAT, ?::DECFLOAT, ?::DECFLOAT, ?::DECFLOAT")) {

          ps.setBigDecimal(1, new BigDecimal("1234567890.1234567890123456789012345678"));
          ps.setDouble(2, 123.45);
          ps.setString(3, "678.9");
          ps.setInt(4, 1);
          ps.setFloat(5, 2.5f);
          ps.setLong(6, 123456789L);
          ps.setShort(7, (short) 45);
          ps.setObject(8, new BigDecimal("1.2345e4"), SnowflakeUtil.EXTRA_TYPES_DECFLOAT);
          ps.setNull(9, SnowflakeUtil.EXTRA_TYPES_DECFLOAT);

          try (ResultSet rs = ps.executeQuery()) {
            assertTrue(rs.next());
            assertEquals(
                new BigDecimal("1234567890.1234567890123456789012345678"), rs.getBigDecimal(1));
            assertEquals(123.45, rs.getDouble(2), 0.001);
            assertEquals("678.9", rs.getString(3));
            assertEquals(1, rs.getInt(4));
            assertEquals(2.5f, rs.getFloat(5), 0.001f);
            assertEquals(123456789L, rs.getLong(6));
            assertEquals((short) 45, rs.getShort(7));
            assertEquals(new BigDecimal("1.2345e4"), rs.getObject(8));
            assertNull(rs.getObject(9));
          }
        }
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testDecfloatBindingExtremeValues(String queryResultFormat) throws SQLException {
    try (Connection con = getConnection()) {
      try (Statement ignored = createStatement(con, queryResultFormat)) {
        try (PreparedStatement ps =
            con.prepareStatement("SELECT ?::DECFLOAT, ?::DECFLOAT, ?::DECFLOAT")) {

          BigDecimal veryLargeValue = new BigDecimal("1.2345678901234567890123456789012345678e120");
          BigDecimal verySmallValue =
              new BigDecimal("-9.8765432099999998623226732747455716901E-250");
          BigDecimal expValue = new BigDecimal("-1.2345e2"); // -123.45

          ps.setObject(1, veryLargeValue, SnowflakeUtil.EXTRA_TYPES_DECFLOAT);
          ps.setObject(2, verySmallValue, SnowflakeUtil.EXTRA_TYPES_DECFLOAT);
          ps.setObject(3, expValue, SnowflakeUtil.EXTRA_TYPES_DECFLOAT);

          try (ResultSet rs = ps.executeQuery()) {
            assertTrue(rs.next());
            BigDecimal result1 = rs.getBigDecimal(1);
            BigDecimal result2 = rs.getBigDecimal(2);
            BigDecimal result3 = rs.getBigDecimal(3);

            assertEquals(veryLargeValue, result1);
            assertEquals(verySmallValue, result2);
            assertEquals(expValue, result3);
          }
        }
      }
    }
  }

  @ArgumentsSource(SimpleResultFormatProvider.class)
  @ParameterizedTest
  public void testDecfloatBindingArray(String queryResultString) throws SQLException {
    try (Connection con = getConnection()) {
      try (Statement stmt = createStatement(con, queryResultString)) {
        try {
          stmt.execute("CREATE OR REPLACE TABLE test_decfloat (value DECFLOAT)");

          List<BigDecimal> firstArray =
              Arrays.asList(
                  new BigDecimal("123.45"),
                  new BigDecimal("1234567890.1234567890123456789012345678"),
                  new BigDecimal("1.2345678901234567890123456789012345678e120"));

          try (PreparedStatement ps =
              con.prepareStatement("INSERT INTO test_decfloat VALUES (?)")) {
            for (BigDecimal value : firstArray) {
              ps.setObject(1, value, SnowflakeUtil.EXTRA_TYPES_DECFLOAT);
              ps.addBatch();
            }
            ps.executeBatch();
          }

          try (ResultSet rs =
              stmt.executeQuery("SELECT * FROM test_decfloat order by value desc")) {
            assertTrue(rs.next());
            BigDecimal firstValue = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("1.2345678901234567890123456789012345678e120"), firstValue);

            assertTrue(rs.next());
            BigDecimal secondValue = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("1234567890.1234567890123456789012345678"), secondValue);

            assertTrue(rs.next());
            BigDecimal thirdValue = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("123.45"), thirdValue);
          }
        } finally {
          // Cleanup
          stmt.execute("DROP TABLE IF EXISTS test_decfloat");
        }
      }
    }
  }

  @DontRunOnGithubActions
  @ArgumentsSource(SimpleResultFormatProvider.class)
  @ParameterizedTest
  public void testDecfloatBindingBatchInserts(String queryResultString) throws SQLException {
    try (Connection con = getConnection()) {
      try (Statement stmt = createStatement(con, queryResultString)) {
        try {
          stmt.execute("CREATE OR REPLACE TABLE test_decfloat (value DECFLOAT)");

          List<BigDecimal> secondArray =
              Arrays.asList(
                  new BigDecimal("-987.45e-4"),
                  new BigDecimal("-1234.423e3"),
                  new BigDecimal("-9.8765432099999998623226732747455716901E-250"));

          stmt.execute("ALTER SESSION SET CLIENT_STAGE_ARRAY_BINDING_THRESHOLD = 1");
          try (PreparedStatement ps =
              con.prepareStatement("INSERT INTO test_decfloat VALUES (?)")) {
            for (BigDecimal value : secondArray) {
              ps.setObject(1, value, SnowflakeUtil.EXTRA_TYPES_DECFLOAT);
              ps.addBatch();
            }
            ps.executeBatch();
          }

          try (ResultSet rs =
              stmt.executeQuery("SELECT * FROM test_decfloat order by value desc")) {
            assertTrue(rs.next());
            BigDecimal firstValue = rs.getBigDecimal(1);
            assertEquals(
                new BigDecimal("-9.8765432099999998623226732747455716901E-250"), firstValue);

            assertTrue(rs.next());
            BigDecimal fourthValue = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("-0.098745"), fourthValue);

            assertTrue(rs.next());
            BigDecimal fifthValue = rs.getBigDecimal(1);
            assertEquals(new BigDecimal("-1234.423e3"), fifthValue);
          }
        } finally {
          // Cleanup
          stmt.execute("DROP TABLE IF EXISTS test_decfloat");
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
