package net.snowflake.client.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Period;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.providers.SimpleResultFormatProvider;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

@Tag(TestTags.STATEMENT)
public class IntervalYearMonthTypeLatestIT extends BaseJDBCTest {

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testIntervalYearMonthConversions(String queryResultFormat) throws SQLException {
    try (Connection con = getConnection()) {
      try (Statement stmt = createStatement(con, queryResultFormat)) {
        // Test Period conversions with Interval Year-Month SB8
        ResultSet rsSB8 =
            stmt.executeQuery(
                "SELECT '999999999-11'::INTERVAL YEAR TO MONTH, '-999999999-11'::INTERVAL YEAR TO MONTH, NULL::INTERVAL YEAR TO MONTH");
        assertTrue(rsSB8.next());

        Period periodValueMaxSB8 = rsSB8.getObject(1, Period.class);
        assertEquals(Period.of(999999999, 11, 0), periodValueMaxSB8);
        Period periodValueMinSB8 = rsSB8.getObject(2, Period.class);
        assertEquals(Period.of(-999999999, -11, 0), periodValueMinSB8);
        Period nullPeriodSB8 = rsSB8.getObject(3, Period.class);
        assertNull(nullPeriodSB8);

        // Test Period conversions with Interval Year-Month SB4
        ResultSet rsSB4 =
            stmt.executeQuery(
                "SELECT '1-2'::INTERVAL YEAR(7) TO MONTH, NULL::INTERVAL YEAR(7) TO MONTH");
        assertTrue(rsSB4.next());

        Period periodValueSB4 = rsSB4.getObject(1, Period.class);
        assertEquals(Period.of(1, 2, 0), periodValueSB4);
        Period nullPeriodSB4 = rsSB4.getObject(2, Period.class);
        assertNull(nullPeriodSB4);

        // Test Period conversions with Interval Year-Month SB2
        ResultSet rsSB2 =
            stmt.executeQuery(
                "SELECT '1-2'::INTERVAL YEAR(2) TO MONTH, NULL::INTERVAL YEAR(2) TO MONTH");
        assertTrue(rsSB2.next());

        Period periodValueSB2 = rsSB2.getObject(1, Period.class);
        assertEquals(Period.of(1, 2, 0), periodValueSB2);
        Period nullPeriodSB2 = rsSB2.getObject(2, Period.class);
        assertNull(nullPeriodSB2);
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testIntervalYearMonthBindingBasicTypes(String queryResultFormat) throws SQLException {
    try (Connection con = getConnection()) {
      try (Statement ignored = createStatement(con, queryResultFormat)) {
        try (PreparedStatement ps =
            con.prepareStatement(
                "SELECT ?::INTERVAL YEAR TO MONTH, ?::INTERVAL YEAR TO MONTH, ?::INTERVAL YEAR, ?::INTERVAL MONTH, ?::INTERVAL YEAR TO MONTH")) {

          ps.setObject(1, "999999999-11", SnowflakeUtil.EXTRA_TYPES_YEAR_MONTH_INTERVAL);
          ps.setObject(2, "-999999999-11", SnowflakeUtil.EXTRA_TYPES_YEAR_MONTH_INTERVAL);
          ps.setObject(3, "2", SnowflakeUtil.EXTRA_TYPES_YEAR_MONTH_INTERVAL);
          ps.setObject(4, "5", SnowflakeUtil.EXTRA_TYPES_YEAR_MONTH_INTERVAL);
          ps.setNull(5, SnowflakeUtil.EXTRA_TYPES_YEAR_MONTH_INTERVAL);

          try (ResultSet rs = ps.executeQuery()) {
            assertTrue(rs.next());
            assertEquals(Period.of(999999999, 11, 0), rs.getObject(1, Period.class));
            assertEquals(Period.of(-999999999, -11, 0), rs.getObject(2, Period.class));
            assertEquals(Period.of(2, 0, 0), rs.getObject(3, Period.class));
            assertEquals(Period.of(0, 5, 0), rs.getObject(4, Period.class));
            assertNull(rs.getObject(5));
          }
        }
      }
    }
  }

  private Statement createStatement(Connection connection, String queryResultFormat)
      throws SQLException {
    Statement stmt = connection.createStatement();
    stmt.execute("ALTER SESSION SET JDBC_QUERY_RESULT_FORMAT = '" + queryResultFormat + "'");
    stmt.execute("ALTER SESSION SET FEATURE_INTERVAL_TYPES = enabled");
    return stmt;
  }
}
