package net.snowflake.client.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.providers.SimpleResultFormatProvider;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

@Tag(TestTags.STATEMENT)
public class IntervalDayTimeTypeLatestIT extends BaseJDBCTest {

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testIntervalDayTimeConversions(String queryResultFormat) throws SQLException {
    try (Connection con = getConnection()) {
      try (Statement stmt = createStatement(con, queryResultFormat)) {
        // Test Duration conversions with Interval Day-Time SB16
        ResultSet rsSB16 =
            stmt.executeQuery(
                "SELECT '0 0:0:0.1'::INTERVAL DAY TO SECOND, NULL::INTERVAL DAY TO SECOND");
        assertTrue(rsSB16.next());

        Duration durationValueSB16 = rsSB16.getObject(1, Duration.class);
        assertEquals(Duration.ofNanos(100_000_000), durationValueSB16);
        Duration nullDurationSB16 = rsSB16.getObject(2, Duration.class);
        assertNull(nullDurationSB16);

        // Test Duration conversions with Interval Day-Time SB8
        ResultSet rsSB8 =
            stmt.executeQuery(
                "SELECT '0 0:0:0.1'::INTERVAL DAY(6) TO SECOND, NULL::INTERVAL DAY(6) TO SECOND");
        assertTrue(rsSB8.next());

        Duration durationValueSB8 = rsSB8.getObject(1, Duration.class);
        assertEquals(Duration.ofNanos(100_000_000), durationValueSB8);
        Duration nullDurationSB8 = rsSB8.getObject(2, Duration.class);
        assertNull(nullDurationSB8);
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
