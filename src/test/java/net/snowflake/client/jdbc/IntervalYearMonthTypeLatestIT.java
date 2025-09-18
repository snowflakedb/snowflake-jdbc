package net.snowflake.client.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
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
        ResultSet rs =
            stmt.executeQuery("SELECT '1-2'::INTERVAL YEAR TO MONTH, NULL::INTERVAL YEAR TO MONTH");
        assertTrue(rs.next());

        // Test Period conversions
        Period periodValue = rs.getObject(1, Period.class);
        assertEquals(Period.ofMonths(14), periodValue);
        Period nullPeriod = rs.getObject(2, Period.class);
        assertNull(nullPeriod);
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
