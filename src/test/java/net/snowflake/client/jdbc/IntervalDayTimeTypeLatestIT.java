package net.snowflake.client.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
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
public class IntervalDayTimeTypeLatestIT extends BaseJDBCTest {

    @ParameterizedTest
    @ArgumentsSource(SimpleResultFormatProvider.class)
    public void testIntervalDayTimeConversions(String queryResultFormat) throws SQLException {
        try (Connection con = getConnection()) {
            try (Statement stmt = createStatement(con, queryResultFormat)) {
                ResultSet rs = stmt.executeQuery("SELECT '0 0:0:0.1'::INTERVAL DAY TO SECOND, NULL::INTERVAL DAY TO SECOND");
                assertTrue(rs.next());

                // Test Duration conversions
                Duration durationValue = rs.getObject(1, Duration.class);
                assertEquals(Duration.ofNanos(1_000_000_000), durationValue);
                Duration nullDuration = rs.getObject(2, Duration.class);
                assertNull(nullDuration);
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