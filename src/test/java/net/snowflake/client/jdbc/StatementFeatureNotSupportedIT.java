package net.snowflake.client.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.STATEMENT)
public class StatementFeatureNotSupportedIT extends BaseJDBCTest {
  @Test
  public void testFeatureNotSupportedException() throws Throwable {
    try (Connection connection = getConnection()) {
      try (Statement statement = connection.createStatement()) {
        expectFeatureNotSupportedException(() -> statement.execute("select 1", new int[] {}));
        expectFeatureNotSupportedException(() -> statement.execute("select 1", new String[] {}));
        expectFeatureNotSupportedException(
            () ->
                statement.executeUpdate(
                    "insert into a values(1)", Statement.RETURN_GENERATED_KEYS));
        expectFeatureNotSupportedException(
            () -> statement.executeUpdate("insert into a values(1)", new int[] {}));
        expectFeatureNotSupportedException(
            () -> statement.executeUpdate("insert into a values(1)", new String[] {}));
        expectFeatureNotSupportedException(
            () ->
                statement.executeLargeUpdate(
                    "insert into a values(1)", Statement.RETURN_GENERATED_KEYS));
        expectFeatureNotSupportedException(
            () -> statement.executeLargeUpdate("insert into a values(1)", new int[] {}));
        expectFeatureNotSupportedException(
            () -> statement.executeLargeUpdate("insert into a values(1)", new String[] {}));
        expectFeatureNotSupportedException(() -> statement.setCursorName("curname"));
        expectFeatureNotSupportedException(
            () -> statement.setFetchDirection(ResultSet.FETCH_REVERSE));
        expectFeatureNotSupportedException(
            () -> statement.setFetchDirection(ResultSet.FETCH_UNKNOWN));
        expectFeatureNotSupportedException(() -> statement.setMaxFieldSize(10));
        expectFeatureNotSupportedException(statement::closeOnCompletion);
        expectFeatureNotSupportedException(statement::isCloseOnCompletion);
      }
    }
  }
}
