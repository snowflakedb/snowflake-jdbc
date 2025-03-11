package net.snowflake.client.jdbc;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.STATEMENT)
public class StatementAlreadyClosedIT extends BaseJDBCTest {
  @Test
  public void testStatementAlreadyClosed() throws Throwable {
    try (Connection connection = getConnection()) {
      Statement statement = connection.createStatement();
      assertFalse(statement.isClosed());
      statement.close();
      assertTrue(statement.isClosed());

      expectStatementAlreadyClosedException(
          () -> statement.execute("select 1", Statement.NO_GENERATED_KEYS));
      expectStatementAlreadyClosedException(statement::executeBatch);
      expectStatementAlreadyClosedException(() -> statement.executeQuery("select 1"));
      expectStatementAlreadyClosedException(() -> statement.executeUpdate("update t set c1=1"));
      expectStatementAlreadyClosedException(() -> statement.execute("update t set c1=1"));
      expectStatementAlreadyClosedException(statement::getConnection);
      expectStatementAlreadyClosedException(statement::getFetchDirection);
      expectStatementAlreadyClosedException(statement::getFetchSize);
      expectStatementAlreadyClosedException(statement::getMoreResults);
      expectStatementAlreadyClosedException(() -> statement.getMoreResults(0));
      expectStatementAlreadyClosedException(statement::getQueryTimeout);
      expectStatementAlreadyClosedException(statement::getResultSet);
      expectStatementAlreadyClosedException(statement::getResultSetConcurrency);
      expectStatementAlreadyClosedException(statement::getResultSetHoldability);
      expectStatementAlreadyClosedException(statement::getResultSetType);
      expectStatementAlreadyClosedException(statement::getUpdateCount);
      expectStatementAlreadyClosedException(statement::getWarnings);
      expectStatementAlreadyClosedException(() -> statement.setEscapeProcessing(true));
      expectStatementAlreadyClosedException(
          () -> statement.setFetchDirection(ResultSet.FETCH_FORWARD));
      expectStatementAlreadyClosedException(() -> statement.setFetchSize(10));
      expectStatementAlreadyClosedException(() -> statement.setMaxRows(10));
      expectStatementAlreadyClosedException(statement::isPoolable);
      expectStatementAlreadyClosedException(() -> statement.setPoolable(false));
      expectStatementAlreadyClosedException(() -> statement.setQueryTimeout(10));
      expectStatementAlreadyClosedException(statement::cancel);
      expectStatementAlreadyClosedException(statement::clearWarnings);
      expectStatementAlreadyClosedException(() -> statement.addBatch("select 2"));
      expectStatementAlreadyClosedException(statement::clearBatch);
    }
  }
}
