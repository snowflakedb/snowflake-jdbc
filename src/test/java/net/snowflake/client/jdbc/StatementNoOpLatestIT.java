package net.snowflake.client.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.STATEMENT)
public class StatementNoOpLatestIT {
  @Test
  public void testSnowflakeNoOpStatement() throws SQLException {
    SnowflakeStatementV1.NoOpSnowflakeStatementV1 statement =
        new SnowflakeStatementV1.NoOpSnowflakeStatementV1();
    expectSQLException((() -> statement.executeQuery("select 1")));
    expectSQLException((() -> statement.executeUpdate("insert into a values(1)")));
    expectSQLException((() -> statement.executeLargeUpdate("insert into a values(1)")));
    expectSQLException((() -> statement.execute("select 1")));
    expectSQLException((() -> statement.execute("select 1", Statement.RETURN_GENERATED_KEYS)));
    expectSQLException((() -> statement.execute("select 1", new int[] {})));
    expectSQLException((() -> statement.execute("select 1", new String[] {})));
    expectSQLException((statement::executeBatch));
    expectSQLException((statement::executeLargeBatch));
    expectSQLException(
        (() ->
            statement.executeUpdate("insert into a values(1)", Statement.RETURN_GENERATED_KEYS)));
    expectSQLException(
        (() ->
            statement.executeLargeUpdate(
                "insert into a values(1)", Statement.RETURN_GENERATED_KEYS)));
    expectSQLException((() -> statement.executeUpdate("insert into a values(1)", new int[] {})));
    expectSQLException(
        (() -> statement.executeLargeUpdate("insert into a values(1)", new int[] {})));
    expectSQLException((() -> statement.executeUpdate("insert into a values(1)", new String[] {})));
    expectSQLException(
        (() -> statement.executeLargeUpdate("insert into a values(1)", new String[] {})));
    expectSQLException((statement::getFetchDirection));
    expectSQLException((statement::getFetchSize));
    expectSQLException((statement::getGeneratedKeys));
    expectSQLException((statement::getMaxFieldSize));
    expectSQLException((statement::getMaxRows));
    expectSQLException((statement::getMoreResults));
    expectSQLException((() -> statement.getMoreResults(1)));
    expectSQLException((statement::getQueryTimeout));
    expectSQLException((statement::getResultSet));
    expectSQLException((statement::getResultSetConcurrency));
    expectSQLException((statement::getResultSetHoldability));
    expectSQLException((statement::getResultSetType));
    expectSQLException((statement::getUpdateCount));
    expectSQLException((statement::getLargeUpdateCount));
    expectSQLException((statement::getWarnings));
    expectSQLException((statement::isClosed));
    expectSQLException((statement::isPoolable));
    expectSQLException((statement::isCloseOnCompletion));
    expectSQLException((statement::getUpdateCount));
    expectSQLException((statement::getConnection));
  }

  @Test
  public void testGetQueryID() throws SQLException {
    SnowflakeStatementV1.NoOpSnowflakeStatementV1 statement =
        new SnowflakeStatementV1.NoOpSnowflakeStatementV1();
    assertEquals("invalid_query_id", statement.getQueryID());
    assertEquals(new ArrayList<>(), statement.getBatchQueryIDs());
  }

  @Test
  public void testSetNoOp() throws SQLException {
    SnowflakeStatementV1.NoOpSnowflakeStatementV1 statement =
        new SnowflakeStatementV1.NoOpSnowflakeStatementV1();
    expectNoOp(() -> statement.setMaxRows(2));
    expectNoOp(() -> statement.setCursorName("a"));
    expectNoOp(() -> statement.setEscapeProcessing(false));
    expectNoOp(() -> statement.setFetchDirection(2));
    expectNoOp(() -> statement.setFetchSize(2));
    expectNoOp(() -> statement.setMaxFieldSize(2));
    expectNoOp(() -> statement.setPoolable(false));
    expectNoOp(() -> statement.setParameter("ab", 3));
    expectNoOp(() -> statement.setQueryTimeout(2));
    expectNoOp(() -> statement.setMaxRows(2));
    expectNoOp(() -> statement.setMaxRows(2));
    expectNoOp(() -> statement.addBatch("select 1"));
    expectNoOp(() -> statement.close(false));
    expectNoOp(statement::close);
    expectNoOp(statement::closeOnCompletion);
    expectNoOp(statement::cancel);
    expectNoOp(statement::clearWarnings);
    expectNoOp(statement::clearBatch);
  }

  protected void expectSQLException(BaseJDBCTest.MethodRaisesSQLException f) {
    assertThrows(SQLException.class, f::run);
  }

  protected void expectNoOp(NoOpMethod f) throws SQLException {
    f.run();
  }

  protected interface NoOpMethod {
    void run() throws SQLException;
  }
}
