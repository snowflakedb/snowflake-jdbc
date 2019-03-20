/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.jdbc;

import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class StatementAlreadyClosedIT extends BaseJDBCTest
{
  private void expectAlreadyClosedException(MethodRaisesSQLException f)
  {
    try
    {
      f.run();
      fail("must raise exception");
    }
    catch (SQLException ex)
    {
      assertStatementClosedError(ex);
    }
  }

  @Test
  public void testStatementAlreadyClosed() throws Throwable
  {
    try (Connection connection = getConnection())
    {
      Statement statement = connection.createStatement();
      statement.close();

      expectAlreadyClosedException(() -> statement.execute("select 1", Statement.NO_GENERATED_KEYS));
      expectAlreadyClosedException(statement::executeBatch);
      expectAlreadyClosedException(() -> statement.executeQuery("select 1"));
      expectAlreadyClosedException(() -> statement.executeUpdate("update t set c1=1"));
      expectAlreadyClosedException(() -> statement.execute("update t set c1=1"));
      expectAlreadyClosedException(statement::getConnection);
      expectAlreadyClosedException(statement::getFetchDirection);
      expectAlreadyClosedException(statement::getFetchSize);
      expectAlreadyClosedException(statement::getMoreResults);
      expectAlreadyClosedException(() -> statement.getMoreResults(0));
      expectAlreadyClosedException(statement::getQueryTimeout);
      expectAlreadyClosedException(statement::getResultSet);
      expectAlreadyClosedException(statement::getResultSetConcurrency);
      expectAlreadyClosedException(statement::getResultSetHoldability);
      expectAlreadyClosedException(statement::getResultSetType);
      expectAlreadyClosedException(statement::getUpdateCount);
      expectAlreadyClosedException(statement::getWarnings);
      expectAlreadyClosedException(() -> statement.setEscapeProcessing(true));
      expectAlreadyClosedException(() -> statement.setFetchDirection(ResultSet.FETCH_FORWARD));
      expectAlreadyClosedException(() -> statement.setFetchSize(10));
      expectAlreadyClosedException(() -> statement.setMaxRows(10));
      expectAlreadyClosedException(statement::isPoolable);
      expectAlreadyClosedException(() -> statement.setPoolable(false));
      expectAlreadyClosedException(() -> statement.setQueryTimeout(10));
      expectAlreadyClosedException(statement::cancel);
      expectAlreadyClosedException(statement::clearWarnings);
      expectAlreadyClosedException(() -> statement.addBatch("select 2"));
      expectAlreadyClosedException(statement::clearBatch);
    }
  }

  private void assertStatementClosedError(SQLException ex)
  {
    assertEquals((int) ErrorCode.STATEMENT_CLOSED.getMessageCode(), ex.getErrorCode());
  }
}
