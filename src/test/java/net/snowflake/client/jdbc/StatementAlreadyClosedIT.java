/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.jdbc;

import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class StatementAlreadyClosedIT extends BaseJDBCTest
{
  @Test
  public void testStatementAlreadyClosed() throws Throwable
  {
    try (Connection connection = getConnection())
    {
      Statement statement = connection.createStatement();
      statement.close();

      try
      {
        statement.execute("select 1", Statement.NO_GENERATED_KEYS);
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertStatementClosedError(ex);
      }
      try
      {
        statement.executeBatch();
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertStatementClosedError(ex);
      }
      try
      {
        statement.executeQuery("select 1");
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertStatementClosedError(ex);
      }
      try
      {
        statement.executeUpdate("update t set c1=1");
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertStatementClosedError(ex);
      }
      try
      {
        statement.execute("update t set c1=1");
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertStatementClosedError(ex);
      }
      try
      {
        statement.getConnection();
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertStatementClosedError(ex);
      }
      try
      {
        statement.getFetchDirection();
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertStatementClosedError(ex);
      }
      try
      {
        statement.getFetchSize();
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertStatementClosedError(ex);
      }
      try
      {
        statement.getMoreResults();
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertStatementClosedError(ex);
      }
      try
      {
        statement.getMoreResults(0);
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertStatementClosedError(ex);
      }
      try
      {
        statement.getQueryTimeout();
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertStatementClosedError(ex);
      }
      try
      {
        statement.getResultSet();
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertStatementClosedError(ex);
      }
      try
      {
        statement.getResultSetConcurrency();
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertStatementClosedError(ex);
      }
      try
      {
        statement.getResultSetHoldability();
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertStatementClosedError(ex);
      }
      try
      {
        statement.getResultSetType();
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertStatementClosedError(ex);
      }
      try
      {
        statement.getUpdateCount();
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertStatementClosedError(ex);
      }
      try
      {
        statement.getWarnings();
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertStatementClosedError(ex);
      }
      try
      {
        statement.setEscapeProcessing(true);
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertStatementClosedError(ex);
      }
      try
      {
        statement.setFetchDirection(ResultSet.FETCH_FORWARD);
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertStatementClosedError(ex);
      }
      try
      {
        statement.setFetchSize(10);
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertStatementClosedError(ex);
      }
      try
      {
        statement.setMaxRows(10);
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertStatementClosedError(ex);
      }
      try
      {
        statement.isPoolable();
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertStatementClosedError(ex);
      }
      try
      {
        statement.setPoolable(false);
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertStatementClosedError(ex);
      }
      try
      {
        statement.setQueryTimeout(10);
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertStatementClosedError(ex);
      }
      try
      {
        statement.cancel();
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertStatementClosedError(ex);
      }
      try
      {
        statement.clearWarnings();
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertStatementClosedError(ex);
      }
      try
      {
        statement.addBatch("select 2");
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertStatementClosedError(ex);
      }
      try
      {
        statement.clearBatch();
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertStatementClosedError(ex);
      }
    }
  }

  private void assertStatementClosedError(SQLException ex)
  {
    assertEquals((int) ErrorCode.STATEMENT_CLOSED.getMessageCode(), ex.getErrorCode());
  }
}
