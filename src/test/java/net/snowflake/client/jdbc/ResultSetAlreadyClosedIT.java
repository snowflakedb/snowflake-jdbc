/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class ResultSetAlreadyClosedIT extends BaseJDBCTest
{

  @Test
  public void testClosedResultSet() throws Throwable
  {
    try (Connection connection = getConnection())
    {
      Statement statement = connection.createStatement();
      ResultSet resultSet = statement.executeQuery("select 1");
      resultSet.close();
      resultSet.close(); // second close won't raise exception
      assertFalse(resultSet.next()); // next after close should return false.
      try
      {
        resultSet.wasNull();
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertResultSetClosedError(ex);
      }
      try
      {
        resultSet.getString(1);
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertResultSetClosedError(ex);
      }
      try
      {
        resultSet.getBoolean(1);
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertResultSetClosedError(ex);
      }
      try
      {
        resultSet.getByte(1);
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertResultSetClosedError(ex);
      }
      try
      {
        resultSet.getShort(1);
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertResultSetClosedError(ex);
      }
      try
      {
        resultSet.getInt(1);
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertResultSetClosedError(ex);
      }
      try
      {
        resultSet.getLong(1);
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertResultSetClosedError(ex);
      }
      try
      {
        resultSet.getFloat(1);
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertResultSetClosedError(ex);
      }
      try
      {
        resultSet.getDouble(1);
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertResultSetClosedError(ex);
      }
      try
      {
        resultSet.getBigDecimal(1);
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertResultSetClosedError(ex);
      }
      try
      {
        resultSet.getBytes(1);
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertResultSetClosedError(ex);
      }
      try
      {
        resultSet.getDate(1);
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertResultSetClosedError(ex);
      }
      try
      {
        resultSet.getTime(1);
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertResultSetClosedError(ex);
      }
      try
      {
        resultSet.getTimestamp(1);
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertResultSetClosedError(ex);
      }
      try
      {
        resultSet.getString("col1");
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertResultSetClosedError(ex);
      }
      try
      {
        resultSet.getBoolean("col1");
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertResultSetClosedError(ex);
      }
      try
      {
        resultSet.getByte("col1");
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertResultSetClosedError(ex);
      }
      try
      {
        resultSet.getShort("col1");
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertResultSetClosedError(ex);
      }
      try
      {
        resultSet.getInt("col1");
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertResultSetClosedError(ex);
      }
      try
      {
        resultSet.getLong("col1");
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertResultSetClosedError(ex);
      }
      try
      {
        resultSet.getFloat("col1");
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertResultSetClosedError(ex);
      }
      try
      {
        resultSet.getDouble("col1");
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertResultSetClosedError(ex);
      }
      try
      {
        resultSet.getBigDecimal("col1");
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertResultSetClosedError(ex);
      }
      try
      {
        resultSet.getBytes("col1");
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertResultSetClosedError(ex);
      }
      try
      {
        resultSet.getDate("col1");
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertResultSetClosedError(ex);
      }
      try
      {
        resultSet.getTime("col1");
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertResultSetClosedError(ex);
      }
      try
      {
        resultSet.getTimestamp("col1");
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertResultSetClosedError(ex);
      }
      try
      {
        resultSet.getWarnings();
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertResultSetClosedError(ex);
      }
      try
      {
        resultSet.clearWarnings();
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertResultSetClosedError(ex);
      }
      try
      {
        resultSet.getMetaData();
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertResultSetClosedError(ex);
      }
      try
      {
        resultSet.getObject(1);
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertResultSetClosedError(ex);
      }
      try
      {
        resultSet.getObject("col1");
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertResultSetClosedError(ex);
      }
      try
      {
        resultSet.findColumn("col1");
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertResultSetClosedError(ex);
      }
      try
      {
        resultSet.getCharacterStream(1);
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertResultSetClosedError(ex);
      }
      try
      {
        resultSet.getCharacterStream("col1");
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertResultSetClosedError(ex);
      }
      try
      {
        resultSet.isBeforeFirst();
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertResultSetClosedError(ex);
      }
      try
      {
        resultSet.isAfterLast();
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertResultSetClosedError(ex);
      }
      try
      {
        resultSet.isFirst();
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertResultSetClosedError(ex);
      }
      try
      {
        resultSet.isLast();
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertResultSetClosedError(ex);
      }
      try
      {
        resultSet.getRow();
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertResultSetClosedError(ex);
      }
      try
      {
        resultSet.setFetchDirection(ResultSet.FETCH_FORWARD);
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertResultSetClosedError(ex);
      }
      try
      {
        resultSet.getFetchDirection();
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertResultSetClosedError(ex);
      }
      try
      {
        resultSet.setFetchSize(10);
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertResultSetClosedError(ex);
      }
      try
      {
        resultSet.getFetchSize();
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertResultSetClosedError(ex);
      }
      try
      {
        resultSet.getType();
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertResultSetClosedError(ex);
      }
      try
      {
        resultSet.getConcurrency();
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertResultSetClosedError(ex);
      }
      try
      {
        resultSet.getStatement();
        fail("must raise exception");
      }
      catch (SQLException ex)
      {
        assertResultSetClosedError(ex);
      }
    }
  }

  private void assertResultSetClosedError(SQLException ex)
  {
    assertEquals((int) ErrorCode.RESULTSET_ALREADY_CLOSED.getMessageCode(), ex.getErrorCode());
  }
}
