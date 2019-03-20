/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import org.junit.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ResultSetAlreadyClosedIT extends BaseJDBCTest
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
      assertResultSetClosedError(ex);
    }
  }

  @Test
  public void testQueryResultSetAlreadyClosed() throws Throwable
  {
    try (Connection connection = getConnection())
    {
      Statement statement = connection.createStatement();
      ResultSet resultSet = statement.executeQuery("select 1");
      checkAlreadyClosed(resultSet);
    }
  }

  @Test
  public void testMetadataResultSetAlreadyClosed() throws Throwable
  {
    try (Connection connection = getConnection())
    {
      String database = connection.getCatalog();
      String schema = connection.getSchema();
      DatabaseMetaData metaData = connection.getMetaData();

      checkAlreadyClosed(metaData.getCatalogs());
      checkAlreadyClosed(metaData.getSchemas());
      checkAlreadyClosed(metaData.getSchemas(database, null));
      checkAlreadyClosed(metaData.getTables(database, schema, null, null));
      checkAlreadyClosed(metaData.getColumns(database, schema, null, null));
    }
  }

  @Test
  public void testEmptyResultSetAlreadyClosed() throws Throwable
  {
    ResultSet resultSet = new SnowflakeResultSetV1.EmptyResultSet();
    checkAlreadyClosed(resultSet);
  }

  private void checkAlreadyClosed(ResultSet resultSet) throws SQLException
  {
    resultSet.close();
    resultSet.close(); // second close won't raise exception
    assertTrue(resultSet.isClosed());
    assertFalse(resultSet.next()); // next after close should return false.

    expectAlreadyClosedException(resultSet::wasNull);
    expectAlreadyClosedException(() -> resultSet.getString(1));
    expectAlreadyClosedException(() -> resultSet.getBoolean(1));
    expectAlreadyClosedException(() -> resultSet.getByte(1));
    expectAlreadyClosedException(() -> resultSet.getShort(1));
    expectAlreadyClosedException(() -> resultSet.getInt(1));
    expectAlreadyClosedException(() -> resultSet.getLong(1));
    expectAlreadyClosedException(() -> resultSet.getFloat(1));
    expectAlreadyClosedException(() -> resultSet.getDouble(1));
    expectAlreadyClosedException(() -> resultSet.getBigDecimal(1));
    expectAlreadyClosedException(() -> resultSet.getBytes(1));
    expectAlreadyClosedException(() -> resultSet.getString(1));
    expectAlreadyClosedException(() -> resultSet.getDate(1));
    expectAlreadyClosedException(() -> resultSet.getTime(1));
    expectAlreadyClosedException(() -> resultSet.getTimestamp(1));
    expectAlreadyClosedException(() -> resultSet.getObject(1));
    expectAlreadyClosedException(() -> resultSet.getCharacterStream(1));
    expectAlreadyClosedException(() -> resultSet.getClob(1));
    expectAlreadyClosedException(() -> resultSet.getDate(1, Calendar.getInstance()));
    expectAlreadyClosedException(() -> resultSet.getTime(1, Calendar.getInstance()));
    expectAlreadyClosedException(() -> resultSet.getTimestamp(1, Calendar.getInstance()));

    expectAlreadyClosedException(() -> resultSet.getString("col1"));
    expectAlreadyClosedException(() -> resultSet.getBoolean("col1"));
    expectAlreadyClosedException(() -> resultSet.getByte("col1"));
    expectAlreadyClosedException(() -> resultSet.getShort("col1"));
    expectAlreadyClosedException(() -> resultSet.getInt("col1"));
    expectAlreadyClosedException(() -> resultSet.getLong("col1"));
    expectAlreadyClosedException(() -> resultSet.getFloat("col1"));
    expectAlreadyClosedException(() -> resultSet.getDouble("col1"));
    expectAlreadyClosedException(() -> resultSet.getBigDecimal("col1"));
    expectAlreadyClosedException(() -> resultSet.getBytes("col1"));
    expectAlreadyClosedException(() -> resultSet.getString("col1"));
    expectAlreadyClosedException(() -> resultSet.getDate("col1"));
    expectAlreadyClosedException(() -> resultSet.getTime("col1"));
    expectAlreadyClosedException(() -> resultSet.getTimestamp("col1"));
    expectAlreadyClosedException(() -> resultSet.getObject("col1"));
    expectAlreadyClosedException(() -> resultSet.getCharacterStream("col1"));
    expectAlreadyClosedException(() -> resultSet.getClob("col1"));
    expectAlreadyClosedException(() -> resultSet.getDate("col1", Calendar.getInstance()));
    expectAlreadyClosedException(() -> resultSet.getTime("col1", Calendar.getInstance()));
    expectAlreadyClosedException(() -> resultSet.getTimestamp("col1", Calendar.getInstance()));

    expectAlreadyClosedException(resultSet::getWarnings);
    expectAlreadyClosedException(resultSet::clearWarnings);
    expectAlreadyClosedException(resultSet::getMetaData);

    expectAlreadyClosedException(() -> resultSet.findColumn("col1"));

    expectAlreadyClosedException(resultSet::isBeforeFirst);
    expectAlreadyClosedException(resultSet::isAfterLast);
    expectAlreadyClosedException(resultSet::isFirst);
    expectAlreadyClosedException(resultSet::isLast);
    expectAlreadyClosedException(resultSet::getRow);

    expectAlreadyClosedException(() -> resultSet.setFetchDirection(ResultSet.FETCH_FORWARD));
    expectAlreadyClosedException(() -> resultSet.setFetchSize(10));
    expectAlreadyClosedException(resultSet::getFetchDirection);
    expectAlreadyClosedException(resultSet::getFetchSize);
    expectAlreadyClosedException(resultSet::getType);
    expectAlreadyClosedException(resultSet::getConcurrency);
    expectAlreadyClosedException(resultSet::getHoldability);
    expectAlreadyClosedException(resultSet::getStatement);
  }

  private void assertResultSetClosedError(SQLException ex)
  {
    assertEquals((int) ErrorCode.RESULTSET_ALREADY_CLOSED.getMessageCode(), ex.getErrorCode());
  }
}
