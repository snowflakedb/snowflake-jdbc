/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ConnectionAlreadyClosedIT extends BaseJDBCTest
{

  @Test
  public void testClosedConnection() throws Throwable
  {
    Connection connection = getConnection();
    connection.close();
    try
    {
      connection.getMetaData();
      fail("must raise exception");
    }
    catch (SQLException ex)
    {
      assertConnectionClosedError(ex);
    }
    try
    {
      connection.nativeSQL("select 1");
      fail("must raise exception");
    }
    catch (SQLException ex)
    {
      assertConnectionClosedError(ex);
    }
    try
    {
      connection.setAutoCommit(false);
      fail("must raise exception");
    }
    catch (SQLException ex)
    {
      assertConnectionClosedError(ex);
    }
    try
    {
      connection.getAutoCommit();
      fail("must raise exception");
    }
    catch (SQLException ex)
    {
      assertConnectionClosedError(ex);
    }
    try
    {
      connection.commit();
      fail("must raise exception");
    }
    catch (SQLException ex)
    {
      assertConnectionClosedError(ex);
    }
    try
    {
      connection.rollback();
      fail("must raise exception");
    }
    catch (SQLException ex)
    {
      assertConnectionClosedError(ex);
    }
    try
    {
      connection.setReadOnly(false);
      fail("must raise exception");
    }
    catch (SQLException ex)
    {
      assertConnectionClosedError(ex);
    }
    try
    {
      connection.isReadOnly();
      fail("must raise exception");
    }
    catch (SQLException ex)
    {
      assertConnectionClosedError(ex);
    }
    try
    {
      connection.setCatalog("fake");
      fail("must raise exception");
    }
    catch (SQLException ex)
    {
      assertConnectionClosedError(ex);
    }
    try
    {
      connection.getCatalog();
      fail("must raise exception");
    }
    catch (SQLException ex)
    {
      assertConnectionClosedError(ex);
    }
    try
    {
      connection.setSchema("fake");
      fail("must raise exception");
    }
    catch (SQLException ex)
    {
      assertConnectionClosedError(ex);
    }
    try
    {
      connection.getSchema();
      fail("must raise exception");
    }
    catch (SQLException ex)
    {
      assertConnectionClosedError(ex);
    }
    try
    {
      connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
      fail("must raise exception");
    }
    catch (SQLException ex)
    {
      assertConnectionClosedError(ex);
    }
    try
    {
      connection.getTransactionIsolation();
      fail("must raise exception");
    }
    catch (SQLException ex)
    {
      assertConnectionClosedError(ex);
    }
    try
    {
      connection.getWarnings();
      fail("must raise exception");
    }
    catch (SQLException ex)
    {
      assertConnectionClosedError(ex);
    }
    try
    {
      connection.clearWarnings();
      fail("must raise exception");
    }
    catch (SQLException ex)
    {
      assertConnectionClosedError(ex);
    }
    try
    {
      Properties prop = new Properties();
      connection.setClientInfo(prop);
      fail("must raise exception");
    }
    catch (SQLClientInfoException ex)
    {
      // nop, closed connection but different error class
    }
    try
    {
      connection.setClientInfo("name", "value");
      fail("must raise exception");
    }
    catch (SQLClientInfoException ex)
    {
      // nop, closed connection but different error class
    }
  }

  private void assertConnectionClosedError(SQLException ex)
  {
    assertEquals((int) ErrorCode.CONNECTION_CLOSED.getMessageCode(), ex.getErrorCode());
  }
}
