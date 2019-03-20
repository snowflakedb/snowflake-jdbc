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
  private void expectAlreadyClosedException(MethodRaisesSQLException f)
  {
    try
    {
      f.run();
      fail("must raise exception");
    }
    catch (SQLException ex)
    {
      assertConnectionClosedError(ex);
    }
  }

  private void expectSQLClientInfoException(MethodRaisesSQLClientInfoException f)
  {
    try
    {
      f.run();
      fail("must raise exception");
    }
    catch (SQLClientInfoException ex)
    {
      // noup
    }
  }

  @Test
  public void testClosedConnection() throws Throwable
  {
    Connection connection = getConnection();
    connection.close();

    expectAlreadyClosedException(connection::getMetaData);
    expectAlreadyClosedException(connection::getAutoCommit);
    expectAlreadyClosedException(connection::commit);
    expectAlreadyClosedException(connection::rollback);
    expectAlreadyClosedException(connection::isReadOnly);
    expectAlreadyClosedException(connection::getCatalog);
    expectAlreadyClosedException(connection::getSchema);
    expectAlreadyClosedException(connection::getTransactionIsolation);
    expectAlreadyClosedException(connection::getWarnings);
    expectAlreadyClosedException(connection::clearWarnings);
    expectAlreadyClosedException(() -> connection.nativeSQL("sekect 1"));
    expectAlreadyClosedException(() -> connection.setAutoCommit(false));
    expectAlreadyClosedException(() -> connection.setReadOnly(false));
    expectAlreadyClosedException(() -> connection.setCatalog("fakedb"));
    expectAlreadyClosedException(() -> connection.setSchema("fakedb"));
    expectAlreadyClosedException(() -> connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED));
    expectSQLClientInfoException(() -> connection.setClientInfo(new Properties()));
    expectSQLClientInfoException(() -> connection.setClientInfo("name", "value"));
  }

  private void assertConnectionClosedError(SQLException ex)
  {
    assertEquals((int) ErrorCode.CONNECTION_CLOSED.getMessageCode(), ex.getErrorCode());
  }
}
