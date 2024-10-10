/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import java.sql.Connection;
import java.util.Properties;
import net.snowflake.client.category.TestCategoryConnection;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.Test;

@Category(TestCategoryConnection.class)
public class ConnectionAlreadyClosedIT extends BaseJDBCTest {

  @Test
  public void testClosedConnection() throws Throwable {
    Connection connection = getConnection();
    connection.close();

    expectConnectionAlreadyClosedException(connection::getMetaData);
    expectConnectionAlreadyClosedException(connection::getAutoCommit);
    expectConnectionAlreadyClosedException(connection::commit);
    expectConnectionAlreadyClosedException(connection::rollback);
    expectConnectionAlreadyClosedException(connection::isReadOnly);
    expectConnectionAlreadyClosedException(connection::getCatalog);
    expectConnectionAlreadyClosedException(connection::getSchema);
    expectConnectionAlreadyClosedException(connection::getTransactionIsolation);
    expectConnectionAlreadyClosedException(connection::getWarnings);
    expectConnectionAlreadyClosedException(connection::clearWarnings);
    expectConnectionAlreadyClosedException(() -> connection.nativeSQL("select 1"));
    expectConnectionAlreadyClosedException(() -> connection.setAutoCommit(false));
    expectConnectionAlreadyClosedException(() -> connection.setReadOnly(false));
    expectConnectionAlreadyClosedException(() -> connection.setCatalog("fakedb"));
    expectConnectionAlreadyClosedException(() -> connection.setSchema("fakedb"));
    expectConnectionAlreadyClosedException(
        () -> connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED));
    expectSQLClientInfoException(() -> connection.setClientInfo(new Properties()));
    expectSQLClientInfoException(() -> connection.setClientInfo("name", "value"));
  }
}
