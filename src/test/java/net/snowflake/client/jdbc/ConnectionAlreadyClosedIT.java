package net.snowflake.client.jdbc;

import java.sql.Connection;
import java.util.Properties;
import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.CONNECTION)
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
