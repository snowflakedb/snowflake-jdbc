package net.snowflake.client.pooling;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import javax.sql.PooledConnection;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.jdbc.BaseJDBCTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.CONNECTION)
public class LogicalConnectionAlreadyClosedLatestIT extends BaseJDBCTest {

  @Test
  public void testLogicalConnectionAlreadyClosed() throws SQLException {
    Map<String, String> properties = getConnectionParameters();

    SnowflakeConnectionPoolDataSource poolDataSource = new SnowflakeConnectionPoolDataSource();

    poolDataSource.setUrl(properties.get("uri"));
    poolDataSource.setPortNumber(Integer.parseInt(properties.get("port")));
    poolDataSource.setSsl("on".equals(properties.get("ssl")));
    poolDataSource.setAccount(properties.get("account"));
    poolDataSource.setUser(properties.get("user"));
    poolDataSource.setPassword(properties.get("password"));

    PooledConnection pooledConnection = poolDataSource.getPooledConnection();
    Connection logicalConnection = pooledConnection.getConnection();
    logicalConnection.close();

    expectConnectionAlreadyClosedException(logicalConnection::getMetaData);
    expectConnectionAlreadyClosedException(logicalConnection::getAutoCommit);
    expectConnectionAlreadyClosedException(logicalConnection::commit);
    expectConnectionAlreadyClosedException(logicalConnection::rollback);
    expectConnectionAlreadyClosedException(logicalConnection::isReadOnly);
    expectConnectionAlreadyClosedException(logicalConnection::getCatalog);
    expectConnectionAlreadyClosedException(logicalConnection::getSchema);
    expectConnectionAlreadyClosedException(logicalConnection::getTransactionIsolation);
    expectConnectionAlreadyClosedException(logicalConnection::getWarnings);
    expectConnectionAlreadyClosedException(logicalConnection::clearWarnings);
    expectConnectionAlreadyClosedException(() -> logicalConnection.nativeSQL("select 1"));
    expectConnectionAlreadyClosedException(() -> logicalConnection.setAutoCommit(false));
    expectConnectionAlreadyClosedException(() -> logicalConnection.setReadOnly(false));
    expectConnectionAlreadyClosedException(() -> logicalConnection.setCatalog("fakedb"));
    expectConnectionAlreadyClosedException(() -> logicalConnection.setSchema("fakedb"));
    expectConnectionAlreadyClosedException(
        () -> logicalConnection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED));
    expectConnectionAlreadyClosedException(() -> logicalConnection.createArrayOf("faketype", null));
  }
}
