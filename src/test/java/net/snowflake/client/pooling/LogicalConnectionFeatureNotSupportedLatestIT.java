package net.snowflake.client.pooling;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import javax.sql.PooledConnection;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.jdbc.BaseJDBCTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.CONNECTION)
public class LogicalConnectionFeatureNotSupportedLatestIT extends BaseJDBCTest {

  @Test
  public void testLogicalConnectionFeatureNotSupported() throws SQLException {
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
    expectFeatureNotSupportedException(() -> logicalConnection.rollback(new FakeSavepoint()));
    expectFeatureNotSupportedException(
        () -> logicalConnection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE));
    expectFeatureNotSupportedException(
        () -> logicalConnection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ));
    expectFeatureNotSupportedException(
        () -> logicalConnection.prepareStatement("select 1", new int[] {1, 2}));
    expectFeatureNotSupportedException(
        () -> logicalConnection.prepareStatement("select 1", new String[] {"c1", "c2"}));
    expectFeatureNotSupportedException(
        () ->
            logicalConnection.prepareStatement(
                "select 1", ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY));
    expectFeatureNotSupportedException(
        () ->
            logicalConnection.prepareStatement(
                "select 1",
                ResultSet.TYPE_SCROLL_SENSITIVE,
                ResultSet.CONCUR_READ_ONLY,
                ResultSet.HOLD_CURSORS_OVER_COMMIT));
    expectFeatureNotSupportedException(
        () ->
            logicalConnection.createStatement(
                ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY));
    expectFeatureNotSupportedException(() -> logicalConnection.setTypeMap(new HashMap<>()));
    expectFeatureNotSupportedException(logicalConnection::setSavepoint);
    expectFeatureNotSupportedException(() -> logicalConnection.setSavepoint("fake"));
    expectFeatureNotSupportedException(
        () -> logicalConnection.releaseSavepoint(new FakeSavepoint()));
    expectFeatureNotSupportedException(logicalConnection::createBlob);
    expectFeatureNotSupportedException(logicalConnection::createNClob);
    expectFeatureNotSupportedException(logicalConnection::createSQLXML);
    expectFeatureNotSupportedException(
        () -> logicalConnection.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT));
    expectFeatureNotSupportedException(
        () -> logicalConnection.createStruct("fakeType", new Object[] {}));
    expectFeatureNotSupportedException(
        () -> logicalConnection.prepareStatement("select 1", Statement.RETURN_GENERATED_KEYS));
  }

  class FakeSavepoint implements Savepoint {
    @Override
    public int getSavepointId() throws SQLException {
      return 0;
    }

    @Override
    public String getSavepointName() throws SQLException {
      return "";
    }
  }
}
