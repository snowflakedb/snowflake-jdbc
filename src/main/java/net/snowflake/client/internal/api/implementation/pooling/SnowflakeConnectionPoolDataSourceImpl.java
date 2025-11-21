package net.snowflake.client.internal.api.implementation.pooling;

import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.PooledConnection;
import net.snowflake.client.api.pooling.SnowflakeConnectionPoolDataSource;
import net.snowflake.client.internal.api.implementation.datasource.SnowflakeBasicDataSource;

public class SnowflakeConnectionPoolDataSourceImpl extends SnowflakeBasicDataSource
    implements SnowflakeConnectionPoolDataSource {
  @Override
  public PooledConnection getPooledConnection() throws SQLException {
    Connection connection = super.getConnection();
    return new SnowflakePooledConnection(connection);
  }

  @Override
  public PooledConnection getPooledConnection(String user, String password) throws SQLException {
    Connection connection = super.getConnection(user, password);
    return new SnowflakePooledConnection(connection);
  }
}
