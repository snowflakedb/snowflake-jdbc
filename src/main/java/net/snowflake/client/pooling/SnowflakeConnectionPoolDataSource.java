/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.pooling;

import net.snowflake.client.jdbc.SnowflakeBasicDataSource;

import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;
import java.sql.Connection;
import java.sql.SQLException;

public class SnowflakeConnectionPoolDataSource
    extends SnowflakeBasicDataSource
    implements ConnectionPoolDataSource
{
  @Override
  public PooledConnection getPooledConnection() throws SQLException
  {
    Connection connection = super.getConnection();
    return new SnowflakePooledConnection(connection);
  }

  @Override
  public PooledConnection getPooledConnection(String user, String password)
  throws SQLException
  {
    Connection connection = super.getConnection(user, password);
    return new SnowflakePooledConnection(connection);
  }
}
