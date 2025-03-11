package net.snowflake.client.jdbc;

import java.sql.SQLException;

public interface SnowflakeCallableStatement {
  String getQueryID() throws SQLException;
}
