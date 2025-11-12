package net.snowflake.client.api.statement;

import java.sql.SQLException;

public interface SnowflakeCallableStatement {
  String getQueryID() throws SQLException;
}
