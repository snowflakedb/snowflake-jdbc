package net.snowflake.client.api.statement;

import java.sql.SQLException;

// Do we need this interface? How is it expected to be used?
public interface SnowflakeCallableStatement {
  String getQueryID() throws SQLException;
}
