package net.snowflake.client.api.statement;

import java.sql.SQLException;
import net.snowflake.client.jdbc.*;

public interface SnowflakeCallableStatement {
  String getQueryID() throws SQLException;
}
