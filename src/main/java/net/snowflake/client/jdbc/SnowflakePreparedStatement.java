package net.snowflake.client.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface SnowflakePreparedStatement {
  /** @return the Snowflake query ID of the latest executed query */
  String getQueryID() throws SQLException;

  /**
   * Execute a query asynchronously
   *
   * @return ResultSet containing results
   * @throws SQLException
   */
  ResultSet executeAsyncQuery() throws SQLException;
}
