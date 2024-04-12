package net.snowflake.client.jdbc;

import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public interface SnowflakePreparedStatement {
  /**
   * @return the Snowflake query ID of the latest executed query
   */
  String getQueryID() throws SQLException;

  /**
   * Execute a query asynchronously
   *
   * @return ResultSet containing results
   * @throws SQLException
   */
  ResultSet executeAsyncQuery() throws SQLException;

  /**
   * Sets the designated parameter to the given BigInteger value.
   *
   * @param parameterIndex
   * @param x
   * @throws SQLException
   */
  void setBigInteger(int parameterIndex, BigInteger x) throws SQLException;
  /**
   * Sets the designated parameter to the given Map instance.
   *
   * @param parameterIndex
   * @param map
   * @throws SQLException
   */
  <T> void setMap(int parameterIndex, Map<String, T> map, int type) throws SQLException;
}
