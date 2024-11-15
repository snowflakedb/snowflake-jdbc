package net.snowflake.client.jdbc;

import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public interface SnowflakePreparedStatement {
  /**
   * @return the Snowflake query ID of the latest executed query
   * @throws SQLException if an error occurs
   */
  String getQueryID() throws SQLException;

  /**
   * Execute a query asynchronously
   *
   * @return ResultSet containing results
   * @throws SQLException if an error occurs
   */
  ResultSet executeAsyncQuery() throws SQLException;

  /**
   * Sets the designated parameter to the given BigInteger value.
   *
   * @param parameterIndex the parameter index
   * @param x the BigInteger value
   * @throws SQLException if an error occurs
   */
  void setBigInteger(int parameterIndex, BigInteger x) throws SQLException;

  /**
   * Sets the designated parameter to the given Map instance.
   *
   * @param parameterIndex the parameter index
   * @param map the map instance
   * @param type the type
   * @param <T> generic type
   * @throws SQLException if an error occurs
   */
  <T> void setMap(int parameterIndex, Map<String, T> map, int type) throws SQLException;
}
