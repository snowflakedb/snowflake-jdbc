package net.snowflake.client.jdbc;

import java.math.BigInteger;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
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
   * Sets the designated parameter to the given <code>java.sql.Array</code> object.
   * The driver converts this to an SQL <code>ARRAY</code> value when it
   * sends it to the database.
   *
   * @param parameterIndex the first parameter is 1, the second is 2, ...
   * @param x an <code>Array</code> object that maps an SQL <code>ARRAY</code> value
   * @exception SQLException if parameterIndex does not correspond to a parameter
   * marker in the SQL statement; if a database access error occurs or
   * this method is called on a closed <code>PreparedStatement</code>
   * @throws SQLFeatureNotSupportedException  if the JDBC driver does not support this method
   * @since 1.2
   */
  void setArray (int parameterIndex, Array x, SnowflakeType snowflakeType) throws SQLException;
  /**
   * Sets the designated parameter to the given Map instance.
   *
   * @param parameterIndex
   * @param map
   * @throws SQLException
   */
  <T> void setMap(int parameterIndex, Map<String, T> map, int type) throws SQLException;
  /**
   * Sets the designated parameter to the given Map instance.
   *
   * @param parameterIndex
   * @param map
   * @throws SQLException
   */
  <T> void setMap(int parameterIndex, Map<String, T> map, int type, SnowflakeType snowflakeType) throws SQLException;
}
