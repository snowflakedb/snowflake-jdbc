package net.snowflake.client.core;

import java.sql.Array;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Map;

@SnowflakeJdbcInternalApi
public class SfSqlArray implements Array {

  private int baseType;
  private Object elements;

  public SfSqlArray(int baseType, Object elements) {
    this.baseType = baseType;
    this.elements = elements;
  }

  @Override
  public String getBaseTypeName() throws SQLException {
    return JDBCType.valueOf(baseType).getName();
  }

  @Override
  public int getBaseType() throws SQLException {
    return baseType;
  }

  @Override
  public Object getArray() throws SQLException {
    return elements;
  }

  @Override
  public Object getArray(Map<String, Class<?>> map) throws SQLException {
    throw new SQLFeatureNotSupportedException("getArray(Map<String, Class<?>> map)");
  }

  @Override
  public Object getArray(long index, int count) throws SQLException {
    throw new SQLFeatureNotSupportedException("getArray(long index, int count)");
  }

  @Override
  public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "getArray(long index, int count, Map<String, Class<?>> map)");
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "getArray(long index, int count, Map<String, Class<?>> map)");
  }

  @Override
  public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
    throw new SQLFeatureNotSupportedException("getResultSet(Map<String, Class<?>> map)");
  }

  @Override
  public ResultSet getResultSet(long index, int count) throws SQLException {
    throw new SQLFeatureNotSupportedException("getResultSet(long index, int count)");
  }

  @Override
  public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map)
      throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "getResultSet(long index, int count, Map<String, Class<?>> map)");
  }

  @Override
  public void free() throws SQLException {}
}
