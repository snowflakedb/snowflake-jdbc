package net.snowflake.client.jdbc;

import net.snowflake.client.core.SFStatementMetaData;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

/**
 * Naive implementation of ParameterMetadata class.
 * <p>
 * This class is backed by SFStatementMetadata class, where metadata
 * information is stored as describe sql response.
 */
class SnowflakeParameterMetadata implements ParameterMetaData
{
  private SFStatementMetaData statementMetaData;

  SnowflakeParameterMetadata(SFStatementMetaData sfStatementMetaData)
  {
    this.statementMetaData = sfStatementMetaData;
  }

  @Override
  public int getParameterCount() throws SQLException
  {
    return statementMetaData.getNumberOfBinds();
  }

  @Override
  public int isNullable(int param) throws SQLException
  {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isSigned(int param) throws SQLException
  {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getPrecision(int param) throws SQLException
  {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getScale(int param) throws SQLException
  {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getParameterType(int param) throws SQLException
  {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getParameterTypeName(int param) throws SQLException
  {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getParameterClassName(int param) throws SQLException
  {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getParameterMode(int param) throws SQLException
  {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public <T> T unwrap(java.lang.Class<T> iface) throws SQLException
  {
    if (!isWrapperFor(iface))
    {
      throw new SQLException(
          this.getClass().getName() + " not unwrappable from " + iface
              .getName());
    }
    return (T) this;
  }

  @Override
  public boolean isWrapperFor(java.lang.Class<?> iface) throws SQLException
  {
    return iface.isInstance(this);
  }
}
