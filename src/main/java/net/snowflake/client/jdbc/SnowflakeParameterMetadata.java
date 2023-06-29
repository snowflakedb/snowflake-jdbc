package net.snowflake.client.jdbc;

import static net.snowflake.client.jdbc.SnowflakeType.convertStringToType;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import net.snowflake.client.core.MetaDataOfBinds;
import net.snowflake.client.core.SFBaseSession;
import net.snowflake.client.core.SFPreparedStatementMetaData;

/**
 * Naive implementation of ParameterMetadata class.
 *
 * <p>This class is backed by SFStatementMetadata class, where metadata information is stored as
 * describe sql response.
 */
class SnowflakeParameterMetadata implements ParameterMetaData {
  private SFPreparedStatementMetaData sfPreparedStatementMetaData;
  private SFBaseSession session;

  SnowflakeParameterMetadata(
      SFPreparedStatementMetaData sfStatementMetaData, SFBaseSession session) {
    this.sfPreparedStatementMetaData = sfStatementMetaData;
    this.session = session;
  }

  @Override
  public int getParameterCount() throws SQLException {
    return sfPreparedStatementMetaData.getNumberOfBinds();
  }

  @Override
  public int isNullable(int param) throws SQLException {
    MetaDataOfBinds paramInfo = sfPreparedStatementMetaData.getMetaDataForBindParam(param);
    if (paramInfo.isNullable()) {
      return ParameterMetaData.parameterNullable;
    }
    return ParameterMetaData.parameterNoNulls;
  }

  @Override
  public boolean isSigned(int param) throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public int getPrecision(int param) throws SQLException {
    MetaDataOfBinds paramInfo = sfPreparedStatementMetaData.getMetaDataForBindParam(param);
    return paramInfo.getPrecision();
  }

  @Override
  public int getScale(int param) throws SQLException {
    MetaDataOfBinds paramInfo = sfPreparedStatementMetaData.getMetaDataForBindParam(param);
    return paramInfo.getScale();
  }

  @Override
  public int getParameterType(int param) throws SQLException {
    MetaDataOfBinds paramInfo = sfPreparedStatementMetaData.getMetaDataForBindParam(param);
    return convertStringToType(paramInfo.getTypeName());
  }

  @Override
  public String getParameterTypeName(int param) throws SQLException {
    MetaDataOfBinds paramInfo = sfPreparedStatementMetaData.getMetaDataForBindParam(param);
    return paramInfo.getTypeName();
  }

  @Override
  public String getParameterClassName(int param) throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public int getParameterMode(int param) throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T unwrap(java.lang.Class<T> iface) throws SQLException {
    if (!isWrapperFor(iface)) {
      throw new SQLException(
          this.getClass().getName() + " not unwrappable from " + iface.getName());
    }
    return (T) this;
  }

  @Override
  public boolean isWrapperFor(java.lang.Class<?> iface) throws SQLException {
    return iface.isInstance(this);
  }
}
