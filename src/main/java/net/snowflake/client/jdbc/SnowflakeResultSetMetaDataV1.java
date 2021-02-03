/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import net.snowflake.client.core.SFBaseSession;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SFResultSetMetaData;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

/** Snowflake ResultSetMetaData */
class SnowflakeResultSetMetaDataV1 implements ResultSetMetaData, SnowflakeResultSetMetaData {

  public enum QueryType {
    ASYNC,
    SYNC
  };

  static final SFLogger logger = SFLoggerFactory.getLogger(SnowflakeResultSetMetaDataV1.class);

  private SFResultSetMetaData resultSetMetaData;
  private String queryId;
  private QueryType queryType = QueryType.SYNC;
  private SFBaseSession session;

  SnowflakeResultSetMetaDataV1(SFResultSetMetaData resultSetMetaData) throws SnowflakeSQLException {
    this.resultSetMetaData = resultSetMetaData;
    this.queryId = resultSetMetaData.getQueryId();
    this.session = resultSetMetaData.getSession();
  }

  public void setQueryType(QueryType type) {
    this.queryType = type;
  }

  /** @return query id */
  public String getQueryID() throws SQLException {
    return this.queryId;
  }

  /**
   * Override the original query ID to provide the accurate query ID for metadata produced from an
   * SFAsyncResultSet. The original query ID is from the result_scan query. The user expects to
   * retrieve the query ID from the original query instead.
   */
  public void setQueryIdForAsyncResults(String queryId) {
    this.queryId = queryId;
  }

  /** @return list of column names */
  public List<String> getColumnNames() throws SQLException {
    return resultSetMetaData.getColumnNames();
  }

  /** @return index of the column by name, index starts from zero */
  public int getColumnIndex(String columnName) throws SQLException {
    return resultSetMetaData.getColumnIndex(columnName);
  }

  public int getInternalColumnType(int column) throws SQLException {
    try {
      return resultSetMetaData.getInternalColumnType(column);
    } catch (SFException ex) {
      throw new SnowflakeSQLLoggedException(
          session, ex.getSqlState(), ex.getVendorCode(), ex.getCause(), ex.getParams());
    }
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    logger.debug("public <T> T unwrap(Class<T> iface)");

    if (!iface.isInstance(this)) {
      throw new SQLException(
          this.getClass().getName() + " not unwrappable from " + iface.getName());
    }
    return (T) this;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    logger.debug("public boolean isWrapperFor(Class<?> iface)");

    return iface.isInstance(this);
  }

  @Override
  public boolean isAutoIncrement(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isCaseSensitive(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isSearchable(int column) throws SQLException {
    return true;
  }

  @Override
  public boolean isCurrency(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isReadOnly(int column) throws SQLException {
    return true; // metadata column is always readonly
  }

  @Override
  public boolean isWritable(int column) throws SQLException {
    return false; // never writable
  }

  @Override
  public boolean isDefinitelyWritable(int column) throws SQLException {
    return false; // never writable
  }

  @Override
  public String getColumnClassName(int column) throws SQLException {
    logger.debug("public String getColumnClassName(int column)");

    int type = this.getColumnType(column);

    return SnowflakeType.javaTypeToClassName(type);
  }

  /**
   * @return column count
   * @throws java.sql.SQLException if failed to get column count
   */
  @Override
  public int getColumnCount() throws SQLException {
    return resultSetMetaData.getColumnCount();
  }

  @Override
  public boolean isSigned(int column) throws SQLException {
    return resultSetMetaData.isSigned(column);
  }

  @Override
  public String getColumnLabel(int column) throws SQLException {
    return resultSetMetaData.getColumnLabel(column);
  }

  @Override
  public String getColumnName(int column) throws SQLException {
    return resultSetMetaData.getColumnName(column);
  }

  @Override
  public int getPrecision(int column) throws SQLException {
    return resultSetMetaData.getPrecision(column);
  }

  @Override
  public int getScale(int column) throws SQLException {
    return resultSetMetaData.getScale(column);
  }

  @Override
  public int getColumnType(int column) throws SQLException {
    try {
      return resultSetMetaData.getColumnType(column);
    } catch (SFException ex) {
      throw new SnowflakeSQLLoggedException(
          session, ex.getSqlState(), ex.getVendorCode(), ex.getCause(), ex.getParams());
    }
  }

  @Override
  public String getColumnTypeName(int column) throws SQLException {
    try {
      return resultSetMetaData.getColumnTypeName(column);
    } catch (SFException ex) {
      throw new SnowflakeSQLLoggedException(
          session, ex.getSqlState(), ex.getVendorCode(), ex.getCause(), ex.getParams());
    }
  }

  @Override
  public int isNullable(int column) throws SQLException {
    return resultSetMetaData.isNullable(column);
  }

  @Override
  public String getCatalogName(int column) throws SQLException {
    if (this.queryType == QueryType.SYNC) {
      return resultSetMetaData.getCatalogName(column);
    }
    return "";
  }

  @Override
  public String getSchemaName(int column) throws SQLException {
    if (this.queryType == QueryType.SYNC) {
      return resultSetMetaData.getSchemaName(column);
    }
    return "";
  }

  @Override
  public String getTableName(int column) throws SQLException {
    if (this.queryType == QueryType.SYNC) {
      return resultSetMetaData.getTableName(column);
    }
    return "";
  }

  @Override
  public int getColumnDisplaySize(int column) throws SQLException {
    return resultSetMetaData.getColumnDisplaySize(column);
  }
}
