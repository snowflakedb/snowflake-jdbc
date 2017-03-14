/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SFResultSetMetaData;

import java.sql.SQLException;
import java.util.List;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

/**
 * Snowflake ResultSetMetaData
 *
 * @author jhuang
 */
public class SnowflakeResultSetMetaDataV1 extends SnowflakeResultSetMetaData
{
  static final
  SFLogger logger = SFLoggerFactory.getLogger(SnowflakeResultSetMetaDataV1.class);

  private SFResultSetMetaData resultSetMetaData;

  public SnowflakeResultSetMetaDataV1(SFResultSetMetaData resultSetMetaData)
          throws SnowflakeSQLException
  {
    this.resultSetMetaData = resultSetMetaData;
  }

  /**
   * @return query id
   */
  public String getQueryId()
  {
    return resultSetMetaData.getQueryId();
  }

  /**
   * @return list of column names
   */
  public List<String> getColumnNames()
  {
    return resultSetMetaData.getColumnNames();
  }

  /**
   * @return index of the column by name
   */
  public int getColumnIndex(String columnName)
  {
    return resultSetMetaData.getColumnIndex(columnName);
  }

  /**
   * @return column count
   * @throws java.sql.SQLException if failed to get column count
   */
  @Override
  public int getColumnCount() throws SQLException
  {
    return resultSetMetaData.getColumnCount();
  }

  @Override
  public boolean isSigned(int column) throws SQLException
  {
    return resultSetMetaData.isSigned(column);
  }

  @Override
  public String getColumnLabel(int column) throws SQLException
  {
    return resultSetMetaData.getColumnLabel(column);
  }

  @Override
  public String getColumnName(int column) throws SQLException
  {
    return resultSetMetaData.getColumnName(column);
  }

  @Override
  public int getPrecision(int column) throws SQLException
  {
    return resultSetMetaData.getPrecision(column);
  }

  @Override
  public int getScale(int column) throws SQLException
  {
    return resultSetMetaData.getScale(column);
  }

  @Override
  public int getInternalColumnType(int column) throws SQLException
  {
    try
    {
      return resultSetMetaData.getInternalColumnType(column);
    }
    catch (SFException ex)
    {
      throw new SnowflakeSQLException(ex.getCause(),
          ex.getSqlState(), ex.getVendorCode(), ex.getParams());
    }
  }

  @Override
  public int getColumnType(int column) throws SQLException
  {
    try
    {
      return resultSetMetaData.getColumnType(column);
    }
    catch (SFException ex)
    {
      throw new SnowflakeSQLException(ex.getCause(),
          ex.getSqlState(), ex.getVendorCode(), ex.getParams());
    }
  }

  @Override
  public String getColumnTypeName(int column) throws SQLException
  {
    try
    {
      return resultSetMetaData.getColumnTypeName(column);
    }
    catch (SFException ex)
    {
      throw new SnowflakeSQLException(ex.getCause(),
          ex.getSqlState(), ex.getVendorCode(), ex.getParams());
    }
  }

  @Override
  public int isNullable(int column) throws SQLException
  {
    return resultSetMetaData.isNullable(column);
  }

  @Override
  public String getCatalogName(int column) throws SQLException
  {
    return resultSetMetaData.getCatalogName(column);
  }

  @Override
  public String getSchemaName(int column) throws SQLException
  {
    return resultSetMetaData.getSchemaName(column);
  }

  @Override
  public String getTableName(int column) throws SQLException
  {
    return resultSetMetaData.getTableName(column);
  }

  @Override
  public int getColumnDisplaySize(int column) throws SQLException
  {
    return resultSetMetaData.getColumnDisplaySize(column);
  }
}
