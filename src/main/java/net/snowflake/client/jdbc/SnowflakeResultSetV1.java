/*
 * Copyright (c) 2012-2018 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import net.snowflake.client.core.SFBaseResultSet;
import net.snowflake.client.core.SFResultSet;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SFStatementType;

import java.math.BigDecimal;
import java.sql.Clob;
import java.sql.Date;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.TimeZone;
import java.util.logging.Level;

/**
 * Snowflake ResultSet implementation
 * <p>
 * @author jhuang
 */
class SnowflakeResultSetV1 extends SnowflakeBaseResultSet
{
  private SFBaseResultSet sfBaseResultSet;
  private Statement statement;

  /**
   * Constructor takes an inputstream from the API response that we get from
   * executing a SQL statement.
   * <p>
   * The constructor will fetch the first row (if any) so that it can initialize
   * the ResultSetMetaData.
   * </p>
   *
   * @param sfBaseResultSet snowflake core base result rest object
   * @param statement query statement that generates this result set
   * @throws SQLException if failed to construct snowflake result set metadata
   */
  SnowflakeResultSetV1(SFBaseResultSet sfBaseResultSet, Statement statement)
          throws SQLException
  {
    this.sfBaseResultSet = sfBaseResultSet;
    this.statement = statement;
    try
    {
      this.resultSetMetaData =
          new SnowflakeResultSetMetaDataV1(sfBaseResultSet.getMetaData());
    }
    catch (SFException ex)
    {
      throw new SnowflakeSQLException(ex.getCause(),
          ex.getSqlState(), ex.getVendorCode(), ex.getParams());
    }
  }


  /**
   * Advance to next row
   *
   * @return true if next row exists, false otherwise
   * @throws java.sql.SQLException if failed to move to the next row
   */
  @Override
  public boolean next() throws SQLException
  {
    try
    {
      return sfBaseResultSet.next();
    }
    catch (SFException ex)
    {
      throw new SnowflakeSQLException(ex.getCause(),
          ex.getSqlState(), ex.getVendorCode(), ex.getParams());
    }
  }

  @Override
  public void close() throws SQLException
  {
    sfBaseResultSet.close();
  }


  public boolean wasNull()
  {
    return sfBaseResultSet.wasNull();
  }

  public String getString(int columnIndex) throws SQLException
  {
    try
    {
      return sfBaseResultSet.getString(columnIndex);
    }
    catch (SFException ex)
    {
      throw new SnowflakeSQLException(ex.getCause(),
          ex.getSqlState(), ex.getVendorCode(), ex.getParams());
    }
  }

  public Clob getClob(int columnIndex) throws SQLException
  {
    try
    {
      String content = sfBaseResultSet.getString(columnIndex);
      Clob clob = new SnowflakeClob(content);
      return clob;
    }
    catch (SFException ex)
    {
      throw new SnowflakeSQLException(ex.getCause(),
          ex.getSqlState(), ex.getVendorCode(), ex.getParams());
    }
  }

  public boolean getBoolean(int columnIndex) throws SQLException
  {
    try
    {
      return sfBaseResultSet.getBoolean(columnIndex);
    }
    catch (SFException ex)
    {
      throw new SnowflakeSQLException(ex.getCause(),
          ex.getSqlState(), ex.getVendorCode(), ex.getParams());
    }
  }

  public short getShort(int columnIndex) throws SQLException
  {
    try
    {
      return sfBaseResultSet.getShort(columnIndex);
    }
    catch (SFException ex)
    {
      throw new SnowflakeSQLException(ex.getCause(),
          ex.getSqlState(), ex.getVendorCode(), ex.getParams());
    }
  }

  public int getInt(int columnIndex) throws SQLException
  {
    try
    {
      return sfBaseResultSet.getInt(columnIndex);
    }
    catch (SFException ex)
    {
      throw new SnowflakeSQLException(ex.getCause(),
          ex.getSqlState(), ex.getVendorCode(), ex.getParams());
    }
  }

  public long getLong(int columnIndex) throws SQLException
  {
    try
    {
      return sfBaseResultSet.getLong(columnIndex);
    }
    catch (SFException ex)
    {
      throw new SnowflakeSQLException(ex.getCause(),
          ex.getSqlState(), ex.getVendorCode(), ex.getParams());
    }
  }

  public float getFloat(int columnIndex) throws SQLException
  {
    try
    {
      return sfBaseResultSet.getFloat(columnIndex);
    }
    catch (SFException ex)
    {
      throw new SnowflakeSQLException(ex.getCause(),
          ex.getSqlState(), ex.getVendorCode(), ex.getParams());
    }

  }

  public double getDouble(int columnIndex) throws SQLException
  {
    try
    {
      return sfBaseResultSet.getDouble(columnIndex);
    }
    catch (SFException ex)
    {
      throw new SnowflakeSQLException(ex.getCause(),
          ex.getSqlState(), ex.getVendorCode(), ex.getParams());
    }
  }

  public Date getDate(int columnIndex, TimeZone tz) throws SQLException
  {
    try
    {
      return sfBaseResultSet.getDate(columnIndex);
    }
    catch (SFException ex)
    {
      throw new SnowflakeSQLException(ex.getCause(),
          ex.getSqlState(), ex.getVendorCode(), ex.getParams());
    }
  }

  public Time getTime(int columnIndex) throws SQLException
  {
    try
    {
      return sfBaseResultSet.getTime(columnIndex);
    }
    catch (SFException ex)
    {
      throw new SnowflakeSQLException(ex.getCause(),
          ex.getSqlState(), ex.getVendorCode(), ex.getParams());
    }
  }

  public Timestamp getTimestamp(int columnIndex, TimeZone tz)
      throws SQLException
  {
    try
    {
      return sfBaseResultSet.getTimestamp(columnIndex, tz);
    }
    catch (SFException ex)
    {
      throw new SnowflakeSQLException(ex.getCause(),
          ex.getSqlState(), ex.getVendorCode(), ex.getParams());
    }
  }

  public ResultSetMetaData getMetaData() throws SQLException
  {
    logger.debug("public ResultSetMetaData getMetaData()");

    return resultSetMetaData;
  }

  public Object getObject(int columnIndex) throws SQLException
  {
    try
    {
      return sfBaseResultSet.getObject(columnIndex);
    }
    catch (SFException ex)
    {
      throw new SnowflakeSQLException(ex.getCause(),
          ex.getSqlState(), ex.getVendorCode(), ex.getParams());
    }
  }

  public BigDecimal getBigDecimal(int columnIndex) throws SQLException
  {
    try
    {
      return sfBaseResultSet.getBigDecimal(columnIndex);
    }
    catch (SFException ex)
    {
      throw new SnowflakeSQLException(ex.getCause(),
          ex.getSqlState(), ex.getVendorCode(), ex.getParams());
    }
  }

  public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException
  {
    try
    {
      return sfBaseResultSet.getBigDecimal(columnIndex, scale);
    }
    catch (SFException ex)
    {
      throw new SnowflakeSQLException(ex.getCause(),
          ex.getSqlState(), ex.getVendorCode(), ex.getParams());
    }
  }

  public byte[] getBytes(int columnIndex) throws SQLException
  {
    try
    {
      return sfBaseResultSet.getBytes(columnIndex);
    }
    catch (SFException ex)
    {
      throw new SnowflakeSQLException(ex.getCause(),
          ex.getSqlState(), ex.getVendorCode(), ex.getParams());
    }
  }

  public int getRow() throws SQLException
  {
    logger.debug("public int getRow()");

    return sfBaseResultSet.getRow();
  }

  public boolean isFirst() throws SQLException
  {
    logger.debug("public boolean isFirst()");
    
    return sfBaseResultSet.isFirst();
  }

  public Statement getStatement() throws SQLException
  {
    return this.statement;
  }

  public boolean isClosed() throws SQLException
  {
    return sfBaseResultSet.isClosed();
  }

  @Override
  public boolean isLast() throws SQLException
  {
    return sfBaseResultSet.isLast();
  }

  @Override
  public boolean isAfterLast() throws SQLException
  {
    return sfBaseResultSet.isAfterLast();
  }

  @Override
  public boolean isBeforeFirst() throws SQLException
  {
    return sfBaseResultSet.isBeforeFirst();
  }
}
