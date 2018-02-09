/*
 * Copyright (c) 2012-2018 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import net.snowflake.common.core.SqlState;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.sql.Types;
import java.util.TimeZone;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

/**
 * Base class for query result set and metadata result set
 *
 * @author jhuang
 */
abstract class SnowflakeBaseResultSet implements ResultSet
{

  static final SFLogger logger = SFLoggerFactory.getLogger(SnowflakeBaseResultSet.class);

  protected Statement statement;

  private boolean wasNull = false;

  protected Object[] nextRow = null;

  protected SnowflakeResultSetMetaData resultSetMetaData = null;

  protected int row = 0;

  protected Map<String, Object> parameters = new HashMap<>();

  private int fetchSize = 0;

  @Override
  abstract public boolean next() throws SQLException;

  @Override
  abstract public void close() throws SQLException;

  @Override
  public boolean wasNull() throws SQLException
  {
    logger.debug("public boolean wasNull() returning {}", wasNull);

    return wasNull;
  }

  @Override
  public String getString(int columnIndex) throws SQLException
  {
    logger.debug("public String getString(int columnIndex)");

    // Column index starts from 1, not 0.
    Object obj = getObjectInternal(columnIndex);

    return obj == null ? null : obj.toString();
  }

  @Override
  public boolean getBoolean(int columnIndex) throws SQLException
  {
    logger.debug(
               "public boolean getBoolean(int columnIndex)");

    // Column index starts from 1, not 0.
    Object obj = getObjectInternal(columnIndex);

    if (obj == null)
      return false;

    if (obj instanceof String)
    {
      if (obj.toString().equals("1"))
      {
        return Boolean.TRUE;
      }
      return Boolean.FALSE;
    }
    else
    {
      return ((Boolean) obj).booleanValue();
    }
  }

  @Override
  public byte getByte(int columnIndex) throws SQLException
  {
    logger.debug("public byte getByte(int columnIndex)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public short getShort(int columnIndex) throws SQLException
  {
    logger.debug("public short getShort(int columnIndex)");

    // Column index starts from 1, not 0.
    Object obj = getObjectInternal(columnIndex);

    if (obj == null)
      return 0;

    if (obj instanceof String)
    {
      return (Short.valueOf((String) obj)).shortValue();
    }
    else
    {
      return ((Number) obj).shortValue();
    }
  }

  @Override
  public int getInt(int columnIndex) throws SQLException
  {
    logger.debug("public int getInt(int columnIndex)");

    // Column index starts from 1, not 0.
    Object obj = getObjectInternal(columnIndex);

    if (obj == null)
      return 0;

    if (obj instanceof String)
    {
      return (Integer.valueOf((String) obj)).intValue();
    }
    else
    {
      return ((Number) obj).intValue();
    }

  }

  @Override
  public long getLong(int columnIndex) throws SQLException
  {
    logger.debug("public long getLong(int columnIndex)");

    // Column index starts from 1, not 0.
    Object obj = getObjectInternal(columnIndex);

    if (obj == null)
      return 0;

    try
    {
      if (obj instanceof String)
      {
        return (Long.valueOf((String) obj)).longValue();
      }
      else
      {
        return ((Number) obj).longValue();
      }
    }
    catch (NumberFormatException nfe)
    {
      throw new SnowflakeSQLException(SqlState.INTERNAL_ERROR,
                                      ErrorCode.INTERNAL_ERROR.getMessageCode(),
                                      "Invalid long: " + (String) obj);
    }
  }

  @Override
  public float getFloat(int columnIndex) throws SQLException
  {
    logger.debug("public float getFloat(int columnIndex)");

    // Column index starts from 1, not 0.
    Object obj = getObjectInternal(columnIndex);

    if (obj == null)
      return 0;

    if (obj instanceof String)
    {
      return (Float.valueOf((String) obj)).floatValue();
    }
    else
    {
      return ((Number) obj).floatValue();
    }
  }

  @Override
  public double getDouble(int columnIndex) throws SQLException
  {
    logger.debug("public double getDouble(int columnIndex)");

    // Column index starts from 1, not 0.
    Object obj = getObjectInternal(columnIndex);

    // snow-11974: null for getDouble should return 0
    if (obj == null)
      return 0;

    if (obj instanceof String)
    {
      return (Double.valueOf((String) obj)).doubleValue();
    }
    else
    {
      return ((Number) obj).doubleValue();
    }
  }

  /**
   * @deprecated
   */
  @Override
  public BigDecimal getBigDecimal(int columnIndex, int scale)
          throws SQLException
  {
    logger.debug(
               "public BigDecimal getBigDecimal(int columnIndex, int scale)");

    BigDecimal value = null;

    // Column index starts from 1, not 0.
    Object obj = getObjectInternal(columnIndex);

    if (obj == null)
      return null;

    if (obj instanceof String)
    {
      value = new BigDecimal((String) obj);
    }
    else
    {
      value = new BigDecimal(obj.toString());
    }

    value = value.setScale(scale, RoundingMode.HALF_UP);

    return value;
  }

  @Override
  abstract public byte[] getBytes(int columnIndex) throws SQLException;

  abstract public Date getDate(int columnIndex, TimeZone tz) throws SQLException;

  @Override
  public Date getDate(int columnIndex) throws SQLException
  {
    return getDate(columnIndex, TimeZone.getDefault());
  }

  @Override
  abstract public Time getTime(int columnIndex) throws SQLException;

  @Override
  public Timestamp getTimestamp(int columnIndex) throws SQLException
  {
    return getTimestamp(columnIndex, TimeZone.getDefault());
  }

  abstract public Timestamp getTimestamp(int columnIndex, TimeZone tz)
      throws SQLException;

  /**
   * Parse seconds since epoch with both seconds and fractional seconds after
   * decimal point (e.g 123.456 with a scale of 3) to a representation with
   * fractions normalized to an integer (e.g. 123456)
   * @param secondsSinceEpochStr
   * @param scale
   * @return a BigDecimal containing the number of fractional seconds since
   * epoch.
   */
  private BigDecimal parseSecondsSinceEpoch(String secondsSinceEpochStr,
                                                       int scale)
  {
      // seconds since epoch has both seconds and fractional seconds after decimal
      // point. Ex: 134567890.12345678
      // Note: can actually contain timezone in the lowest part
      // Example: obj is e.g. "123.456" (scale=3)
      //          Then, secondsSinceEpoch is 123.456
      BigDecimal secondsSinceEpoch = new BigDecimal(secondsSinceEpochStr);

      // Representation with fractions normalized to an integer
      // Note: can actually contain timezone in the lowest part
      // Example: fractionsSinceEpoch is 123456
      return secondsSinceEpoch.scaleByPowerOfTen(scale);
  }

  @Override
  public InputStream getAsciiStream(int columnIndex) throws SQLException
  {
    logger.debug(
               "public InputStream getAsciiStream(int columnIndex)");

    throw new SQLFeatureNotSupportedException();
  }

  /**
   * @deprecated
   */
  @Override
  public InputStream getUnicodeStream(int columnIndex) throws SQLException
  {
    logger.debug(
               "public InputStream getUnicodeStream(int columnIndex)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public InputStream getBinaryStream(int columnIndex) throws SQLException
  {
    logger.debug(
               "public InputStream getBinaryStream(int columnIndex)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getString(String columnLabel) throws SQLException
  {
    logger.debug(
               "public String getString(String columnLabel)");

    return getString(findColumn(columnLabel));
  }

  @Override
  public boolean getBoolean(String columnLabel) throws SQLException
  {
    logger.debug(
               "public boolean getBoolean(String columnLabel)");

    return getBoolean(findColumn(columnLabel));
  }

  @Override
  public byte getByte(String columnLabel) throws SQLException
  {
    logger.debug("public byte getByte(String columnLabel)");

    return getByte(findColumn(columnLabel));
  }

  @Override
  public short getShort(String columnLabel) throws SQLException
  {
    logger.debug(
               "public short getShort(String columnLabel)");

    return getShort(findColumn(columnLabel));
  }

  @Override
  public int getInt(String columnLabel) throws SQLException
  {
    logger.debug("public int getInt(String columnLabel)");

    return getInt(findColumn(columnLabel));
  }

  @Override
  public long getLong(String columnLabel) throws SQLException
  {
    logger.debug("public long getLong(String columnLabel)");

    return getLong(findColumn(columnLabel));
  }

  @Override
  public float getFloat(String columnLabel) throws SQLException
  {
    logger.debug(
               "public float getFloat(String columnLabel)");

    return getFloat(findColumn(columnLabel));
  }

  @Override
  public double getDouble(String columnLabel) throws SQLException
  {
    logger.debug(
               "public double getDouble(String columnLabel)");

    return getDouble(findColumn(columnLabel));
  }

  /**
   * @deprecated
   */
  @Override
  public BigDecimal getBigDecimal(String columnLabel, int scale)
          throws SQLException
  {
    logger.debug(
               "public BigDecimal getBigDecimal(String columnLabel, "
               + "int scale)");

    return getBigDecimal(findColumn(columnLabel), scale);
  }

  @Override
  public byte[] getBytes(String columnLabel) throws SQLException
  {
    logger.debug(
               "public byte[] getBytes(String columnLabel)");

    return getBytes(findColumn(columnLabel));
  }

  @Override
  public Date getDate(String columnLabel) throws SQLException
  {
    logger.debug("public Date getDate(String columnLabel)");

    return getDate(findColumn(columnLabel));
  }

  @Override
  public Time getTime(String columnLabel) throws SQLException
  {
    logger.debug("public Time getTime(String columnLabel)");

    return getTime(findColumn(columnLabel));
  }

  @Override
  public Timestamp getTimestamp(String columnLabel) throws SQLException
  {
    logger.debug(
               "public Timestamp getTimestamp(String columnLabel)");

    return getTimestamp(findColumn(columnLabel));
  }

  @Override
  public InputStream getAsciiStream(String columnLabel) throws SQLException
  {
    logger.debug(
               "public InputStream getAsciiStream(String columnLabel)");

    throw new SQLFeatureNotSupportedException();
  }

  /**
   * @deprecated
   */
  @Override
  public InputStream getUnicodeStream(String columnLabel) throws SQLException
  {
    logger.debug(
               "public InputStream getUnicodeStream(String columnLabel)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public InputStream getBinaryStream(String columnLabel) throws SQLException
  {
    logger.debug(
               "public InputStream getBinaryStream(String columnLabel)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public SQLWarning getWarnings() throws SQLException
  {
    logger.debug("public SQLWarning getWarnings()");

    return null;
  }

  @Override
  public void clearWarnings() throws SQLException
  {
    logger.debug("public void clearWarnings()");

    // do nothing since warnings are not tracked
  }

  @Override
  public String getCursorName() throws SQLException
  {
    logger.debug("public String getCursorName()");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException
  {
    logger.debug("public ResultSetMetaData getMetaData()");

    return resultSetMetaData;
  }

  protected Object getObjectInternal(int columnIndex) throws SQLException
  {
    logger.debug(
               "public Object getObjectInternal(int columnIndex)");

    if (nextRow == null)
    {
      throw new SQLException("No row found.");
    }

    if (columnIndex > nextRow.length)
    {
      throw new SQLException("Invalid column index: " + columnIndex);
    }

    wasNull = nextRow[columnIndex - 1] == null;

    logger.debug(
               "Returning column: " + columnIndex + ": "
               + nextRow[columnIndex - 1]);

    return nextRow[columnIndex - 1];
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException
  {
    logger.debug(
               "public Object getObject(int columnIndex)");

    int type = resultSetMetaData.getColumnType(columnIndex);

    Object internalObj = getObjectInternal(columnIndex);
    if (internalObj == null)
      return null;

    switch(type)
    {
      case Types.VARCHAR:
      case Types.CHAR:
        return getString(columnIndex);

      case Types.BINARY:
        return getBytes(columnIndex);

      case Types.INTEGER:
      case Types.SMALLINT:
        return Integer.valueOf(getInt(columnIndex));

      case Types.DECIMAL:
        return getBigDecimal(columnIndex);

      case Types.BIGINT:
        return getLong(columnIndex);

      case Types.DOUBLE:
        return Double.valueOf(getDouble(columnIndex));

      case Types.TIMESTAMP:
        return getTimestamp(columnIndex);

      case Types.DATE:
        return getDate(columnIndex);

      case Types.TIME:
        return getTime(columnIndex);

      case Types.BOOLEAN:
        return getBoolean(columnIndex);

      default:
        throw new SQLFeatureNotSupportedException();
    }
  }

  @Override
  public Object getObject(String columnLabel) throws SQLException
  {
    logger.debug(
               "public Object getObject(String columnLabel)");

    return getObject(findColumn(columnLabel));
  }

  @Override
  public int findColumn(String columnLabel) throws SQLException
  {
    logger.debug(
               "public int findColumn(String columnLabel)");

    int columnIndex = resultSetMetaData.getColumnIndex(columnLabel);

    if (columnIndex == -1)
    {
      throw new SQLException("Column not found: " + columnLabel);
    }
    else
    {
      return ++columnIndex;
    }
  }

  @Override
  public Reader getCharacterStream(int columnIndex) throws SQLException
  {
    logger.debug(
               "public Reader getCharacterStream(int columnIndex)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Reader getCharacterStream(String columnLabel) throws SQLException
  {
    logger.debug(
               "public Reader getCharacterStream(String columnLabel)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex) throws SQLException
  {
    logger.debug(
               "public BigDecimal getBigDecimal(int columnIndex)");

    BigDecimal value = null;

    // Column index starts from 1, not 0.
    Object obj = getObjectInternal(columnIndex);

    if (obj == null)
      return null;

    if (obj instanceof String)
    {
      value = new BigDecimal((String) obj);
    }
    else
    {
      value = new BigDecimal(obj.toString());
    }

    return value;
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel) throws SQLException
  {
    logger.debug(
               "public BigDecimal getBigDecimal(String columnLabel)");

    return getBigDecimal(findColumn(columnLabel));
  }

  @Override
  public boolean isBeforeFirst() throws SQLException
  {
    logger.debug("public boolean isBeforeFirst()");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isAfterLast() throws SQLException
  {
    logger.debug("public boolean isAfterLast()");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isFirst() throws SQLException
  {
    logger.debug("public boolean isFirst()");

    return row == 1;
  }

  @Override
  public boolean isLast() throws SQLException
  {
    logger.debug("public boolean isLast()");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void beforeFirst() throws SQLException
  {
    logger.debug("public void beforeFirst()");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void afterLast() throws SQLException
  {
    logger.debug("public void afterLast()");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean first() throws SQLException
  {
    logger.debug("public boolean first()");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean last() throws SQLException
  {
    logger.debug("public boolean last()");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getRow() throws SQLException
  {
    logger.debug("public int getRow()");

    return row;
  }

  @Override
  public boolean absolute(int row) throws SQLException
  {
    logger.debug("public boolean absolute(int row)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean relative(int rows) throws SQLException
  {
    logger.debug("public boolean relative(int rows)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean previous() throws SQLException
  {
    logger.debug("public boolean previous()");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException
  {
    logger.debug(
               "public void setFetchDirection(int direction)");

    if (direction != ResultSet.FETCH_FORWARD)
      throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getFetchDirection() throws SQLException
  {
    logger.debug("public int getFetchDirection()");

    return ResultSet.FETCH_FORWARD;
  }

  @Override
  public void setFetchSize(int rows) throws SQLException
  {
    logger.debug("public void setFetchSize(int rows)");

    this.fetchSize = rows;
  }

  @Override
  public int getFetchSize() throws SQLException
  {
    logger.debug("public int getFetchSize()");

    return this.fetchSize;
  }

  @Override
  public int getType() throws SQLException
  {
    logger.debug("public int getType()");

    return ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override
  public int getConcurrency() throws SQLException
  {
    logger.debug("public int getConcurrency()");

    throw new SQLFeatureNotSupportedException(
        "Feature not supported",
        ErrorCode.FEATURE_UNSUPPORTED.getSqlState(),
        ErrorCode.FEATURE_UNSUPPORTED.getMessageCode());
  }

  @Override
  public boolean rowUpdated() throws SQLException
  {
    logger.debug("public boolean rowUpdated()");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean rowInserted() throws SQLException
  {
    logger.debug("public boolean rowInserted()");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean rowDeleted() throws SQLException
  {
    logger.debug("public boolean rowDeleted()");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNull(int columnIndex) throws SQLException
  {
    logger.debug("public void updateNull(int columnIndex)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBoolean(int columnIndex, boolean x) throws SQLException
  {
    logger.debug(
               "public void updateBoolean(int columnIndex, boolean x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateByte(int columnIndex, byte x) throws SQLException
  {
    logger.debug(
               "public void updateByte(int columnIndex, byte x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateShort(int columnIndex, short x) throws SQLException
  {
    logger.debug(
               "public void updateShort(int columnIndex, short x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateInt(int columnIndex, int x) throws SQLException
  {
    logger.debug(
               "public void updateInt(int columnIndex, int x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateLong(int columnIndex, long x) throws SQLException
  {
    logger.debug(
               "public void updateLong(int columnIndex, long x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateFloat(int columnIndex, float x) throws SQLException
  {
    logger.debug(
               "public void updateFloat(int columnIndex, float x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateDouble(int columnIndex, double x) throws SQLException
  {
    logger.debug(
               "public void updateDouble(int columnIndex, double x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBigDecimal(int columnIndex, BigDecimal x)
          throws SQLException
  {
    logger.debug(
               "public void updateBigDecimal(int columnIndex, BigDecimal x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateString(int columnIndex, String x) throws SQLException
  {
    logger.debug(
               "public void updateString(int columnIndex, String x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBytes(int columnIndex, byte[] x) throws SQLException
  {
    logger.debug(
               "public void updateBytes(int columnIndex, byte[] x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateDate(int columnIndex, Date x) throws SQLException
  {
    logger.debug(
               "public void updateDate(int columnIndex, Date x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateTime(int columnIndex, Time x) throws SQLException
  {
    logger.debug(
               "public void updateTime(int columnIndex, Time x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException
  {
    logger.debug(
               "public void updateTimestamp(int columnIndex, Timestamp x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, int length)
          throws SQLException
  {
    logger.debug(
               "public void updateAsciiStream(int columnIndex, "
               + "InputStream x, int length)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, int length)
          throws SQLException
  {
    logger.debug(
               "public void updateBinaryStream(int columnIndex, "
               + "InputStream x, int length)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, int length)
          throws SQLException
  {
    logger.debug(
               "public void updateCharacterStream(int columnIndex, "
               + "Reader x, int length)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateObject(int columnIndex, Object x, int scaleOrLength)
          throws SQLException
  {
    logger.debug(
               "public void updateObject(int columnIndex, Object x, "
               + "int scaleOrLength)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateObject(int columnIndex, Object x) throws SQLException
  {
    logger.debug(
               "public void updateObject(int columnIndex, Object x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNull(String columnLabel) throws SQLException
  {
    logger.debug(
               "public void updateNull(String columnLabel)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBoolean(String columnLabel, boolean x) throws SQLException
  {
    logger.debug(
               "public void updateBoolean(String columnLabel, boolean x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateByte(String columnLabel, byte x) throws SQLException
  {
    logger.debug(
               "public void updateByte(String columnLabel, byte x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateShort(String columnLabel, short x) throws SQLException
  {
    logger.debug(
               "public void updateShort(String columnLabel, short x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateInt(String columnLabel, int x) throws SQLException
  {
    logger.debug(
               "public void updateInt(String columnLabel, int x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateLong(String columnLabel, long x) throws SQLException
  {
    logger.debug(
               "public void updateLong(String columnLabel, long x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateFloat(String columnLabel, float x) throws SQLException
  {
    logger.debug(
               "public void updateFloat(String columnLabel, float x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateDouble(String columnLabel, double x) throws SQLException
  {
    logger.debug(
               "public void updateDouble(String columnLabel, double x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBigDecimal(String columnLabel, BigDecimal x)
          throws SQLException
  {
    logger.debug(
               "public void updateBigDecimal(String columnLabel, "
               + "BigDecimal x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateString(String columnLabel, String x) throws SQLException
  {
    logger.debug(
               "public void updateString(String columnLabel, String x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBytes(String columnLabel, byte[] x) throws SQLException
  {
    logger.debug(
               "public void updateBytes(String columnLabel, byte[] x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateDate(String columnLabel, Date x) throws SQLException
  {
    logger.debug(
               "public void updateDate(String columnLabel, Date x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateTime(String columnLabel, Time x) throws SQLException
  {
    logger.debug(
               "public void updateTime(String columnLabel, Time x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateTimestamp(String columnLabel, Timestamp x)
          throws SQLException
  {
    logger.debug(
               "public void updateTimestamp(String columnLabel, Timestamp x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, int length)
          throws SQLException
  {
    logger.debug(
               "public void updateAsciiStream(String columnLabel, "
               + "InputStream x, int length)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, int length)
          throws SQLException
  {
    logger.debug(
               "public void updateBinaryStream(String columnLabel, "
               + "InputStream x, int length)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader,
                                    int length) throws SQLException
  {
    logger.debug(
               "public void updateCharacterStream(String columnLabel, "
               + "Reader reader,int length)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateObject(String columnLabel, Object x, int scaleOrLength)
          throws SQLException
  {
    logger.debug(
               "public void updateObject(String columnLabel, Object x, "
               + "int scaleOrLength)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateObject(String columnLabel, Object x) throws SQLException
  {
    logger.debug(
               "public void updateObject(String columnLabel, Object x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void insertRow() throws SQLException
  {
    logger.debug("public void insertRow()");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateRow() throws SQLException
  {
    logger.debug("public void updateRow()");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void deleteRow() throws SQLException
  {
    logger.debug("public void deleteRow()");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void refreshRow() throws SQLException
  {
    logger.debug("public void refreshRow()");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void cancelRowUpdates() throws SQLException
  {
    logger.debug("public void cancelRowUpdates()");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void moveToInsertRow() throws SQLException
  {
    logger.debug("public void moveToInsertRow()");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void moveToCurrentRow() throws SQLException
  {
    logger.debug("public void moveToCurrentRow()");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Statement getStatement() throws SQLException
  {
    logger.debug("public Statement getStatement()");

    return statement;
  }

  @Override
  public Object getObject(int columnIndex,
                          Map<String, Class<?>> map) throws SQLException
  {
    logger.debug(
               "public Object getObject(int columnIndex, Map<String, "
               + "Class<?>> map)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Ref getRef(int columnIndex) throws SQLException
  {
    logger.debug("public Ref getRef(int columnIndex)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Blob getBlob(int columnIndex) throws SQLException
  {
    logger.debug("public Blob getBlob(int columnIndex)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Clob getClob(int columnIndex) throws SQLException
  {
    logger.debug("public Clob getClob(int columnIndex)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Array getArray(int columnIndex) throws SQLException
  {
    logger.debug("public Array getArray(int columnIndex)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Object getObject(String columnLabel,
                          Map<String, Class<?>> map) throws SQLException
  {
    logger.debug(
               "public Object getObject(String columnLabel, "
               + "Map<String, Class<?>> map)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Ref getRef(String columnLabel) throws SQLException
  {
    logger.debug("public Ref getRef(String columnLabel)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Blob getBlob(String columnLabel) throws SQLException
  {
    logger.debug("public Blob getBlob(String columnLabel)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Clob getClob(String columnLabel) throws SQLException
  {
    logger.debug("public Clob getClob(String columnLabel)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Array getArray(String columnLabel) throws SQLException
  {
    logger.debug(
               "public Array getArray(String columnLabel)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Date getDate(int columnIndex, Calendar cal) throws SQLException
  {
    logger.debug(
               "public Date getDate(int columnIndex, Calendar cal)");

    return getDate(columnIndex, cal.getTimeZone());
  }

  @Override
  public Date getDate(String columnLabel, Calendar cal) throws SQLException
  {
    logger.debug(
               "public Date getDate(String columnLabel, Calendar cal)");

    return getDate(findColumn(columnLabel), cal.getTimeZone());
  }

  @Override
  public Time getTime(int columnIndex, Calendar cal) throws SQLException
  {
    logger.debug(
               "public Time getTime(int columnIndex, Calendar cal)");

    return getTime(columnIndex);
  }

  @Override
  public Time getTime(String columnLabel, Calendar cal) throws SQLException
  {
    logger.debug(
               "public Time getTime(String columnLabel, Calendar cal)");

    return getTime(columnLabel);
  }

  @Override
  public Timestamp getTimestamp(int columnIndex, Calendar cal)
          throws SQLException
  {
    logger.debug(
               "public Timestamp getTimestamp(int columnIndex, Calendar cal)");

    return getTimestamp(columnIndex, cal.getTimeZone());
  }

  @Override
  public Timestamp getTimestamp(String columnLabel, Calendar cal)
          throws SQLException
  {
    logger.debug(
               "public Timestamp getTimestamp(String columnLabel, "
               + "Calendar cal)");

    return getTimestamp(findColumn(columnLabel), cal.getTimeZone());
  }

  @Override
  public URL getURL(int columnIndex) throws SQLException
  {
    logger.debug("public URL getURL(int columnIndex)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public URL getURL(String columnLabel) throws SQLException
  {
    logger.debug("public URL getURL(String columnLabel)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateRef(int columnIndex, Ref x) throws SQLException
  {
    logger.debug(
               "public void updateRef(int columnIndex, Ref x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateRef(String columnLabel, Ref x) throws SQLException
  {
    logger.debug(
               "public void updateRef(String columnLabel, Ref x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBlob(int columnIndex, Blob x) throws SQLException
  {
    logger.debug(
               "public void updateBlob(int columnIndex, Blob x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBlob(String columnLabel, Blob x) throws SQLException
  {
    logger.debug(
               "public void updateBlob(String columnLabel, Blob x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateClob(int columnIndex, Clob x) throws SQLException
  {
    logger.debug(
               "public void updateClob(int columnIndex, Clob x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateClob(String columnLabel, Clob x) throws SQLException
  {
    logger.debug(
               "public void updateClob(String columnLabel, Clob x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateArray(int columnIndex, Array x) throws SQLException
  {
    logger.debug(
               "public void updateArray(int columnIndex, Array x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateArray(String columnLabel, Array x) throws SQLException
  {
    logger.debug(
               "public void updateArray(String columnLabel, Array x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public RowId getRowId(int columnIndex) throws SQLException
  {
    logger.debug("public RowId getRowId(int columnIndex)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public RowId getRowId(String columnLabel) throws SQLException
  {
    logger.debug(
               "public RowId getRowId(String columnLabel)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateRowId(int columnIndex, RowId x) throws SQLException
  {
    logger.debug(
               "public void updateRowId(int columnIndex, RowId x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateRowId(String columnLabel, RowId x) throws SQLException
  {
    logger.debug(
               "public void updateRowId(String columnLabel, RowId x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getHoldability() throws SQLException
  {
    logger.debug("public int getHoldability()");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isClosed() throws SQLException
  {
    logger.debug("public boolean isClosed()");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNString(int columnIndex, String nString) throws SQLException
  {
    logger.debug(
               "public void updateNString(int columnIndex, String nString)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNString(String columnLabel, String nString)
          throws SQLException
  {
    logger.debug(
               "public void updateNString(String columnLabel, String nString)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNClob(int columnIndex, NClob nClob) throws SQLException
  {
    logger.debug(
               "public void updateNClob(int columnIndex, NClob nClob)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNClob(String columnLabel, NClob nClob) throws SQLException
  {
    logger.debug(
               "public void updateNClob(String columnLabel, NClob nClob)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public NClob getNClob(int columnIndex) throws SQLException
  {
    logger.debug("public NClob getNClob(int columnIndex)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public NClob getNClob(String columnLabel) throws SQLException
  {
    logger.debug(
               "public NClob getNClob(String columnLabel)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public SQLXML getSQLXML(int columnIndex) throws SQLException
  {
    logger.debug(
               "public SQLXML getSQLXML(int columnIndex)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public SQLXML getSQLXML(String columnLabel) throws SQLException
  {
    logger.debug(
               "public SQLXML getSQLXML(String columnLabel)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateSQLXML(int columnIndex, SQLXML xmlObject)
          throws SQLException
  {
    logger.debug(
               "public void updateSQLXML(int columnIndex, SQLXML xmlObject)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateSQLXML(String columnLabel, SQLXML xmlObject)
          throws SQLException
  {
    logger.debug(
               "public void updateSQLXML(String columnLabel, SQLXML xmlObject)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getNString(int columnIndex) throws SQLException
  {
    logger.debug(
               "public String getNString(int columnIndex)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getNString(String columnLabel) throws SQLException
  {
    logger.debug(
               "public String getNString(String columnLabel)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Reader getNCharacterStream(int columnIndex) throws SQLException
  {
    logger.debug(
               "public Reader getNCharacterStream(int columnIndex)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Reader getNCharacterStream(String columnLabel) throws SQLException
  {
    logger.debug(
                "public Reader getNCharacterStream(String columnLabel)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x, long length)
          throws SQLException
  {
    logger.debug(
               "public void updateNCharacterStream(int columnIndex, "
               + "Reader x, long length)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader,
                                     long length) throws SQLException
  {
    logger.debug(
               "public void updateNCharacterStream(String columnLabel, "
               + "Reader reader,long length)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, long length)
          throws SQLException
  {
    logger.debug(
               "public void updateAsciiStream(int columnIndex, "
               + "InputStream x, long length)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, long length)
          throws SQLException
  {
    logger.debug(
               "public void updateBinaryStream(int columnIndex, "
               + "InputStream x, long length)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, long length)
          throws SQLException
  {
    logger.debug(
               "public void updateCharacterStream(int columnIndex, Reader x, "
               + "long length)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, long length)
          throws SQLException
  {
    logger.debug(
               "public void updateAsciiStream(String columnLabel, "
               + "InputStream x, long length)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, long length)
          throws SQLException
  {
    logger.debug(
               "public void updateBinaryStream(String columnLabel, "
               + "InputStream x, long length)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader,
                                    long length) throws SQLException
  {
    logger.debug(
               "public void updateCharacterStream(String columnLabel, "
               + "Reader reader,long length)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream, long length)
          throws SQLException
  {
    logger.debug(
               "public void updateBlob(int columnIndex, InputStream "
               + "inputStream, long length)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream,
                         long length) throws SQLException
  {
    logger.debug(
               "public void updateBlob(String columnLabel, "
               + "InputStream inputStream,long length)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateClob(int columnIndex, Reader reader, long length)
          throws SQLException
  {
    logger.debug(
               "public void updateClob(int columnIndex, Reader reader, "
               + "long length)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateClob(String columnLabel, Reader reader, long length)
          throws SQLException
  {
    logger.debug(
               "public void updateClob(String columnLabel, Reader reader, "
               + "long length)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader, long length)
          throws SQLException
  {
    logger.debug(
               "public void updateNClob(int columnIndex, Reader reader, "
               + "long length)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader, long length)
          throws SQLException
  {
    logger.debug(
               "public void updateNClob(String columnLabel, Reader reader, "
               + "long length)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x)
          throws SQLException
  {
    logger.debug(
               "public void updateNCharacterStream(int columnIndex, Reader x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader)
          throws SQLException
  {
    logger.debug(
               "public void updateNCharacterStream(String columnLabel, "
               + "Reader reader)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x)
          throws SQLException
  {
    logger.debug(
               "public void updateAsciiStream(int columnIndex, InputStream x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x)
          throws SQLException
  {
    logger.debug(
               "public void updateBinaryStream(int columnIndex, InputStream x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x)
          throws SQLException
  {
    logger.debug(
               "public void updateCharacterStream(int columnIndex, Reader x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x)
          throws SQLException
  {
    logger.debug(
               "public void updateAsciiStream(String columnLabel, InputStream x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x)
          throws SQLException
  {
    logger.debug(
               "public void updateBinaryStream(String columnLabel, InputStream x)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader)
          throws SQLException
  {
    logger.debug(
               "public void updateCharacterStream(String columnLabel, "
               + "Reader reader)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream)
          throws SQLException
  {
    logger.debug(
               "public void updateBlob(int columnIndex, InputStream inputStream)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream)
          throws SQLException
  {
    logger.debug(
               "public void updateBlob(String columnLabel, InputStream "
               + "inputStream)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateClob(int columnIndex, Reader reader) throws SQLException
  {
    logger.debug(
               "public void updateClob(int columnIndex, Reader reader)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateClob(String columnLabel, Reader reader) throws SQLException
  {
    logger.debug(
               "public void updateClob(String columnLabel, Reader reader)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader) throws SQLException
  {
    logger.debug(
               "public void updateNClob(int columnIndex, Reader reader)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader) throws SQLException
  {
    logger.debug(
               "public void updateNClob(String columnLabel, Reader reader)");

    throw new SQLFeatureNotSupportedException();
  }

  //@Override
  public <T> T getObject(int columnIndex,
                         Class<T> type) throws SQLException
  {
    logger.debug(
               "public <T> T getObject(int columnIndex,Class<T> type)");

    throw new SQLFeatureNotSupportedException();
  }

  //@Override
  public <T> T getObject(String columnLabel,
                         Class<T> type) throws SQLException
  {
    logger.debug(
               "public <T> T getObject(String columnLabel,Class<T> type)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public <T> T unwrap(
          Class<T> iface) throws SQLException
  {
    logger.debug("public <T> T unwrap(Class<T> iface)");

    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isWrapperFor(
          Class<?> iface) throws SQLException
  {
    logger.debug(
               "public boolean isWrapperFor(Class<?> iface)");

    throw new SQLFeatureNotSupportedException();
  }
}
