/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import net.snowflake.client.core.SFBaseSession;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.common.core.SqlState;

/** Base class for query result set and metadata result set */
abstract class SnowflakeBaseResultSet implements ResultSet {
  static final SFLogger logger = SFLoggerFactory.getLogger(SnowflakeBaseResultSet.class);
  private final int resultSetType;
  private final int resultSetConcurrency;
  private final int resultSetHoldability;
  // Snowflake supports sessionless result set. For this case, there is no
  // statement for this result set.
  protected final Statement statement;
  protected SnowflakeResultSetMetaDataV1 resultSetMetaData = null;
  protected Map<String, Object> parameters = new HashMap<>();
  private int fetchSize = 0;
  protected SFBaseSession session = null;

  SnowflakeBaseResultSet(Statement statement) throws SQLException {
    this.statement = statement;
    this.resultSetType = statement.getResultSetType();
    this.resultSetConcurrency = statement.getResultSetConcurrency();
    this.resultSetHoldability = statement.getResultSetHoldability();
    try {
      this.session = statement.unwrap(SnowflakeStatementV1.class).connection.getSFBaseSession();
    } catch (SQLException e) {
      // This exception shouldn't be hit. Statement class should be able to be unwrapped.
      logger.error(
          "Unable to unwrap SnowflakeStatementV1 class to retrieve session. Session is null.");
      this.session = null;
    }
  }

  /**
   * Create an sessionless result set, there is no statement and session for the result set.
   *
   * @param resultSetSerializable The result set serializable object which includes all metadata to
   *     create the result set
   */
  public SnowflakeBaseResultSet(SnowflakeResultSetSerializableV1 resultSetSerializable)
      throws SQLException {
    // This is a sessionless result set, so there is no actual statement for it.
    this.statement = new SnowflakeStatementV1.NoOpSnowflakeStatementV1();
    this.resultSetType = resultSetSerializable.getResultSetType();
    this.resultSetConcurrency = resultSetSerializable.getResultSetConcurrency();
    this.resultSetHoldability = resultSetSerializable.getResultSetHoldability();
  }

  /**
   * This should never be used. Simply needed this for SFAsynchronousResult subclass
   *
   * @throws SQLException
   */
  protected SnowflakeBaseResultSet() throws SQLException {
    this.resultSetType = 0;
    this.resultSetConcurrency = 0;
    this.resultSetHoldability = 0;
    this.statement = new SnowflakeStatementV1.NoOpSnowflakeStatementV1();
  }

  @Override
  public abstract boolean next() throws SQLException;

  @Override
  public abstract boolean isClosed() throws SQLException;

  /**
   * Raises SQLException if the result set is closed
   *
   * @throws SQLException if the result set is closed.
   */
  protected void raiseSQLExceptionIfResultSetIsClosed() throws SQLException {
    if (isClosed()) {
      throw new SnowflakeSQLException(ErrorCode.RESULTSET_ALREADY_CLOSED);
    }
  }

  @Override
  public abstract byte[] getBytes(int columnIndex) throws SQLException;

  public abstract Date getDate(int columnIndex, TimeZone tz) throws SQLException;

  @Override
  public Date getDate(int columnIndex) throws SQLException {
    raiseSQLExceptionIfResultSetIsClosed();
    return getDate(columnIndex, (TimeZone) null);
  }

  @Override
  public abstract Time getTime(int columnIndex) throws SQLException;

  @Override
  public Timestamp getTimestamp(int columnIndex) throws SQLException {
    raiseSQLExceptionIfResultSetIsClosed();
    return getTimestamp(columnIndex, (TimeZone) null);
  }

  public abstract Timestamp getTimestamp(int columnIndex, TimeZone tz) throws SQLException;

  @Override
  public InputStream getAsciiStream(int columnIndex) throws SQLException {
    logger.debug("public InputStream getAsciiStream(int columnIndex)");
    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  /** @deprecated */
  @Deprecated
  @Override
  public InputStream getUnicodeStream(int columnIndex) throws SQLException {
    logger.debug("public InputStream getUnicodeStream(int columnIndex)");
    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public InputStream getBinaryStream(int columnIndex) throws SQLException {
    logger.debug("public InputStream getBinaryStream(int columnIndex)");
    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public String getString(String columnLabel) throws SQLException {
    logger.debug("public String getString(String columnLabel)");

    return getString(findColumn(columnLabel));
  }

  @Override
  public boolean getBoolean(String columnLabel) throws SQLException {
    logger.debug("public boolean getBoolean(String columnLabel)");

    return getBoolean(findColumn(columnLabel));
  }

  @Override
  public byte getByte(String columnLabel) throws SQLException {
    logger.debug("public byte getByte(String columnLabel)");
    raiseSQLExceptionIfResultSetIsClosed();

    return getByte(findColumn(columnLabel));
  }

  @Override
  public short getShort(String columnLabel) throws SQLException {
    logger.debug("public short getShort(String columnLabel)");

    return getShort(findColumn(columnLabel));
  }

  @Override
  public int getInt(String columnLabel) throws SQLException {
    logger.debug("public int getInt(String columnLabel)");

    return getInt(findColumn(columnLabel));
  }

  @Override
  public long getLong(String columnLabel) throws SQLException {
    logger.debug("public long getLong(String columnLabel)");

    return getLong(findColumn(columnLabel));
  }

  @Override
  public float getFloat(String columnLabel) throws SQLException {
    logger.debug("public float getFloat(String columnLabel)");

    return getFloat(findColumn(columnLabel));
  }

  @Override
  public double getDouble(String columnLabel) throws SQLException {
    logger.debug("public double getDouble(String columnLabel)");

    return getDouble(findColumn(columnLabel));
  }

  /** @deprecated */
  @Deprecated
  @Override
  public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
    logger.debug("public BigDecimal getBigDecimal(String columnLabel, " + "int scale)");

    return getBigDecimal(findColumn(columnLabel), scale);
  }

  @Override
  public byte[] getBytes(String columnLabel) throws SQLException {
    logger.debug("public byte[] getBytes(String columnLabel)");

    return getBytes(findColumn(columnLabel));
  }

  @Override
  public Date getDate(String columnLabel) throws SQLException {
    logger.debug("public Date getDate(String columnLabel)");

    return getDate(findColumn(columnLabel));
  }

  @Override
  public Time getTime(String columnLabel) throws SQLException {
    logger.debug("public Time getTime(String columnLabel)");

    return getTime(findColumn(columnLabel));
  }

  @Override
  public Timestamp getTimestamp(String columnLabel) throws SQLException {
    logger.debug("public Timestamp getTimestamp(String columnLabel)");

    return getTimestamp(findColumn(columnLabel));
  }

  @Override
  public InputStream getAsciiStream(String columnLabel) throws SQLException {
    logger.debug("public InputStream getAsciiStream(String columnLabel)");
    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  /** @deprecated */
  @Deprecated
  @Override
  public InputStream getUnicodeStream(String columnLabel) throws SQLException {
    logger.debug("public InputStream getUnicodeStream(String columnLabel)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public InputStream getBinaryStream(String columnLabel) throws SQLException {
    logger.debug("public InputStream getBinaryStream(String columnLabel)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    logger.debug("public SQLWarning getWarnings()");
    raiseSQLExceptionIfResultSetIsClosed();
    return null;
  }

  @Override
  public void clearWarnings() throws SQLException {
    logger.debug("public void clearWarnings()");
    raiseSQLExceptionIfResultSetIsClosed();
  }

  @Override
  public String getCursorName() throws SQLException {
    logger.debug("public String getCursorName()");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    logger.debug("public ResultSetMetaData getMetaData()");
    raiseSQLExceptionIfResultSetIsClosed();
    return resultSetMetaData;
  }

  @Override
  public Object getObject(String columnLabel) throws SQLException {
    logger.debug("public Object getObject(String columnLabel)");

    return getObject(findColumn(columnLabel));
  }

  @Override
  public int findColumn(String columnLabel) throws SQLException {
    logger.debug("public int findColumn(String columnLabel)");
    raiseSQLExceptionIfResultSetIsClosed();

    int columnIndex = resultSetMetaData.getColumnIndex(columnLabel);

    if (columnIndex == -1) {
      throw new SQLException("Column not found: " + columnLabel, SqlState.UNDEFINED_COLUMN);
    } else {
      return ++columnIndex;
    }
  }

  @Override
  public Reader getCharacterStream(int columnIndex) throws SQLException {
    logger.debug("public Reader getCharacterStream(int columnIndex)");
    raiseSQLExceptionIfResultSetIsClosed();
    String streamData = getString(columnIndex);
    return (streamData == null) ? null : new StringReader(streamData);
  }

  @Override
  public Reader getCharacterStream(String columnLabel) throws SQLException {
    logger.debug("public Reader getCharacterStream(String columnLabel)");
    return getCharacterStream(findColumn(columnLabel));
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
    logger.debug("public BigDecimal getBigDecimal(String columnLabel)");

    return getBigDecimal(findColumn(columnLabel));
  }

  @Override
  public void beforeFirst() throws SQLException {
    logger.debug("public void beforeFirst()");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void afterLast() throws SQLException {
    logger.debug("public void afterLast()");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public boolean first() throws SQLException {
    logger.debug("public boolean first()");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public boolean last() throws SQLException {
    logger.debug("public boolean last()");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public boolean absolute(int row) throws SQLException {
    logger.debug("public boolean absolute(int row)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public boolean relative(int rows) throws SQLException {
    logger.debug("public boolean relative(int rows)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public boolean previous() throws SQLException {
    logger.debug("public boolean previous()");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public int getFetchDirection() throws SQLException {
    logger.debug("public int getFetchDirection()");
    raiseSQLExceptionIfResultSetIsClosed();
    return ResultSet.FETCH_FORWARD;
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    logger.debug("public void setFetchDirection(int direction)");

    raiseSQLExceptionIfResultSetIsClosed();
    if (direction != ResultSet.FETCH_FORWARD) {
      throw new SnowflakeLoggedFeatureNotSupportedException(session);
    }
  }

  @Override
  public int getFetchSize() throws SQLException {
    logger.debug("public int getFetchSize()");
    raiseSQLExceptionIfResultSetIsClosed();
    return this.fetchSize;
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    logger.debug("public void setFetchSize(int rows)");
    raiseSQLExceptionIfResultSetIsClosed();

    this.fetchSize = rows;
  }

  @Override
  public int getType() throws SQLException {
    logger.debug("public int getType()");
    raiseSQLExceptionIfResultSetIsClosed();
    return resultSetType;
  }

  @Override
  public int getConcurrency() throws SQLException {
    logger.debug("public int getConcurrency()");
    raiseSQLExceptionIfResultSetIsClosed();
    return resultSetConcurrency;
  }

  @Override
  public boolean rowUpdated() throws SQLException {
    logger.debug("public boolean rowUpdated()");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public boolean rowInserted() throws SQLException {
    logger.debug("public boolean rowInserted()");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public boolean rowDeleted() throws SQLException {
    logger.debug("public boolean rowDeleted()");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateNull(int columnIndex) throws SQLException {
    logger.debug("public void updateNull(int columnIndex)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateBoolean(int columnIndex, boolean x) throws SQLException {
    logger.debug("public void updateBoolean(int columnIndex, boolean x)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateByte(int columnIndex, byte x) throws SQLException {
    logger.debug("public void updateByte(int columnIndex, byte x)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateShort(int columnIndex, short x) throws SQLException {
    logger.debug("public void updateShort(int columnIndex, short x)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateInt(int columnIndex, int x) throws SQLException {
    logger.debug("public void updateInt(int columnIndex, int x)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateLong(int columnIndex, long x) throws SQLException {
    logger.debug("public void updateLong(int columnIndex, long x)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateFloat(int columnIndex, float x) throws SQLException {
    logger.debug("public void updateFloat(int columnIndex, float x)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateDouble(int columnIndex, double x) throws SQLException {
    logger.debug("public void updateDouble(int columnIndex, double x)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
    logger.debug("public void updateBigDecimal(int columnIndex, BigDecimal x)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateString(int columnIndex, String x) throws SQLException {
    logger.debug("public void updateString(int columnIndex, String x)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateBytes(int columnIndex, byte[] x) throws SQLException {
    logger.debug("public void updateBytes(int columnIndex, byte[] x)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateDate(int columnIndex, Date x) throws SQLException {
    logger.debug("public void updateDate(int columnIndex, Date x)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateTime(int columnIndex, Time x) throws SQLException {
    logger.debug("public void updateTime(int columnIndex, Time x)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
    logger.debug("public void updateTimestamp(int columnIndex, Timestamp x)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
    logger.debug("public void updateAsciiStream(int columnIndex, " + "InputStream x, int length)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
    logger.debug("public void updateBinaryStream(int columnIndex, " + "InputStream x, int length)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
    logger.debug("public void updateCharacterStream(int columnIndex, " + "Reader x, int length)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
    logger.debug("public void updateObject(int columnIndex, Object x, " + "int scaleOrLength)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateObject(int columnIndex, Object x) throws SQLException {
    logger.debug("public void updateObject(int columnIndex, Object x)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateNull(String columnLabel) throws SQLException {
    logger.debug("public void updateNull(String columnLabel)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateBoolean(String columnLabel, boolean x) throws SQLException {
    logger.debug("public void updateBoolean(String columnLabel, boolean x)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateByte(String columnLabel, byte x) throws SQLException {
    logger.debug("public void updateByte(String columnLabel, byte x)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateShort(String columnLabel, short x) throws SQLException {
    logger.debug("public void updateShort(String columnLabel, short x)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateInt(String columnLabel, int x) throws SQLException {
    logger.debug("public void updateInt(String columnLabel, int x)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateLong(String columnLabel, long x) throws SQLException {
    logger.debug("public void updateLong(String columnLabel, long x)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateFloat(String columnLabel, float x) throws SQLException {
    logger.debug("public void updateFloat(String columnLabel, float x)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateDouble(String columnLabel, double x) throws SQLException {
    logger.debug("public void updateDouble(String columnLabel, double x)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
    logger.debug("public void updateBigDecimal(String columnLabel, " + "BigDecimal x)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateString(String columnLabel, String x) throws SQLException {
    logger.debug("public void updateString(String columnLabel, String x)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateBytes(String columnLabel, byte[] x) throws SQLException {
    logger.debug("public void updateBytes(String columnLabel, byte[] x)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateDate(String columnLabel, Date x) throws SQLException {
    logger.debug("public void updateDate(String columnLabel, Date x)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateTime(String columnLabel, Time x) throws SQLException {
    logger.debug("public void updateTime(String columnLabel, Time x)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
    logger.debug("public void updateTimestamp(String columnLabel, Timestamp x)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
    logger.debug(
        "public void updateAsciiStream(String columnLabel, " + "InputStream x, int length)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, int length)
      throws SQLException {
    logger.debug(
        "public void updateBinaryStream(String columnLabel, " + "InputStream x, int length)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, int length)
      throws SQLException {
    logger.debug(
        "public void updateCharacterStream(String columnLabel, " + "Reader reader,int length)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
    logger.debug("public void updateObject(String columnLabel, Object x, " + "int scaleOrLength)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateObject(String columnLabel, Object x) throws SQLException {
    logger.debug("public void updateObject(String columnLabel, Object x)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void insertRow() throws SQLException {
    logger.debug("public void insertRow()");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateRow() throws SQLException {
    logger.debug("public void updateRow()");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void deleteRow() throws SQLException {
    logger.debug("public void deleteRow()");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void refreshRow() throws SQLException {
    logger.debug("public void refreshRow()");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void cancelRowUpdates() throws SQLException {
    logger.debug("public void cancelRowUpdates()");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void moveToInsertRow() throws SQLException {
    logger.debug("public void moveToInsertRow()");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void moveToCurrentRow() throws SQLException {
    logger.debug("public void moveToCurrentRow()");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public Statement getStatement() throws SQLException {
    logger.debug("public Statement getStatement()");
    raiseSQLExceptionIfResultSetIsClosed();
    return statement;
  }

  @Override
  public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
    logger.debug("public Object getObject(int columnIndex, Map<String, " + "Class<?>> map)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public Ref getRef(int columnIndex) throws SQLException {
    logger.debug("public Ref getRef(int columnIndex)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public Blob getBlob(int columnIndex) throws SQLException {
    logger.debug("public Blob getBlob(int columnIndex)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public Clob getClob(int columnIndex) throws SQLException {
    logger.debug("public Clob getClob(int columnIndex)");
    return new SnowflakeClob(getString(columnIndex));
  }

  @Override
  public Array getArray(int columnIndex) throws SQLException {
    logger.debug("public Array getArray(int columnIndex)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
    logger.debug("public Object getObject(String columnLabel, " + "Map<String, Class<?>> map)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public Ref getRef(String columnLabel) throws SQLException {
    logger.debug("public Ref getRef(String columnLabel)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public Blob getBlob(String columnLabel) throws SQLException {
    logger.debug("public Blob getBlob(String columnLabel)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public Clob getClob(String columnLabel) throws SQLException {
    logger.debug("public Clob getClob(String columnLabel)");

    return new SnowflakeClob(getString(columnLabel));
  }

  @Override
  public Array getArray(String columnLabel) throws SQLException {
    logger.debug("public Array getArray(String columnLabel)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public Date getDate(int columnIndex, Calendar cal) throws SQLException {
    logger.debug("public Date getDate(int columnIndex, Calendar cal)");
    return getDate(columnIndex, cal.getTimeZone());
  }

  @Override
  public Date getDate(String columnLabel, Calendar cal) throws SQLException {
    logger.debug("public Date getDate(String columnLabel, Calendar cal)");

    return getDate(findColumn(columnLabel), cal.getTimeZone());
  }

  @Override
  public Time getTime(int columnIndex, Calendar cal) throws SQLException {
    logger.debug("public Time getTime(int columnIndex, Calendar cal)");

    return getTime(columnIndex);
  }

  @Override
  public Time getTime(String columnLabel, Calendar cal) throws SQLException {
    logger.debug("public Time getTime(String columnLabel, Calendar cal)");

    return getTime(columnLabel);
  }

  @Override
  public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
    logger.debug("public Timestamp getTimestamp(int columnIndex, Calendar cal)");

    return getTimestamp(columnIndex, cal.getTimeZone());
  }

  @Override
  public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
    logger.debug("public Timestamp getTimestamp(String columnLabel, " + "Calendar cal)");

    return getTimestamp(findColumn(columnLabel), cal.getTimeZone());
  }

  @Override
  public URL getURL(int columnIndex) throws SQLException {
    logger.debug("public URL getURL(int columnIndex)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public URL getURL(String columnLabel) throws SQLException {
    logger.debug("public URL getURL(String columnLabel)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateRef(int columnIndex, Ref x) throws SQLException {
    logger.debug("public void updateRef(int columnIndex, Ref x)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateRef(String columnLabel, Ref x) throws SQLException {
    logger.debug("public void updateRef(String columnLabel, Ref x)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateBlob(int columnIndex, Blob x) throws SQLException {
    logger.debug("public void updateBlob(int columnIndex, Blob x)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateBlob(String columnLabel, Blob x) throws SQLException {
    logger.debug("public void updateBlob(String columnLabel, Blob x)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateClob(int columnIndex, Clob x) throws SQLException {
    logger.debug("public void updateClob(int columnIndex, Clob x)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateClob(String columnLabel, Clob x) throws SQLException {
    logger.debug("public void updateClob(String columnLabel, Clob x)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateArray(int columnIndex, Array x) throws SQLException {
    logger.debug("public void updateArray(int columnIndex, Array x)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateArray(String columnLabel, Array x) throws SQLException {
    logger.debug("public void updateArray(String columnLabel, Array x)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public RowId getRowId(int columnIndex) throws SQLException {
    logger.debug("public RowId getRowId(int columnIndex)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public RowId getRowId(String columnLabel) throws SQLException {
    logger.debug("public RowId getRowId(String columnLabel)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateRowId(int columnIndex, RowId x) throws SQLException {
    logger.debug("public void updateRowId(int columnIndex, RowId x)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateRowId(String columnLabel, RowId x) throws SQLException {
    logger.debug("public void updateRowId(String columnLabel, RowId x)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public int getHoldability() throws SQLException {
    logger.debug("public int getHoldability()");
    raiseSQLExceptionIfResultSetIsClosed();
    return resultSetHoldability;
  }

  @Override
  public void updateNString(int columnIndex, String nString) throws SQLException {
    logger.debug("public void updateNString(int columnIndex, String nString)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateNString(String columnLabel, String nString) throws SQLException {
    logger.debug("public void updateNString(String columnLabel, String nString)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
    logger.debug("public void updateNClob(int columnIndex, NClob nClob)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
    logger.debug("public void updateNClob(String columnLabel, NClob nClob)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public NClob getNClob(int columnIndex) throws SQLException {
    logger.debug("public NClob getNClob(int columnIndex)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public NClob getNClob(String columnLabel) throws SQLException {
    logger.debug("public NClob getNClob(String columnLabel)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public SQLXML getSQLXML(int columnIndex) throws SQLException {
    logger.debug("public SQLXML getSQLXML(int columnIndex)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public SQLXML getSQLXML(String columnLabel) throws SQLException {
    logger.debug("public SQLXML getSQLXML(String columnLabel)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
    logger.debug("public void updateSQLXML(int columnIndex, SQLXML xmlObject)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
    logger.debug("public void updateSQLXML(String columnLabel, SQLXML xmlObject)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public String getNString(int columnIndex) throws SQLException {
    logger.debug("public String getNString(int columnIndex)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public String getNString(String columnLabel) throws SQLException {
    logger.debug("public String getNString(String columnLabel)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public Reader getNCharacterStream(int columnIndex) throws SQLException {
    logger.debug("public Reader getNCharacterStream(int columnIndex)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public Reader getNCharacterStream(String columnLabel) throws SQLException {
    logger.debug("public Reader getNCharacterStream(String columnLabel)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    logger.debug("public void updateNCharacterStream(int columnIndex, " + "Reader x, long length)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader, long length)
      throws SQLException {
    logger.debug(
        "public void updateNCharacterStream(String columnLabel, " + "Reader reader,long length)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
    logger.debug("public void updateAsciiStream(int columnIndex, " + "InputStream x, long length)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
    logger.debug(
        "public void updateBinaryStream(int columnIndex, " + "InputStream x, long length)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    logger.debug("public void updateCharacterStream(int columnIndex, Reader x, " + "long length)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, long length)
      throws SQLException {
    logger.debug(
        "public void updateAsciiStream(String columnLabel, " + "InputStream x, long length)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, long length)
      throws SQLException {
    logger.debug(
        "public void updateBinaryStream(String columnLabel, " + "InputStream x, long length)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, long length)
      throws SQLException {
    logger.debug(
        "public void updateCharacterStream(String columnLabel, " + "Reader reader,long length)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream, long length)
      throws SQLException {
    logger.debug(
        "public void updateBlob(int columnIndex, InputStream " + "inputStream, long length)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream, long length)
      throws SQLException {
    logger.debug(
        "public void updateBlob(String columnLabel, " + "InputStream inputStream,long length)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
    logger.debug("public void updateClob(int columnIndex, Reader reader, " + "long length)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
    logger.debug("public void updateClob(String columnLabel, Reader reader, " + "long length)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
    logger.debug("public void updateNClob(int columnIndex, Reader reader, " + "long length)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
    logger.debug("public void updateNClob(String columnLabel, Reader reader, " + "long length)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
    logger.debug("public void updateNCharacterStream(int columnIndex, Reader x)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
    logger.debug("public void updateNCharacterStream(String columnLabel, " + "Reader reader)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
    logger.debug("public void updateAsciiStream(int columnIndex, InputStream x)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
    logger.debug("public void updateBinaryStream(int columnIndex, InputStream x)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
    logger.debug("public void updateCharacterStream(int columnIndex, Reader x)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
    logger.debug("public void updateAsciiStream(String columnLabel, InputStream x)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
    logger.debug("public void updateBinaryStream(String columnLabel, InputStream x)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
    logger.debug("public void updateCharacterStream(String columnLabel, " + "Reader reader)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
    logger.debug("public void updateBlob(int columnIndex, InputStream inputStream)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
    logger.debug("public void updateBlob(String columnLabel, InputStream " + "inputStream)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateClob(int columnIndex, Reader reader) throws SQLException {
    logger.debug("public void updateClob(int columnIndex, Reader reader)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateClob(String columnLabel, Reader reader) throws SQLException {
    logger.debug("public void updateClob(String columnLabel, Reader reader)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader) throws SQLException {
    logger.debug("public void updateNClob(int columnIndex, Reader reader)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader) throws SQLException {
    logger.debug("public void updateNClob(String columnLabel, Reader reader)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  // @Override
  public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
    logger.debug("public <T> T getObject(int columnIndex,Class<T> type)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  // @Override
  public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
    logger.debug("public <T> T getObject(String columnLabel,Class<T> type)");

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @SuppressWarnings("unchecked")
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
}
