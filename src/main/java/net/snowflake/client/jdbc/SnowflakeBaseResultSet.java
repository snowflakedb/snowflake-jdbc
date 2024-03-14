/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
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
import java.sql.SQLData;
import java.sql.SQLException;
import java.sql.SQLInput;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.function.Supplier;
import net.snowflake.client.core.SFBaseSession;
import net.snowflake.client.core.structs.SnowflakeObjectTypeFactories;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.common.core.SqlState;

/** Base class for query result set and metadata result set */
public abstract class SnowflakeBaseResultSet implements ResultSet {
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
          "Unable to unwrap SnowflakeStatementV1 class to retrieve session. Session is null.",
          false);
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
    logger.debug("public InputStream getAsciiStream(int columnIndex)", false);
    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  /**
   * @deprecated
   */
  @Deprecated
  @Override
  public InputStream getUnicodeStream(int columnIndex) throws SQLException {
    logger.debug("public InputStream getUnicodeStream(int columnIndex)", false);
    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public InputStream getBinaryStream(int columnIndex) throws SQLException {
    logger.debug("public InputStream getBinaryStream(int columnIndex)", false);
    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public String getString(String columnLabel) throws SQLException {
    logger.debug("public String getString(String columnLabel)", false);

    return getString(findColumn(columnLabel));
  }

  @Override
  public boolean getBoolean(String columnLabel) throws SQLException {
    logger.debug("public boolean getBoolean(String columnLabel)", false);

    return getBoolean(findColumn(columnLabel));
  }

  @Override
  public byte getByte(String columnLabel) throws SQLException {
    logger.debug("public byte getByte(String columnLabel)", false);
    raiseSQLExceptionIfResultSetIsClosed();

    return getByte(findColumn(columnLabel));
  }

  @Override
  public short getShort(String columnLabel) throws SQLException {
    logger.debug("public short getShort(String columnLabel)", false);

    return getShort(findColumn(columnLabel));
  }

  @Override
  public int getInt(String columnLabel) throws SQLException {
    logger.debug("public int getInt(String columnLabel)", false);

    return getInt(findColumn(columnLabel));
  }

  @Override
  public long getLong(String columnLabel) throws SQLException {
    logger.debug("public long getLong(String columnLabel)", false);

    return getLong(findColumn(columnLabel));
  }

  @Override
  public float getFloat(String columnLabel) throws SQLException {
    logger.debug("public float getFloat(String columnLabel)", false);

    return getFloat(findColumn(columnLabel));
  }

  @Override
  public double getDouble(String columnLabel) throws SQLException {
    logger.debug("public double getDouble(String columnLabel)", false);

    return getDouble(findColumn(columnLabel));
  }

  /**
   * @deprecated
   */
  @Deprecated
  @Override
  public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
    logger.debug("public BigDecimal getBigDecimal(String columnLabel, " + "int scale)", false);

    return getBigDecimal(findColumn(columnLabel), scale);
  }

  @Override
  public byte[] getBytes(String columnLabel) throws SQLException {
    logger.debug("public byte[] getBytes(String columnLabel)", false);

    return getBytes(findColumn(columnLabel));
  }

  @Override
  public Date getDate(String columnLabel) throws SQLException {
    logger.debug("public Date getDate(String columnLabel)", false);

    return getDate(findColumn(columnLabel));
  }

  @Override
  public Time getTime(String columnLabel) throws SQLException {
    logger.debug("public Time getTime(String columnLabel)", false);

    return getTime(findColumn(columnLabel));
  }

  @Override
  public Timestamp getTimestamp(String columnLabel) throws SQLException {
    logger.debug("public Timestamp getTimestamp(String columnLabel)", false);

    return getTimestamp(findColumn(columnLabel));
  }

  @Override
  public InputStream getAsciiStream(String columnLabel) throws SQLException {
    logger.debug("public InputStream getAsciiStream(String columnLabel)", false);
    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  /**
   * @deprecated
   */
  @Deprecated
  @Override
  public InputStream getUnicodeStream(String columnLabel) throws SQLException {
    logger.debug("public InputStream getUnicodeStream(String columnLabel)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public InputStream getBinaryStream(String columnLabel) throws SQLException {
    logger.debug("public InputStream getBinaryStream(String columnLabel)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    logger.debug("public SQLWarning getWarnings()", false);
    raiseSQLExceptionIfResultSetIsClosed();
    return null;
  }

  @Override
  public void clearWarnings() throws SQLException {
    logger.debug("public void clearWarnings()", false);
    raiseSQLExceptionIfResultSetIsClosed();
  }

  @Override
  public String getCursorName() throws SQLException {
    logger.debug("public String getCursorName()", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    logger.debug("public ResultSetMetaData getMetaData()", false);
    raiseSQLExceptionIfResultSetIsClosed();
    return resultSetMetaData;
  }

  @Override
  public Object getObject(String columnLabel) throws SQLException {
    logger.debug("public Object getObject(String columnLabel)", false);

    return getObject(findColumn(columnLabel));
  }

  @Override
  public int findColumn(String columnLabel) throws SQLException {
    logger.debug("public int findColumn(String columnLabel)", false);
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
    logger.debug("public Reader getCharacterStream(int columnIndex)", false);
    raiseSQLExceptionIfResultSetIsClosed();
    String streamData = getString(columnIndex);
    return (streamData == null) ? null : new StringReader(streamData);
  }

  @Override
  public Reader getCharacterStream(String columnLabel) throws SQLException {
    logger.debug("public Reader getCharacterStream(String columnLabel)", false);
    return getCharacterStream(findColumn(columnLabel));
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
    logger.debug("public BigDecimal getBigDecimal(String columnLabel)", false);

    return getBigDecimal(findColumn(columnLabel));
  }

  @Override
  public void beforeFirst() throws SQLException {
    logger.debug("public void beforeFirst()", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void afterLast() throws SQLException {
    logger.debug("public void afterLast()", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public boolean first() throws SQLException {
    logger.debug("public boolean first()", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public boolean last() throws SQLException {
    logger.debug("public boolean last()", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public boolean absolute(int row) throws SQLException {
    logger.debug("public boolean absolute(int row)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public boolean relative(int rows) throws SQLException {
    logger.debug("public boolean relative(int rows)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public boolean previous() throws SQLException {
    logger.debug("public boolean previous()", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public int getFetchDirection() throws SQLException {
    logger.debug("public int getFetchDirection()", false);
    raiseSQLExceptionIfResultSetIsClosed();
    return ResultSet.FETCH_FORWARD;
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    logger.debug("public void setFetchDirection(int direction)", false);

    raiseSQLExceptionIfResultSetIsClosed();
    if (direction != ResultSet.FETCH_FORWARD) {
      throw new SnowflakeLoggedFeatureNotSupportedException(session);
    }
  }

  @Override
  public int getFetchSize() throws SQLException {
    logger.debug("public int getFetchSize()", false);
    raiseSQLExceptionIfResultSetIsClosed();
    return this.fetchSize;
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    logger.debug("public void setFetchSize(int rows)", false);
    raiseSQLExceptionIfResultSetIsClosed();

    this.fetchSize = rows;
  }

  @Override
  public int getType() throws SQLException {
    logger.debug("public int getType()", false);
    raiseSQLExceptionIfResultSetIsClosed();
    return resultSetType;
  }

  @Override
  public int getConcurrency() throws SQLException {
    logger.debug("public int getConcurrency()", false);
    raiseSQLExceptionIfResultSetIsClosed();
    return resultSetConcurrency;
  }

  @Override
  public boolean rowUpdated() throws SQLException {
    logger.debug("public boolean rowUpdated()", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public boolean rowInserted() throws SQLException {
    logger.debug("public boolean rowInserted()", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public boolean rowDeleted() throws SQLException {
    logger.debug("public boolean rowDeleted()", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateNull(int columnIndex) throws SQLException {
    logger.debug("public void updateNull(int columnIndex)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateBoolean(int columnIndex, boolean x) throws SQLException {
    logger.debug("public void updateBoolean(int columnIndex, boolean x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateByte(int columnIndex, byte x) throws SQLException {
    logger.debug("public void updateByte(int columnIndex, byte x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateShort(int columnIndex, short x) throws SQLException {
    logger.debug("public void updateShort(int columnIndex, short x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateInt(int columnIndex, int x) throws SQLException {
    logger.debug("public void updateInt(int columnIndex, int x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateLong(int columnIndex, long x) throws SQLException {
    logger.debug("public void updateLong(int columnIndex, long x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateFloat(int columnIndex, float x) throws SQLException {
    logger.debug("public void updateFloat(int columnIndex, float x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateDouble(int columnIndex, double x) throws SQLException {
    logger.debug("public void updateDouble(int columnIndex, double x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
    logger.debug("public void updateBigDecimal(int columnIndex, BigDecimal x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateString(int columnIndex, String x) throws SQLException {
    logger.debug("public void updateString(int columnIndex, String x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateBytes(int columnIndex, byte[] x) throws SQLException {
    logger.debug("public void updateBytes(int columnIndex, byte[] x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateDate(int columnIndex, Date x) throws SQLException {
    logger.debug("public void updateDate(int columnIndex, Date x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateTime(int columnIndex, Time x) throws SQLException {
    logger.debug("public void updateTime(int columnIndex, Time x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
    logger.debug("public void updateTimestamp(int columnIndex, Timestamp x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
    logger.debug(
        "public void updateAsciiStream(int columnIndex, " + "InputStream x, int length)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
    logger.debug(
        "public void updateBinaryStream(int columnIndex, " + "InputStream x, int length)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
    logger.debug(
        "public void updateCharacterStream(int columnIndex, " + "Reader x, int length)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
    logger.debug(
        "public void updateObject(int columnIndex, Object x, " + "int scaleOrLength)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateObject(int columnIndex, Object x) throws SQLException {
    logger.debug("public void updateObject(int columnIndex, Object x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateNull(String columnLabel) throws SQLException {
    logger.debug("public void updateNull(String columnLabel)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateBoolean(String columnLabel, boolean x) throws SQLException {
    logger.debug("public void updateBoolean(String columnLabel, boolean x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateByte(String columnLabel, byte x) throws SQLException {
    logger.debug("public void updateByte(String columnLabel, byte x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateShort(String columnLabel, short x) throws SQLException {
    logger.debug("public void updateShort(String columnLabel, short x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateInt(String columnLabel, int x) throws SQLException {
    logger.debug("public void updateInt(String columnLabel, int x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateLong(String columnLabel, long x) throws SQLException {
    logger.debug("public void updateLong(String columnLabel, long x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateFloat(String columnLabel, float x) throws SQLException {
    logger.debug("public void updateFloat(String columnLabel, float x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateDouble(String columnLabel, double x) throws SQLException {
    logger.debug("public void updateDouble(String columnLabel, double x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
    logger.debug("public void updateBigDecimal(String columnLabel, " + "BigDecimal x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateString(String columnLabel, String x) throws SQLException {
    logger.debug("public void updateString(String columnLabel, String x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateBytes(String columnLabel, byte[] x) throws SQLException {
    logger.debug("public void updateBytes(String columnLabel, byte[] x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateDate(String columnLabel, Date x) throws SQLException {
    logger.debug("public void updateDate(String columnLabel, Date x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateTime(String columnLabel, Time x) throws SQLException {
    logger.debug("public void updateTime(String columnLabel, Time x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
    logger.debug("public void updateTimestamp(String columnLabel, Timestamp x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
    logger.debug(
        "public void updateAsciiStream(String columnLabel, " + "InputStream x, int length)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, int length)
      throws SQLException {
    logger.debug(
        "public void updateBinaryStream(String columnLabel, " + "InputStream x, int length)",
        false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, int length)
      throws SQLException {
    logger.debug(
        "public void updateCharacterStream(String columnLabel, " + "Reader reader,int length)",
        false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
    logger.debug(
        "public void updateObject(String columnLabel, Object x, " + "int scaleOrLength)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateObject(String columnLabel, Object x) throws SQLException {
    logger.debug("public void updateObject(String columnLabel, Object x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void insertRow() throws SQLException {
    logger.debug("public void insertRow()", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateRow() throws SQLException {
    logger.debug("public void updateRow()", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void deleteRow() throws SQLException {
    logger.debug("public void deleteRow()", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void refreshRow() throws SQLException {
    logger.debug("public void refreshRow()", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void cancelRowUpdates() throws SQLException {
    logger.debug("public void cancelRowUpdates()", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void moveToInsertRow() throws SQLException {
    logger.debug("public void moveToInsertRow()", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void moveToCurrentRow() throws SQLException {
    logger.debug("public void moveToCurrentRow()", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public Statement getStatement() throws SQLException {
    logger.debug("public Statement getStatement()", false);
    raiseSQLExceptionIfResultSetIsClosed();
    return statement;
  }

  @Override
  public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
    logger.debug("public Object getObject(int columnIndex, Map<String, " + "Class<?>> map)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public Ref getRef(int columnIndex) throws SQLException {
    logger.debug("public Ref getRef(int columnIndex)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public Blob getBlob(int columnIndex) throws SQLException {
    logger.debug("public Blob getBlob(int columnIndex)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public Clob getClob(int columnIndex) throws SQLException {
    logger.debug("public Clob getClob(int columnIndex)", false);
    String columnValue = getString(columnIndex);

    return columnValue == null ? null : new SnowflakeClob(columnValue);
  }

  @Override
  public Array getArray(int columnIndex) throws SQLException {
    logger.debug("public Array getArray(int columnIndex)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
    logger.debug(
        "public Object getObject(String columnLabel, " + "Map<String, Class<?>> map)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public Ref getRef(String columnLabel) throws SQLException {
    logger.debug("public Ref getRef(String columnLabel)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public Blob getBlob(String columnLabel) throws SQLException {
    logger.debug("public Blob getBlob(String columnLabel)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public Clob getClob(String columnLabel) throws SQLException {
    logger.debug("public Clob getClob(String columnLabel)", false);
    String columnValue = getString(columnLabel);

    return columnValue == null ? null : new SnowflakeClob(columnValue);
  }

  @Override
  public Array getArray(String columnLabel) throws SQLException {
    logger.debug("public Array getArray(String columnLabel)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public Date getDate(int columnIndex, Calendar cal) throws SQLException {
    logger.debug("public Date getDate(int columnIndex, Calendar cal)", false);
    return getDate(columnIndex, cal.getTimeZone());
  }

  @Override
  public Date getDate(String columnLabel, Calendar cal) throws SQLException {
    logger.debug("public Date getDate(String columnLabel, Calendar cal)", false);

    return getDate(findColumn(columnLabel), cal.getTimeZone());
  }

  @Override
  public Time getTime(int columnIndex, Calendar cal) throws SQLException {
    logger.debug("public Time getTime(int columnIndex, Calendar cal)", false);

    return getTime(columnIndex);
  }

  @Override
  public Time getTime(String columnLabel, Calendar cal) throws SQLException {
    logger.debug("public Time getTime(String columnLabel, Calendar cal)", false);

    return getTime(columnLabel);
  }

  @Override
  public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
    logger.debug("public Timestamp getTimestamp(int columnIndex, Calendar cal)", false);

    return getTimestamp(columnIndex, cal.getTimeZone());
  }

  @Override
  public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
    logger.debug("public Timestamp getTimestamp(String columnLabel, " + "Calendar cal)", false);

    return getTimestamp(findColumn(columnLabel), cal.getTimeZone());
  }

  @Override
  public URL getURL(int columnIndex) throws SQLException {
    logger.debug("public URL getURL(int columnIndex)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public URL getURL(String columnLabel) throws SQLException {
    logger.debug("public URL getURL(String columnLabel)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateRef(int columnIndex, Ref x) throws SQLException {
    logger.debug("public void updateRef(int columnIndex, Ref x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateRef(String columnLabel, Ref x) throws SQLException {
    logger.debug("public void updateRef(String columnLabel, Ref x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateBlob(int columnIndex, Blob x) throws SQLException {
    logger.debug("public void updateBlob(int columnIndex, Blob x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateBlob(String columnLabel, Blob x) throws SQLException {
    logger.debug("public void updateBlob(String columnLabel, Blob x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateClob(int columnIndex, Clob x) throws SQLException {
    logger.debug("public void updateClob(int columnIndex, Clob x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateClob(String columnLabel, Clob x) throws SQLException {
    logger.debug("public void updateClob(String columnLabel, Clob x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateArray(int columnIndex, Array x) throws SQLException {
    logger.debug("public void updateArray(int columnIndex, Array x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateArray(String columnLabel, Array x) throws SQLException {
    logger.debug("public void updateArray(String columnLabel, Array x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public RowId getRowId(int columnIndex) throws SQLException {
    logger.debug("public RowId getRowId(int columnIndex)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public RowId getRowId(String columnLabel) throws SQLException {
    logger.debug("public RowId getRowId(String columnLabel)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateRowId(int columnIndex, RowId x) throws SQLException {
    logger.debug("public void updateRowId(int columnIndex, RowId x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateRowId(String columnLabel, RowId x) throws SQLException {
    logger.debug("public void updateRowId(String columnLabel, RowId x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public int getHoldability() throws SQLException {
    logger.debug("public int getHoldability()", false);
    raiseSQLExceptionIfResultSetIsClosed();
    return resultSetHoldability;
  }

  @Override
  public void updateNString(int columnIndex, String nString) throws SQLException {
    logger.debug("public void updateNString(int columnIndex, String nString)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateNString(String columnLabel, String nString) throws SQLException {
    logger.debug("public void updateNString(String columnLabel, String nString)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
    logger.debug("public void updateNClob(int columnIndex, NClob nClob)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
    logger.debug("public void updateNClob(String columnLabel, NClob nClob)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public NClob getNClob(int columnIndex) throws SQLException {
    logger.debug("public NClob getNClob(int columnIndex)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public NClob getNClob(String columnLabel) throws SQLException {
    logger.debug("public NClob getNClob(String columnLabel)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public SQLXML getSQLXML(int columnIndex) throws SQLException {
    logger.debug("public SQLXML getSQLXML(int columnIndex)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public SQLXML getSQLXML(String columnLabel) throws SQLException {
    logger.debug("public SQLXML getSQLXML(String columnLabel)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
    logger.debug("public void updateSQLXML(int columnIndex, SQLXML xmlObject)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
    logger.debug("public void updateSQLXML(String columnLabel, SQLXML xmlObject)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public String getNString(int columnIndex) throws SQLException {
    logger.debug("public String getNString(int columnIndex)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public String getNString(String columnLabel) throws SQLException {
    logger.debug("public String getNString(String columnLabel)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public Reader getNCharacterStream(int columnIndex) throws SQLException {
    logger.debug("public Reader getNCharacterStream(int columnIndex)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public Reader getNCharacterStream(String columnLabel) throws SQLException {
    logger.debug("public Reader getNCharacterStream(String columnLabel)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    logger.debug(
        "public void updateNCharacterStream(int columnIndex, " + "Reader x, long length)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader, long length)
      throws SQLException {
    logger.debug(
        "public void updateNCharacterStream(String columnLabel, " + "Reader reader,long length)",
        false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
    logger.debug(
        "public void updateAsciiStream(int columnIndex, " + "InputStream x, long length)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
    logger.debug(
        "public void updateBinaryStream(int columnIndex, " + "InputStream x, long length)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    logger.debug(
        "public void updateCharacterStream(int columnIndex, Reader x, " + "long length)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, long length)
      throws SQLException {
    logger.debug(
        "public void updateAsciiStream(String columnLabel, " + "InputStream x, long length)",
        false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, long length)
      throws SQLException {
    logger.debug(
        "public void updateBinaryStream(String columnLabel, " + "InputStream x, long length)",
        false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, long length)
      throws SQLException {
    logger.debug(
        "public void updateCharacterStream(String columnLabel, " + "Reader reader,long length)",
        false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream, long length)
      throws SQLException {
    logger.debug(
        "public void updateBlob(int columnIndex, InputStream " + "inputStream, long length)",
        false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream, long length)
      throws SQLException {
    logger.debug(
        "public void updateBlob(String columnLabel, " + "InputStream inputStream,long length)",
        false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
    logger.debug("public void updateClob(int columnIndex, Reader reader, " + "long length)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
    logger.debug(
        "public void updateClob(String columnLabel, Reader reader, " + "long length)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
    logger.debug(
        "public void updateNClob(int columnIndex, Reader reader, " + "long length)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
    logger.debug(
        "public void updateNClob(String columnLabel, Reader reader, " + "long length)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
    logger.debug("public void updateNCharacterStream(int columnIndex, Reader x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
    logger.debug(
        "public void updateNCharacterStream(String columnLabel, " + "Reader reader)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
    logger.debug("public void updateAsciiStream(int columnIndex, InputStream x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
    logger.debug("public void updateBinaryStream(int columnIndex, InputStream x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
    logger.debug("public void updateCharacterStream(int columnIndex, Reader x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
    logger.debug("public void updateAsciiStream(String columnLabel, InputStream x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
    logger.debug("public void updateBinaryStream(String columnLabel, InputStream x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
    logger.debug(
        "public void updateCharacterStream(String columnLabel, " + "Reader reader)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
    logger.debug("public void updateBlob(int columnIndex, InputStream inputStream)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
    logger.debug("public void updateBlob(String columnLabel, InputStream " + "inputStream)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateClob(int columnIndex, Reader reader) throws SQLException {
    logger.debug("public void updateClob(int columnIndex, Reader reader)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateClob(String columnLabel, Reader reader) throws SQLException {
    logger.debug("public void updateClob(String columnLabel, Reader reader)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader) throws SQLException {
    logger.debug("public void updateNClob(int columnIndex, Reader reader)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader) throws SQLException {
    logger.debug("public void updateNClob(String columnLabel, Reader reader)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
    logger.debug("public <T> T getObject(int columnIndex,Class<T> type)", false);
    if (SQLData.class.isAssignableFrom(type)) {
      Optional<Supplier<SQLData>> typeFactory = SnowflakeObjectTypeFactories.get(type);
      SQLData instance =
          typeFactory
              .map(Supplier::get)
              .orElseGet(() -> createUsingReflection((Class<SQLData>) type));
      SQLInput sqlInput = (SQLInput) getObject(columnIndex);
      instance.readSQL(sqlInput, null);
      return (T) instance;
    } else {
      return (T) getObject(columnIndex);
    }
  }

  private SQLData createUsingReflection(Class<? extends SQLData> type) {
    try {
      return type.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
    logger.debug("public <T> T getObject(String columnLabel,Class<T> type)", false);
    return getObject(findColumn(columnLabel), type);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    logger.debug("public <T> T unwrap(Class<T> iface)", false);

    if (!iface.isInstance(this)) {
      throw new SQLException(
          this.getClass().getName() + " not unwrappable from " + iface.getName());
    }
    return (T) this;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    logger.debug("public boolean isWrapperFor(Class<?> iface)", false);

    return iface.isInstance(this);
  }
}
