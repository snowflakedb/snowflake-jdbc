package net.snowflake.client.jdbc;

import static net.snowflake.client.jdbc.SnowflakeUtil.mapSFExceptionToSQLException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import net.snowflake.client.core.ColumnTypeHelper;
import net.snowflake.client.core.JsonSqlInput;
import net.snowflake.client.core.ObjectMapperFactory;
import net.snowflake.client.core.SFBaseResultSet;
import net.snowflake.client.core.SFBaseSession;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.arrow.StructObjectWrapper;
import net.snowflake.client.core.structs.SQLDataCreationHelper;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.common.core.SqlState;

/** Base class for query result set and metadata result set */
public abstract class SnowflakeBaseResultSet implements ResultSet {
  private static final SFLogger logger = SFLoggerFactory.getLogger(SnowflakeBaseResultSet.class);
  private final int resultSetType;
  private final int resultSetConcurrency;
  private final int resultSetHoldability;
  protected SFBaseResultSet sfBaseResultSet;
  // Snowflake supports sessionless result set. For this case, there is no
  // statement for this result set.
  protected final Statement statement;
  protected SnowflakeResultSetMetaDataV1 resultSetMetaData = null;
  protected Map<String, Object> parameters = new HashMap<>();
  private int fetchSize = 0;
  protected SFBaseSession session;
  private final SnowflakeResultSetSerializableV1 serializable;
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getObjectMapper();

  SnowflakeBaseResultSet(Statement statement) throws SQLException {
    this.statement = statement;
    this.resultSetType = statement.getResultSetType();
    this.resultSetConcurrency = statement.getResultSetConcurrency();
    this.resultSetHoldability = statement.getResultSetHoldability();
    this.session = maybeGetSession(statement);
    this.serializable = null;
  }

  private static SFBaseSession maybeGetSession(Statement statement) {
    try {
      return statement.unwrap(SnowflakeStatementV1.class).connection.getSFBaseSession();
    } catch (SQLException e) {
      // This exception shouldn't be hit. Statement class should be able to be unwrapped.
      logger.error(
          "Unable to unwrap SnowflakeStatementV1 class to retrieve session. Session is null.",
          false);
      return null;
    }
  }

  /**
   * Create an sessionless result set, there is no statement and session for the result set.
   *
   * @param resultSetSerializable The result set serializable object which includes all metadata to
   *     create the result set
   * @throws SQLException if an error occurs
   */
  public SnowflakeBaseResultSet(SnowflakeResultSetSerializableV1 resultSetSerializable)
      throws SQLException {
    // This is a sessionless result set, so there is no actual statement for it.
    this.statement = new SnowflakeStatementV1.NoOpSnowflakeStatementV1();
    this.resultSetType = resultSetSerializable.getResultSetType();
    this.resultSetConcurrency = resultSetSerializable.getResultSetConcurrency();
    this.resultSetHoldability = resultSetSerializable.getResultSetHoldability();
    this.session = null;
    this.serializable = resultSetSerializable;
  }

  /**
   * This should never be used. Simply needed this for SFAsynchronousResult subclass
   *
   * @throws SQLException if an error occurs
   */
  protected SnowflakeBaseResultSet() throws SQLException {
    this.resultSetType = 0;
    this.resultSetConcurrency = 0;
    this.resultSetHoldability = 0;
    this.statement = new SnowflakeStatementV1.NoOpSnowflakeStatementV1();
    this.session = null;
    this.serializable = null;
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

  /**
   * Get Date value
   *
   * @param columnIndex column index
   * @param tz timezone
   * @return Date value at column index
   * @throws SQLException if data at column index is incompatible with Date type
   */
  public abstract Date getDate(int columnIndex, TimeZone tz) throws SQLException;

  private boolean getGetDateUseNullTimezone() {
    if (this.session != null) {
      return this.session.getGetDateUseNullTimezone();
    }

    if (this.serializable != null) {
      return this.serializable.getGetDateUseNullTimezone();
    }

    return false;
  }

  @Override
  public Date getDate(int columnIndex) throws SQLException {
    raiseSQLExceptionIfResultSetIsClosed();
    return getDate(columnIndex, getGetDateUseNullTimezone() ? null : TimeZone.getDefault());
  }

  @Override
  public abstract Time getTime(int columnIndex) throws SQLException;

  @Override
  public Timestamp getTimestamp(int columnIndex) throws SQLException {
    raiseSQLExceptionIfResultSetIsClosed();
    return getTimestamp(columnIndex, (TimeZone) null);
  }

  /**
   * Get timestamp value
   *
   * @param columnIndex column index
   * @param tz timezone
   * @return timestamp value at column index
   * @throws SQLException if data at column index is incompatible with timestamp
   */
  public abstract Timestamp getTimestamp(int columnIndex, TimeZone tz) throws SQLException;

  @Override
  public InputStream getAsciiStream(int columnIndex) throws SQLException {
    logger.trace("InputStream getAsciiStream(int columnIndex)", false);
    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  /**
   * @deprecated
   */
  @Deprecated
  @Override
  public InputStream getUnicodeStream(int columnIndex) throws SQLException {
    logger.trace("InputStream getUnicodeStream(int columnIndex)", false);
    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public InputStream getBinaryStream(int columnIndex) throws SQLException {
    logger.trace("InputStream getBinaryStream(int columnIndex)", false);
    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public String getString(String columnLabel) throws SQLException {
    logger.trace("String getString(String columnLabel)", false);

    return getString(findColumn(columnLabel));
  }

  @Override
  public boolean getBoolean(String columnLabel) throws SQLException {
    logger.trace("boolean getBoolean(String columnLabel)", false);

    return getBoolean(findColumn(columnLabel));
  }

  @Override
  public byte getByte(String columnLabel) throws SQLException {
    logger.trace("byte getByte(String columnLabel)", false);
    raiseSQLExceptionIfResultSetIsClosed();

    return getByte(findColumn(columnLabel));
  }

  @Override
  public short getShort(String columnLabel) throws SQLException {
    logger.trace("short getShort(String columnLabel)", false);

    return getShort(findColumn(columnLabel));
  }

  @Override
  public int getInt(String columnLabel) throws SQLException {
    logger.trace("int getInt(String columnLabel)", false);

    return getInt(findColumn(columnLabel));
  }

  @Override
  public long getLong(String columnLabel) throws SQLException {
    logger.trace("long getLong(String columnLabel)", false);

    return getLong(findColumn(columnLabel));
  }

  @Override
  public float getFloat(String columnLabel) throws SQLException {
    logger.trace("float getFloat(String columnLabel)", false);

    return getFloat(findColumn(columnLabel));
  }

  @Override
  public double getDouble(String columnLabel) throws SQLException {
    logger.trace("double getDouble(String columnLabel)", false);

    return getDouble(findColumn(columnLabel));
  }

  /**
   * @deprecated
   */
  @Deprecated
  @Override
  public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
    logger.trace("BigDecimal getBigDecimal(String columnLabel, " + "int scale)", false);

    return getBigDecimal(findColumn(columnLabel), scale);
  }

  @Override
  public byte[] getBytes(String columnLabel) throws SQLException {
    logger.trace("byte[] getBytes(String columnLabel)", false);

    return getBytes(findColumn(columnLabel));
  }

  @Override
  public Date getDate(String columnLabel) throws SQLException {
    logger.trace("Date getDate(String columnLabel)", false);

    return getDate(findColumn(columnLabel));
  }

  @Override
  public Time getTime(String columnLabel) throws SQLException {
    logger.trace("Time getTime(String columnLabel)", false);

    return getTime(findColumn(columnLabel));
  }

  @Override
  public Timestamp getTimestamp(String columnLabel) throws SQLException {
    logger.trace("Timestamp getTimestamp(String columnLabel)", false);

    return getTimestamp(findColumn(columnLabel));
  }

  @Override
  public InputStream getAsciiStream(String columnLabel) throws SQLException {
    logger.trace("InputStream getAsciiStream(String columnLabel)", false);
    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  /**
   * @deprecated
   */
  @Deprecated
  @Override
  public InputStream getUnicodeStream(String columnLabel) throws SQLException {
    logger.trace("InputStream getUnicodeStream(String columnLabel)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public InputStream getBinaryStream(String columnLabel) throws SQLException {
    logger.trace("InputStream getBinaryStream(String columnLabel)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    logger.trace("SQLWarning getWarnings()", false);
    raiseSQLExceptionIfResultSetIsClosed();
    return null;
  }

  @Override
  public void clearWarnings() throws SQLException {
    logger.trace("void clearWarnings()", false);
    raiseSQLExceptionIfResultSetIsClosed();
  }

  @Override
  public String getCursorName() throws SQLException {
    logger.trace("String getCursorName()", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    logger.trace("ResultSetMetaData getMetaData()", false);
    raiseSQLExceptionIfResultSetIsClosed();
    return resultSetMetaData;
  }

  @Override
  public Object getObject(String columnLabel) throws SQLException {
    logger.trace("Object getObject(String columnLabel)", false);

    return getObject(findColumn(columnLabel));
  }

  @Override
  public int findColumn(String columnLabel) throws SQLException {
    logger.trace("int findColumn(String columnLabel)", false);
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
    logger.trace("Reader getCharacterStream(int columnIndex)", false);
    raiseSQLExceptionIfResultSetIsClosed();
    String streamData = getString(columnIndex);
    return (streamData == null) ? null : new StringReader(streamData);
  }

  @Override
  public Reader getCharacterStream(String columnLabel) throws SQLException {
    logger.trace("Reader getCharacterStream(String columnLabel)", false);
    return getCharacterStream(findColumn(columnLabel));
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
    logger.trace("BigDecimal getBigDecimal(String columnLabel)", false);

    return getBigDecimal(findColumn(columnLabel));
  }

  @Override
  public void beforeFirst() throws SQLException {
    logger.trace("void beforeFirst()", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void afterLast() throws SQLException {
    logger.trace("void afterLast()", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public boolean first() throws SQLException {
    logger.trace("boolean first()", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public boolean last() throws SQLException {
    logger.trace("boolean last()", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public boolean absolute(int row) throws SQLException {
    logger.trace("boolean absolute(int row)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public boolean relative(int rows) throws SQLException {
    logger.trace("boolean relative(int rows)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public boolean previous() throws SQLException {
    logger.trace("boolean previous()", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public int getFetchDirection() throws SQLException {
    logger.trace("int getFetchDirection()", false);
    raiseSQLExceptionIfResultSetIsClosed();
    return ResultSet.FETCH_FORWARD;
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    logger.trace("void setFetchDirection(int direction)", false);

    raiseSQLExceptionIfResultSetIsClosed();
    if (direction != ResultSet.FETCH_FORWARD) {
      throw new SnowflakeLoggedFeatureNotSupportedException(session);
    }
  }

  @Override
  public int getFetchSize() throws SQLException {
    logger.trace("int getFetchSize()", false);
    raiseSQLExceptionIfResultSetIsClosed();
    return this.fetchSize;
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    logger.trace("void setFetchSize(int rows)", false);
    raiseSQLExceptionIfResultSetIsClosed();

    this.fetchSize = rows;
  }

  @Override
  public int getType() throws SQLException {
    logger.trace("int getType()", false);
    raiseSQLExceptionIfResultSetIsClosed();
    return resultSetType;
  }

  @Override
  public int getConcurrency() throws SQLException {
    logger.trace("int getConcurrency()", false);
    raiseSQLExceptionIfResultSetIsClosed();
    return resultSetConcurrency;
  }

  @Override
  public boolean rowUpdated() throws SQLException {
    logger.trace("boolean rowUpdated()", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public boolean rowInserted() throws SQLException {
    logger.trace("boolean rowInserted()", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public boolean rowDeleted() throws SQLException {
    logger.trace("boolean rowDeleted()", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateNull(int columnIndex) throws SQLException {
    logger.trace("void updateNull(int columnIndex)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateBoolean(int columnIndex, boolean x) throws SQLException {
    logger.trace("void updateBoolean(int columnIndex, boolean x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateByte(int columnIndex, byte x) throws SQLException {
    logger.trace("void updateByte(int columnIndex, byte x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateShort(int columnIndex, short x) throws SQLException {
    logger.trace("void updateShort(int columnIndex, short x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateInt(int columnIndex, int x) throws SQLException {
    logger.trace("void updateInt(int columnIndex, int x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateLong(int columnIndex, long x) throws SQLException {
    logger.trace("void updateLong(int columnIndex, long x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateFloat(int columnIndex, float x) throws SQLException {
    logger.trace("void updateFloat(int columnIndex, float x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateDouble(int columnIndex, double x) throws SQLException {
    logger.trace("void updateDouble(int columnIndex, double x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
    logger.trace("void updateBigDecimal(int columnIndex, BigDecimal x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateString(int columnIndex, String x) throws SQLException {
    logger.trace("void updateString(int columnIndex, String x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateBytes(int columnIndex, byte[] x) throws SQLException {
    logger.trace("void updateBytes(int columnIndex, byte[] x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateDate(int columnIndex, Date x) throws SQLException {
    logger.trace("void updateDate(int columnIndex, Date x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateTime(int columnIndex, Time x) throws SQLException {
    logger.trace("void updateTime(int columnIndex, Time x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
    logger.trace("void updateTimestamp(int columnIndex, Timestamp x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
    logger.trace(
        "public void updateAsciiStream(int columnIndex, " + "InputStream x, int length)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
    logger.trace(
        "public void updateBinaryStream(int columnIndex, " + "InputStream x, int length)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
    logger.trace(
        "public void updateCharacterStream(int columnIndex, " + "Reader x, int length)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
    logger.trace(
        "public void updateObject(int columnIndex, Object x, " + "int scaleOrLength)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateObject(int columnIndex, Object x) throws SQLException {
    logger.trace("void updateObject(int columnIndex, Object x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateNull(String columnLabel) throws SQLException {
    logger.trace("void updateNull(String columnLabel)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateBoolean(String columnLabel, boolean x) throws SQLException {
    logger.trace("void updateBoolean(String columnLabel, boolean x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateByte(String columnLabel, byte x) throws SQLException {
    logger.trace("void updateByte(String columnLabel, byte x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateShort(String columnLabel, short x) throws SQLException {
    logger.trace("void updateShort(String columnLabel, short x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateInt(String columnLabel, int x) throws SQLException {
    logger.trace("void updateInt(String columnLabel, int x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateLong(String columnLabel, long x) throws SQLException {
    logger.trace("void updateLong(String columnLabel, long x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateFloat(String columnLabel, float x) throws SQLException {
    logger.trace("void updateFloat(String columnLabel, float x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateDouble(String columnLabel, double x) throws SQLException {
    logger.trace("void updateDouble(String columnLabel, double x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
    logger.trace("void updateBigDecimal(String columnLabel, " + "BigDecimal x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateString(String columnLabel, String x) throws SQLException {
    logger.trace("void updateString(String columnLabel, String x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateBytes(String columnLabel, byte[] x) throws SQLException {
    logger.trace("void updateBytes(String columnLabel, byte[] x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateDate(String columnLabel, Date x) throws SQLException {
    logger.trace("void updateDate(String columnLabel, Date x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateTime(String columnLabel, Time x) throws SQLException {
    logger.trace("void updateTime(String columnLabel, Time x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
    logger.trace("void updateTimestamp(String columnLabel, Timestamp x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
    logger.trace(
        "public void updateAsciiStream(String columnLabel, " + "InputStream x, int length)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, int length)
      throws SQLException {
    logger.trace(
        "public void updateBinaryStream(String columnLabel, " + "InputStream x, int length)",
        false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, int length)
      throws SQLException {
    logger.trace(
        "public void updateCharacterStream(String columnLabel, " + "Reader reader,int length)",
        false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
    logger.trace(
        "public void updateObject(String columnLabel, Object x, " + "int scaleOrLength)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateObject(String columnLabel, Object x) throws SQLException {
    logger.trace("void updateObject(String columnLabel, Object x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void insertRow() throws SQLException {
    logger.trace("void insertRow()", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateRow() throws SQLException {
    logger.trace("void updateRow()", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void deleteRow() throws SQLException {
    logger.trace("void deleteRow()", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void refreshRow() throws SQLException {
    logger.trace("void refreshRow()", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void cancelRowUpdates() throws SQLException {
    logger.trace("void cancelRowUpdates()", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void moveToInsertRow() throws SQLException {
    logger.trace("void moveToInsertRow()", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void moveToCurrentRow() throws SQLException {
    logger.trace("void moveToCurrentRow()", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public Statement getStatement() throws SQLException {
    logger.trace("Statement getStatement()", false);
    raiseSQLExceptionIfResultSetIsClosed();
    return statement;
  }

  @Override
  public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
    logger.trace("Object getObject(int columnIndex, Map<String, " + "Class<?>> map)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public Ref getRef(int columnIndex) throws SQLException {
    logger.trace("Ref getRef(int columnIndex)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public Blob getBlob(int columnIndex) throws SQLException {
    logger.trace("Blob getBlob(int columnIndex)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public Clob getClob(int columnIndex) throws SQLException {
    logger.trace("Clob getClob(int columnIndex)", false);
    String columnValue = getString(columnIndex);

    return columnValue == null ? null : new SnowflakeClob(columnValue);
  }

  @Override
  public Array getArray(int columnIndex) throws SQLException {
    logger.trace("Array getArray(int columnIndex)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
    logger.trace(
        "public Object getObject(String columnLabel, " + "Map<String, Class<?>> map)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public Ref getRef(String columnLabel) throws SQLException {
    logger.trace("Ref getRef(String columnLabel)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public Blob getBlob(String columnLabel) throws SQLException {
    logger.trace("Blob getBlob(String columnLabel)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public Clob getClob(String columnLabel) throws SQLException {
    logger.trace("Clob getClob(String columnLabel)", false);
    String columnValue = getString(columnLabel);

    return columnValue == null ? null : new SnowflakeClob(columnValue);
  }

  @Override
  public Array getArray(String columnLabel) throws SQLException {
    logger.trace("Array getArray(String columnLabel)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public Date getDate(int columnIndex, Calendar cal) throws SQLException {
    logger.trace("Date getDate(int columnIndex, Calendar cal)", false);
    return getDate(columnIndex, cal.getTimeZone());
  }

  @Override
  public Date getDate(String columnLabel, Calendar cal) throws SQLException {
    logger.trace("Date getDate(String columnLabel, Calendar cal)", false);

    return getDate(findColumn(columnLabel), cal.getTimeZone());
  }

  @Override
  public Time getTime(int columnIndex, Calendar cal) throws SQLException {
    logger.trace("Time getTime(int columnIndex, Calendar cal)", false);

    return getTime(columnIndex);
  }

  @Override
  public Time getTime(String columnLabel, Calendar cal) throws SQLException {
    logger.trace("Time getTime(String columnLabel, Calendar cal)", false);

    return getTime(columnLabel);
  }

  @Override
  public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
    logger.trace("Timestamp getTimestamp(int columnIndex, Calendar cal)", false);

    return getTimestamp(columnIndex, cal.getTimeZone());
  }

  @Override
  public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
    logger.trace("Timestamp getTimestamp(String columnLabel, " + "Calendar cal)", false);

    return getTimestamp(findColumn(columnLabel), cal.getTimeZone());
  }

  @Override
  public URL getURL(int columnIndex) throws SQLException {
    logger.trace("URL getURL(int columnIndex)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public URL getURL(String columnLabel) throws SQLException {
    logger.trace("URL getURL(String columnLabel)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateRef(int columnIndex, Ref x) throws SQLException {
    logger.trace("void updateRef(int columnIndex, Ref x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateRef(String columnLabel, Ref x) throws SQLException {
    logger.trace("void updateRef(String columnLabel, Ref x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateBlob(int columnIndex, Blob x) throws SQLException {
    logger.trace("void updateBlob(int columnIndex, Blob x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateBlob(String columnLabel, Blob x) throws SQLException {
    logger.trace("void updateBlob(String columnLabel, Blob x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateClob(int columnIndex, Clob x) throws SQLException {
    logger.trace("void updateClob(int columnIndex, Clob x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateClob(String columnLabel, Clob x) throws SQLException {
    logger.trace("void updateClob(String columnLabel, Clob x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateArray(int columnIndex, Array x) throws SQLException {
    logger.trace("void updateArray(int columnIndex, Array x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateArray(String columnLabel, Array x) throws SQLException {
    logger.trace("void updateArray(String columnLabel, Array x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public RowId getRowId(int columnIndex) throws SQLException {
    logger.trace("RowId getRowId(int columnIndex)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public RowId getRowId(String columnLabel) throws SQLException {
    logger.trace("RowId getRowId(String columnLabel)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateRowId(int columnIndex, RowId x) throws SQLException {
    logger.trace("void updateRowId(int columnIndex, RowId x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateRowId(String columnLabel, RowId x) throws SQLException {
    logger.trace("void updateRowId(String columnLabel, RowId x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public int getHoldability() throws SQLException {
    logger.trace("int getHoldability()", false);
    raiseSQLExceptionIfResultSetIsClosed();
    return resultSetHoldability;
  }

  @Override
  public void updateNString(int columnIndex, String nString) throws SQLException {
    logger.trace("void updateNString(int columnIndex, String nString)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateNString(String columnLabel, String nString) throws SQLException {
    logger.trace("void updateNString(String columnLabel, String nString)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
    logger.trace("void updateNClob(int columnIndex, NClob nClob)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
    logger.trace("void updateNClob(String columnLabel, NClob nClob)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public NClob getNClob(int columnIndex) throws SQLException {
    logger.trace("NClob getNClob(int columnIndex)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public NClob getNClob(String columnLabel) throws SQLException {
    logger.trace("NClob getNClob(String columnLabel)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public SQLXML getSQLXML(int columnIndex) throws SQLException {
    logger.trace("SQLXML getSQLXML(int columnIndex)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public SQLXML getSQLXML(String columnLabel) throws SQLException {
    logger.trace("SQLXML getSQLXML(String columnLabel)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
    logger.trace("void updateSQLXML(int columnIndex, SQLXML xmlObject)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
    logger.trace("void updateSQLXML(String columnLabel, SQLXML xmlObject)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public String getNString(int columnIndex) throws SQLException {
    logger.trace("String getNString(int columnIndex)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public String getNString(String columnLabel) throws SQLException {
    logger.trace("String getNString(String columnLabel)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public Reader getNCharacterStream(int columnIndex) throws SQLException {
    logger.trace("Reader getNCharacterStream(int columnIndex)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public Reader getNCharacterStream(String columnLabel) throws SQLException {
    logger.trace("Reader getNCharacterStream(String columnLabel)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    logger.trace(
        "public void updateNCharacterStream(int columnIndex, " + "Reader x, long length)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader, long length)
      throws SQLException {
    logger.trace(
        "public void updateNCharacterStream(String columnLabel, " + "Reader reader,long length)",
        false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
    logger.trace(
        "public void updateAsciiStream(int columnIndex, " + "InputStream x, long length)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
    logger.trace(
        "public void updateBinaryStream(int columnIndex, " + "InputStream x, long length)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    logger.trace(
        "public void updateCharacterStream(int columnIndex, Reader x, " + "long length)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, long length)
      throws SQLException {
    logger.trace(
        "public void updateAsciiStream(String columnLabel, " + "InputStream x, long length)",
        false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, long length)
      throws SQLException {
    logger.trace(
        "public void updateBinaryStream(String columnLabel, " + "InputStream x, long length)",
        false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, long length)
      throws SQLException {
    logger.trace(
        "public void updateCharacterStream(String columnLabel, " + "Reader reader,long length)",
        false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream, long length)
      throws SQLException {
    logger.trace(
        "public void updateBlob(int columnIndex, InputStream " + "inputStream, long length)",
        false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream, long length)
      throws SQLException {
    logger.trace(
        "public void updateBlob(String columnLabel, " + "InputStream inputStream,long length)",
        false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
    logger.trace("void updateClob(int columnIndex, Reader reader, " + "long length)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
    logger.trace(
        "public void updateClob(String columnLabel, Reader reader, " + "long length)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
    logger.trace(
        "public void updateNClob(int columnIndex, Reader reader, " + "long length)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
    logger.trace(
        "public void updateNClob(String columnLabel, Reader reader, " + "long length)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
    logger.trace("void updateNCharacterStream(int columnIndex, Reader x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
    logger.trace(
        "public void updateNCharacterStream(String columnLabel, " + "Reader reader)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
    logger.trace("void updateAsciiStream(int columnIndex, InputStream x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
    logger.trace("void updateBinaryStream(int columnIndex, InputStream x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
    logger.trace("void updateCharacterStream(int columnIndex, Reader x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
    logger.trace("void updateAsciiStream(String columnLabel, InputStream x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
    logger.trace("void updateBinaryStream(String columnLabel, InputStream x)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
    logger.trace(
        "public void updateCharacterStream(String columnLabel, " + "Reader reader)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
    logger.trace("void updateBlob(int columnIndex, InputStream inputStream)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
    logger.trace("void updateBlob(String columnLabel, InputStream " + "inputStream)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateClob(int columnIndex, Reader reader) throws SQLException {
    logger.trace("void updateClob(int columnIndex, Reader reader)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateClob(String columnLabel, Reader reader) throws SQLException {
    logger.trace("void updateClob(String columnLabel, Reader reader)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader) throws SQLException {
    logger.trace("void updateNClob(int columnIndex, Reader reader)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader) throws SQLException {
    logger.trace("void updateNClob(String columnLabel, Reader reader)", false);

    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
    logger.trace("<T> T getObject(int columnIndex,Class<T> type)", false);
    if (resultSetMetaData.isStructuredTypeColumn(columnIndex)) {
      if (SQLData.class.isAssignableFrom(type)) {
        SQLInput sqlInput =
            SnowflakeUtil.mapSFExceptionToSQLException(
                () -> {
                  StructObjectWrapper structObjectWrapper =
                      (StructObjectWrapper) sfBaseResultSet.getObjectWithoutString(columnIndex);
                  if (structObjectWrapper == null) {
                    return null;
                  }
                  return (SQLInput) structObjectWrapper.getObject();
                });
        if (sqlInput == null) {
          return null;
        } else {
          SQLData instance = (SQLData) SQLDataCreationHelper.create(type);
          instance.readSQL(sqlInput, null);
          return (T) instance;
        }
      } else if (Map.class.isAssignableFrom(type)) {
        Object object = getObject(columnIndex);
        if (object == null) {
          return null;
        } else if (object instanceof Map) {
          throw new SQLException(
              "Arrow native struct couldn't be converted to String. To map to SqlData the method getObject(int columnIndex, Class type) should be used");
        } else {
          try {
            return (T)
                OBJECT_MAPPER.readValue(
                    (String) object, new TypeReference<Map<Object, Object>>() {});
          } catch (JsonProcessingException e) {
            throw new SQLException("Value couldn't be converted to Map");
          }
        }
      }
    }
    if (String.class.isAssignableFrom(type)) {
      return (T) getString(columnIndex);
    } else if (Boolean.class.isAssignableFrom(type)) {
      return (T) (Boolean) getBoolean(columnIndex);
    } else if (Byte.class.isAssignableFrom(type)) {
      return (T) (Byte) getByte(columnIndex);
    } else if (Short.class.isAssignableFrom(type)) {
      return (T) (Short) getShort(columnIndex);
    } else if (Integer.class.isAssignableFrom(type)) {
      return (T) (Integer) getInt(columnIndex);
    } else if (Long.class.isAssignableFrom(type)) {
      return (T) (Long) getLong(columnIndex);
    } else if (Float.class.isAssignableFrom(type)) {
      return (T) (Float) getFloat(columnIndex);
    } else if (Double.class.isAssignableFrom(type)) {
      return (T) (Double) getDouble(columnIndex);
    } else if (Date.class.isAssignableFrom(type)) {
      return (T) getDate(columnIndex);
    } else if (Time.class.isAssignableFrom(type)) {
      return (T) getTime(columnIndex);
    } else if (Timestamp.class.isAssignableFrom(type)) {
      return (T) getTimestamp(columnIndex);
    } else if (BigDecimal.class.isAssignableFrom(type)) {
      return (T) getBigDecimal(columnIndex);
    } else {
      logger.debug(
          "Unsupported type passed to getObject(int columnIndex,Class<T> type): " + type.getName());
      throw new SQLException(
          "Type passed to 'getObject(int columnIndex,Class<T> type)' is unsupported. Type: "
              + type.getName());
    }
  }

  public <T> List<T> getList(int columnIndex, Class<T> type) throws SQLException {
    logger.trace("<T> List<T> getList(int columnIndex, Class<T> type)", false);
    if (!resultSetMetaData.isStructuredTypeColumn(columnIndex)) {
      throw new SnowflakeLoggedFeatureNotSupportedException(session);
    }
    T[] sqlInputs = getArray(columnIndex, type);
    return Arrays.asList(sqlInputs);
  }

  public <T> T[] getArray(int columnIndex, Class<T> type) throws SQLException {
    logger.trace("<T> T[] getArray(int columnIndex, Class<T> type)", false);
    if (!resultSetMetaData.isStructuredTypeColumn(columnIndex)) {
      throw new SnowflakeLoggedFeatureNotSupportedException(session);
    }
    List<FieldMetadata> fieldMetadataList = resultSetMetaData.getColumnFields(columnIndex);
    if (fieldMetadataList.size() != 1) {
      throw new SQLException("Wrong size of fields for array type " + fieldMetadataList.size());
    }
    FieldMetadata fieldMetadata = fieldMetadataList.get(0);
    int columnSubType = fieldMetadata.getType();
    int columnType = ColumnTypeHelper.getColumnType(fieldMetadata.getType(), session);
    int scale = fieldMetadata.getScale();
    TimeZone tz = sfBaseResultSet.getSessionTimeZone();
    Array array = getArray(columnIndex);
    if (array == null) {
      return null;
    }
    Object[] objects = (Object[]) array.getArray();
    T[] arr = (T[]) java.lang.reflect.Array.newInstance(type, objects.length);
    int counter = 0;
    for (Object value : objects) {
      if (value == null) {
        arr[counter++] = null;
      } else if (type.isAssignableFrom(value.getClass())) {
        arr[counter++] = (T) value;
      } else {
        if (SQLData.class.isAssignableFrom(type)) {
          SQLData instance = (SQLData) SQLDataCreationHelper.create(type);
          SQLInput sqlInput =
              sfBaseResultSet.createSqlInputForColumn(
                  value, objects.getClass(), columnIndex, session, fieldMetadata.getFields());
          instance.readSQL(sqlInput, null);
          arr[counter++] = (T) instance;
        } else if (String.class.isAssignableFrom(type)) {
          arr[counter++] =
              mapSFExceptionToSQLException(
                  () ->
                      (T)
                          sfBaseResultSet
                              .getConverters()
                              .getStringConverter()
                              .getString(value, columnType, columnSubType, scale));
        } else if (Boolean.class.isAssignableFrom(type)) {
          arr[counter++] =
              mapSFExceptionToSQLException(
                  () ->
                      (T)
                          sfBaseResultSet
                              .getConverters()
                              .getBooleanConverter()
                              .getBoolean(value, columnType));
        } else if (Byte.class.isAssignableFrom(type)) {
          arr[counter++] =
              mapSFExceptionToSQLException(
                  () ->
                      (T)
                          sfBaseResultSet
                              .getConverters()
                              .getBytesConverter()
                              .getBytes(value, columnType, columnSubType, scale));
        } else if (Short.class.isAssignableFrom(type)) {
          arr[counter++] =
              mapSFExceptionToSQLException(
                  () ->
                      (T)
                          Short.valueOf(
                              sfBaseResultSet
                                  .getConverters()
                                  .getNumberConverter()
                                  .getShort(value, columnType)));
        } else if (Integer.class.isAssignableFrom(type)) {
          arr[counter++] =
              mapSFExceptionToSQLException(
                  () ->
                      (T)
                          Integer.valueOf(
                              sfBaseResultSet
                                  .getConverters()
                                  .getNumberConverter()
                                  .getInt(value, columnType)));
        } else if (Long.class.isAssignableFrom(type)) {
          arr[counter++] =
              mapSFExceptionToSQLException(
                  () ->
                      (T)
                          Long.valueOf(
                              sfBaseResultSet
                                  .getConverters()
                                  .getNumberConverter()
                                  .getLong(value, columnType)));
        } else if (Float.class.isAssignableFrom(type)) {
          arr[counter++] =
              mapSFExceptionToSQLException(
                  () ->
                      (T)
                          Float.valueOf(
                              sfBaseResultSet
                                  .getConverters()
                                  .getNumberConverter()
                                  .getFloat(value, columnType)));
        } else if (Double.class.isAssignableFrom(type)) {
          arr[counter++] =
              mapSFExceptionToSQLException(
                  () ->
                      (T)
                          Double.valueOf(
                              sfBaseResultSet
                                  .getConverters()
                                  .getNumberConverter()
                                  .getDouble(value, columnType)));
        } else if (Date.class.isAssignableFrom(type)) {
          arr[counter++] =
              mapSFExceptionToSQLException(
                  () ->
                      (T)
                          sfBaseResultSet
                              .getConverters()
                              .dateStringConverter(session)
                              .convert((String) value));
        } else if (Time.class.isAssignableFrom(type)) {
          arr[counter++] =
              mapSFExceptionToSQLException(
                  () ->
                      (T)
                          sfBaseResultSet
                              .getConverters()
                              .timeFromStringConverter(session)
                              .convert((String) value));
        } else if (Timestamp.class.isAssignableFrom(type)) {
          mapSFExceptionToSQLException(
              () ->
                  (T)
                      sfBaseResultSet
                          .getConverters()
                          .timestampFromStringConverter(
                              columnSubType, columnType, scale, session, null, tz)
                          .convert((String) value));
        } else if (BigDecimal.class.isAssignableFrom(type)) {
          arr[counter++] = (T) getBigDecimal(columnIndex);
        } else {
          logger.debug(
              "Unsupported type passed to getArray(int columnIndex, Class<T> type): "
                  + type.getName());
          throw new SQLException(
              "Type passed to 'getObject(int columnIndex,Class<T> type)' is unsupported. Type: "
                  + type.getName());
        }
      }
    }
    return arr;
  }

  public <T> Map<String, T> getMap(int columnIndex, Class<T> type) throws SQLException {
    logger.trace("<T> Map<String, T> getMap(int columnIndex, Class<T> type)", false);
    if (!resultSetMetaData.isStructuredTypeColumn(columnIndex)) {
      throw new SnowflakeLoggedFeatureNotSupportedException(session);
    }
    List<FieldMetadata> fieldMetadataList = resultSetMetaData.getColumnFields(columnIndex);
    if (fieldMetadataList.size() != 2) {
      throw new SQLException(
          "Wrong size of fields metadata for map type " + fieldMetadataList.size());
    }
    FieldMetadata valueFieldMetadata = fieldMetadataList.get(1);
    int columnSubType = valueFieldMetadata.getType();
    int columnType = ColumnTypeHelper.getColumnType(valueFieldMetadata.getType(), session);
    int scale = valueFieldMetadata.getScale();
    TimeZone tz = sfBaseResultSet.getSessionTimeZone();
    StructObjectWrapper structObjectWrapper =
        (StructObjectWrapper)
            SnowflakeUtil.mapSFExceptionToSQLException(
                () -> sfBaseResultSet.getObjectWithoutString(columnIndex));
    if (structObjectWrapper == null || structObjectWrapper.getObject() == null) {
      return null;
    }
    Map<String, Object> map =
        mapSFExceptionToSQLException(
            () -> prepareMapWithValues(structObjectWrapper.getObject(), type));
    Map<String, T> resultMap = new HashMap<>();
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      if (SQLData.class.isAssignableFrom(type)) {
        SQLData instance = (SQLData) SQLDataCreationHelper.create(type);
        SQLInput sqlInput =
            sfBaseResultSet.createSqlInputForColumn(
                entry.getValue(),
                structObjectWrapper.getObject().getClass(),
                columnIndex,
                session,
                valueFieldMetadata.getFields());
        instance.readSQL(sqlInput, null);
        resultMap.put(entry.getKey(), (T) instance);
      } else if (String.class.isAssignableFrom(type)) {
        resultMap.put(
            entry.getKey(),
            mapSFExceptionToSQLException(
                () ->
                    (T)
                        sfBaseResultSet
                            .getConverters()
                            .getStringConverter()
                            .getString(entry.getValue(), columnType, columnSubType, scale)));
      } else if (Boolean.class.isAssignableFrom(type)) {
        resultMap.put(
            entry.getKey(),
            mapSFExceptionToSQLException(
                () ->
                    (T)
                        sfBaseResultSet
                            .getConverters()
                            .getBooleanConverter()
                            .getBoolean(entry.getValue(), columnType)));
      } else if (Byte.class.isAssignableFrom(type)) {
        resultMap.put(
            entry.getKey(),
            mapSFExceptionToSQLException(
                () ->
                    (T)
                        sfBaseResultSet
                            .getConverters()
                            .getBytesConverter()
                            .getBytes(entry.getValue(), columnType, columnSubType, scale)));
      } else if (Short.class.isAssignableFrom(type)) {
        resultMap.put(
            entry.getKey(),
            mapSFExceptionToSQLException(
                () ->
                    (T)
                        (Short)
                            sfBaseResultSet
                                .getConverters()
                                .getNumberConverter()
                                .getShort(entry.getValue(), columnType)));
      } else if (Integer.class.isAssignableFrom(type)) {
        resultMap.put(
            entry.getKey(),
            mapSFExceptionToSQLException(
                () ->
                    (T)
                        (Integer)
                            sfBaseResultSet
                                .getConverters()
                                .getNumberConverter()
                                .getInt(entry.getValue(), columnType)));
      } else if (Long.class.isAssignableFrom(type)) {
        resultMap.put(
            entry.getKey(),
            mapSFExceptionToSQLException(
                () ->
                    (T)
                        (Long)
                            sfBaseResultSet
                                .getConverters()
                                .getNumberConverter()
                                .getLong(entry.getValue(), columnType)));
      } else if (Float.class.isAssignableFrom(type)) {
        resultMap.put(
            entry.getKey(),
            mapSFExceptionToSQLException(
                () ->
                    (T)
                        (Float)
                            sfBaseResultSet
                                .getConverters()
                                .getNumberConverter()
                                .getFloat(entry.getValue(), columnType)));
      } else if (Double.class.isAssignableFrom(type)) {
        resultMap.put(
            entry.getKey(),
            mapSFExceptionToSQLException(
                () ->
                    (T)
                        (Double)
                            sfBaseResultSet
                                .getConverters()
                                .getNumberConverter()
                                .getDouble(entry.getValue(), columnType)));
      } else if (BigDecimal.class.isAssignableFrom(type)) {
        resultMap.put(
            entry.getKey(),
            mapSFExceptionToSQLException(
                () ->
                    (T)
                        sfBaseResultSet
                            .getConverters()
                            .getNumberConverter()
                            .getBigDecimal(entry.getValue(), columnType)));
      } else if (Date.class.isAssignableFrom(type)) {
        resultMap.put(
            entry.getKey(),
            mapSFExceptionToSQLException(
                () -> (T) sfBaseResultSet.convertToDate(entry.getValue(), tz)));
      } else if (Time.class.isAssignableFrom(type)) {
        resultMap.put(
            entry.getKey(),
            mapSFExceptionToSQLException(
                () -> (T) sfBaseResultSet.convertToTime(entry.getValue(), scale)));

      } else if (Timestamp.class.isAssignableFrom(type)) {
        resultMap.put(
            entry.getKey(),
            mapSFExceptionToSQLException(
                () ->
                    (T)
                        sfBaseResultSet.convertToTimestamp(
                            entry.getValue(), columnType, columnSubType, tz, scale)));

      } else {
        logger.debug(
            "Unsupported type passed to getObject(int columnIndex,Class<T> type): "
                + type.getName());
        throw new SQLException(
            "Type passed to 'getObject(int columnIndex,Class<T> type)' is unsupported. Type: "
                + type.getName());
      }
    }

    return resultMap;
  }

  @Override
  public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
    logger.trace("<T> T getObject(String columnLabel,Class<T> type)", false);
    return getObject(findColumn(columnLabel), type);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    logger.trace("<T> T unwrap(Class<T> iface)", false);

    if (!iface.isInstance(this)) {
      throw new SQLException(
          this.getClass().getName() + " not unwrappable from " + iface.getName());
    }
    return (T) this;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    logger.trace("boolean isWrapperFor(Class<?> iface)", false);

    return iface.isInstance(this);
  }

  private <T> Map<String, Object> prepareMapWithValues(Object object, Class<T> type)
      throws SFException {
    if (object instanceof JsonSqlInput) {
      Map map = new HashMap<>();
      JsonNode jsonNode = ((JsonSqlInput) object).getInput();
      for (Iterator<String> it = jsonNode.fieldNames(); it.hasNext(); ) {
        String name = it.next();
        map.put(
            name,
            SQLData.class.isAssignableFrom(type)
                ? jsonNode.get(name)
                : SnowflakeUtil.getJsonNodeStringValue(jsonNode.get(name)));
      }
      return map;
    } else if (object instanceof Map) {
      return (Map<String, Object>) object;
    } else {
      throw new SFException(ErrorCode.INVALID_STRUCT_DATA, "Object couldn't be converted to map");
    }
  }
}
