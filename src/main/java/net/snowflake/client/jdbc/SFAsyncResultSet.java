/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import java.io.InputStream;
import java.io.Reader;
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
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Pattern;
import net.snowflake.client.core.QueryStatus;
import net.snowflake.client.core.SFBaseResultSet;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SFSession;
import net.snowflake.common.core.SqlState;

/** SFAsyncResultSet implementation */
class SFAsyncResultSet extends SnowflakeBaseResultSet implements SnowflakeResultSet, ResultSet {
  private final SFBaseResultSet sfBaseResultSet;
  private ResultSet resultSetForNext = new EmptyResultSet();
  private boolean resultSetForNextInitialized = false;
  private String queryID;
  private SFSession session;
  private Statement extraStatement;

  /**
   * Constructor takes an inputstream from the API response that we get from executing a SQL
   * statement.
   *
   * <p>The constructor will fetch the first row (if any) so that it can initialize the
   * ResultSetMetaData.
   *
   * @param sfBaseResultSet snowflake core base result rest object
   * @param statement query statement that generates this result set
   * @throws SQLException if failed to construct snowflake result set metadata
   */
  SFAsyncResultSet(SFBaseResultSet sfBaseResultSet, Statement statement) throws SQLException {
    super(statement);
    this.sfBaseResultSet = sfBaseResultSet;
    this.queryID = sfBaseResultSet.getQueryId();
    this.session = sfBaseResultSet.getSession();
    this.extraStatement = statement;
    try {
      this.resultSetMetaData = new SnowflakeResultSetMetaDataV1(sfBaseResultSet.getMetaData());
      this.resultSetMetaData.setQueryIdForAsyncResults(this.queryID);
      this.resultSetMetaData.setQueryType(SnowflakeResultSetMetaDataV1.QueryType.ASYNC);
    } catch (SFException ex) {
      throw new SnowflakeSQLException(
          ex.getCause(), ex.getSqlState(), ex.getVendorCode(), ex.getParams());
    }
  }

  /**
   * Constructor takes a result set serializable object to create a sessionless result set.
   *
   * @param sfBaseResultSet snowflake core base result rest object
   * @param resultSetSerializable The result set serializable object which includes all metadata to
   *     create the result set
   * @throws SQLException if fails to create the result set object
   */
  public SFAsyncResultSet(
      SFBaseResultSet sfBaseResultSet, SnowflakeResultSetSerializableV1 resultSetSerializable)
      throws SQLException {
    super(resultSetSerializable);
    this.queryID = sfBaseResultSet.getQueryId();
    this.sfBaseResultSet = sfBaseResultSet;

    try {
      this.resultSetMetaData = new SnowflakeResultSetMetaDataV1(sfBaseResultSet.getMetaData());
      this.resultSetMetaData.setQueryIdForAsyncResults(this.queryID);
      this.resultSetMetaData.setQueryType(SnowflakeResultSetMetaDataV1.QueryType.ASYNC);
    } catch (SFException ex) {
      throw new SnowflakeSQLException(
          ex.getCause(), ex.getSqlState(), ex.getVendorCode(), ex.getParams());
    }
  }

  public SFAsyncResultSet(String queryID) throws SQLException {
    this.sfBaseResultSet = null;
    queryID.trim();
    if (!Pattern.matches("[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}", queryID)) {
      throw new SQLException(
          "The provided query ID " + queryID + " is invalid.", SqlState.INVALID_PARAMETER_VALUE);
    }
    this.queryID = queryID;
  }

  @Override
  protected void raiseSQLExceptionIfResultSetIsClosed() throws SQLException {
    if (isClosed()) {
      throw new SnowflakeSQLException(ErrorCode.RESULTSET_ALREADY_CLOSED);
    }
  }

  @Override
  public QueryStatus getStatus() throws SQLException {
    if (session == null) {
      throw new SQLException("Session not set");
    }
    if (this.queryID == null) {
      throw new SQLException("QueryID unknown");
    }
    return session.getQueryStatus(this.queryID);
  }

  /**
   * helper function for next() and getMetaData(). Calls result_scan to get resultSet after
   * asynchronous query call
   *
   * @throws SQLException
   */
  private void getRealResults() throws SQLException {
    if (!resultSetForNextInitialized) {
      QueryStatus qs = this.getStatus();
      int noDataRetry = 0;
      final int noDataMaxRetries = 30;
      final int[] retryPattern = {1, 1, 2, 3, 4, 8, 10};
      final int maxIndex = retryPattern.length - 1;
      int retry = 0;
      while (qs != QueryStatus.SUCCESS) {
        // if query is not running due to a failure (Aborted, failed with error, etc), generate
        // exception
        if (!QueryStatus.isStillRunning(qs) && qs.getValue() != QueryStatus.SUCCESS.getValue()) {
          throw new SQLException(
              "Status of query associated with resultSet is "
                  + qs.getDescription()
                  + ". Results not generated.");
        }
        // if no data about the query is returned after about 2 minutes, give up
        if (qs == QueryStatus.NO_DATA) {
          noDataRetry++;
          if (noDataRetry >= noDataMaxRetries) {
            throw new SQLException(
                "Cannot retrieve data on the status of this query. No information returned from server for queryID={}.",
                this.queryID);
          }
        }
        try {
          // Sleep for an amount before trying again. Exponential backoff up to 5 seconds
          // implemented.
          Thread.sleep(500 * retryPattern[retry]);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        if (retry < maxIndex) {
          retry++;
        }
        qs = this.getStatus();
      }
      resultSetForNext =
          extraStatement.executeQuery("select * from table(result_scan('" + this.queryID + "'))");
      resultSetForNextInitialized = true;
    }
  }

  /**
   * Advance to next row
   *
   * @return true if next row exists, false otherwise
   * @throws SQLException if failed to move to the next row
   */
  @Override
  public boolean next() throws SQLException {
    getRealResults();
    return resultSetForNext.next();
  }

  @Override
  public void close() throws SQLException {
    close(true);
  }

  public void close(boolean removeClosedResultSetFromStatement) throws SQLException {
    // no SQLException is raised.
    resultSetForNext.close();
    if (sfBaseResultSet != null) {
      sfBaseResultSet.close();
    }
    if (removeClosedResultSetFromStatement && statement.isWrapperFor(SnowflakeStatementV1.class)) {
      statement.unwrap(SnowflakeStatementV1.class).removeClosedResultSet(this);
    }
  }

  public String getQueryID() {
    return this.queryID;
  }

  public boolean wasNull() throws SQLException {
    raiseSQLExceptionIfResultSetIsClosed();
    return resultSetForNext.wasNull();
  }

  public String getString(int columnIndex) throws SQLException {
    raiseSQLExceptionIfResultSetIsClosed();
    return resultSetForNext.getString(columnIndex);
  }

  public void setSession(SFSession session) {
    this.session = session;
  }

  public void setStatement(Statement statement) {
    this.extraStatement = statement;
  }

  public boolean getBoolean(int columnIndex) throws SQLException {
    raiseSQLExceptionIfResultSetIsClosed();
    return resultSetForNext.getBoolean(columnIndex);
  }

  @Override
  public byte getByte(int columnIndex) throws SQLException {
    raiseSQLExceptionIfResultSetIsClosed();
    return resultSetForNext.getByte(columnIndex);
  }

  public short getShort(int columnIndex) throws SQLException {
    raiseSQLExceptionIfResultSetIsClosed();
    return resultSetForNext.getShort(columnIndex);
  }

  public int getInt(int columnIndex) throws SQLException {
    raiseSQLExceptionIfResultSetIsClosed();
    return resultSetForNext.getInt(columnIndex);
  }

  public long getLong(int columnIndex) throws SQLException {
    raiseSQLExceptionIfResultSetIsClosed();
    return resultSetForNext.getLong(columnIndex);
  }

  public float getFloat(int columnIndex) throws SQLException {
    raiseSQLExceptionIfResultSetIsClosed();
    return resultSetForNext.getFloat(columnIndex);
  }

  public double getDouble(int columnIndex) throws SQLException {
    raiseSQLExceptionIfResultSetIsClosed();
    return resultSetForNext.getDouble(columnIndex);
  }

  public Date getDate(int columnIndex, TimeZone tz) throws SQLException {
    // Note: currently we provide this API but it does not use TimeZone tz.
    // TODO: use the time zone passed from the arguments
    raiseSQLExceptionIfResultSetIsClosed();
    return resultSetForNext.getDate(columnIndex);
  }

  public Time getTime(int columnIndex) throws SQLException {
    raiseSQLExceptionIfResultSetIsClosed();
    return resultSetForNext.getTime(columnIndex);
  }

  public Timestamp getTimestamp(int columnIndex, TimeZone tz) throws SQLException {
    raiseSQLExceptionIfResultSetIsClosed();
    return resultSetForNext.unwrap(SnowflakeResultSetV1.class).getTimestamp(columnIndex, tz);
  }

  public ResultSetMetaData getMetaData() throws SQLException {
    raiseSQLExceptionIfResultSetIsClosed();
    getRealResults();
    this.resultSetMetaData =
        (SnowflakeResultSetMetaDataV1)
            resultSetForNext.unwrap(SnowflakeResultSetV1.class).getMetaData();
    this.resultSetMetaData.setQueryIdForAsyncResults(this.queryID);
    this.resultSetMetaData.setQueryType(SnowflakeResultSetMetaDataV1.QueryType.ASYNC);
    return resultSetMetaData;
  }

  public Object getObject(int columnIndex) throws SQLException {
    raiseSQLExceptionIfResultSetIsClosed();
    return resultSetForNext.getObject(columnIndex);
  }

  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    raiseSQLExceptionIfResultSetIsClosed();
    return resultSetForNext.getBigDecimal(columnIndex);
  }

  @Deprecated
  public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
    raiseSQLExceptionIfResultSetIsClosed();
    return resultSetForNext.getBigDecimal(columnIndex, scale);
  }

  public byte[] getBytes(int columnIndex) throws SQLException {
    raiseSQLExceptionIfResultSetIsClosed();
    return resultSetForNext.getBytes(columnIndex);
  }

  public int getRow() throws SQLException {
    raiseSQLExceptionIfResultSetIsClosed();

    return resultSetForNext.getRow();
  }

  public boolean isFirst() throws SQLException {
    raiseSQLExceptionIfResultSetIsClosed();
    return resultSetForNext.isFirst();
  }

  public boolean isClosed() throws SQLException {
    // no exception is raised.
    if (sfBaseResultSet != null) {
      return (resultSetForNext.isClosed() && sfBaseResultSet.isClosed());
    }
    return resultSetForNext.isClosed();
  }

  @Override
  public boolean isLast() throws SQLException {
    raiseSQLExceptionIfResultSetIsClosed();
    return resultSetForNext.isLast();
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    raiseSQLExceptionIfResultSetIsClosed();
    return resultSetForNext.isAfterLast();
  }

  @Override
  public boolean isBeforeFirst() throws SQLException {
    raiseSQLExceptionIfResultSetIsClosed();
    return resultSetForNext.isBeforeFirst();
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    logger.debug("public boolean isWrapperFor(Class<?> iface)");

    return iface.isInstance(this);
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

  /**
   * Get a list of ResultSetSerializables for the ResultSet in order to parallel processing
   *
   * @param maxSizeInBytes The expected max data size wrapped in the ResultSetSerializables object.
   *     NOTE: this parameter is intended to make the data size in each serializable object to be
   *     less than it. But if user specifies a small value which may be smaller than the data size
   *     of one result chunk. So the definition can't be guaranteed completely. For this special
   *     case, one serializable object is used to wrap the data chunk.
   * @return a list of ResultSetSerializables
   * @throws if fails to get the ResultSetSerializable objects.
   */
  @Override
  public List<SnowflakeResultSetSerializable> getResultSetSerializables(long maxSizeInBytes)
      throws SQLException {
    raiseSQLExceptionIfResultSetIsClosed();
    return sfBaseResultSet.getResultSetSerializables(maxSizeInBytes);
  }

  /** Empty result set */
  static class EmptyResultSet implements ResultSet {
    private boolean isClosed;

    EmptyResultSet() {
      isClosed = false;
    }

    private void raiseSQLExceptionIfResultSetIsClosed() throws SQLException {
      if (isClosed()) {
        throw new SnowflakeSQLException(ErrorCode.RESULTSET_ALREADY_CLOSED);
      }
    }

    @Override
    public boolean wasNull() throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return false;
    }

    @Override
    public boolean next() throws SQLException {
      return false; // no exception
    }

    @Override
    public void close() throws SQLException {
      isClosed = true;
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return false;
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return 0;
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return 0L;
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return (float) 0;
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return (double) 0;
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return (short) 0;
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return (byte) 0;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return "";
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return new byte[0];
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return null;
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return null;
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return null;
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return null;
    }

    @Deprecated
    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return null;
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return null;
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return null;
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return false;
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return 0;
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return 0;
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return 0;
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return 0;
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return 0;
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return 0;
    }

    @Deprecated
    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return null;
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return new byte[0];
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return null;
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return null;
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return null;
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return null;
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return null;
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return false;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return false;
    }

    @Override
    public boolean isFirst() throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return false;
    }

    @Override
    public boolean isLast() throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return false;
    }

    @Override
    public void beforeFirst() throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void afterLast() throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public boolean first() throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return false;
    }

    @Override
    public boolean last() throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return false;
    }

    @Override
    public int getRow() throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return 0;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return false;
    }

    @Override
    public boolean relative(int rows) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return false;
    }

    @Override
    public boolean previous() throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public int getFetchDirection() throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return 0;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public int getFetchSize() throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return 0;
    }

    @Override
    public int getType() throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return 0;
    }

    @Override
    public int getConcurrency() throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return 0;
    }

    @Override
    public boolean rowUpdated() throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return false;
    }

    @Override
    public boolean rowInserted() throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return false;
    }

    @Override
    public boolean rowDeleted() throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return false;
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length)
        throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length)
        throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length)
        throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void insertRow() throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateRow() throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void deleteRow() throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void refreshRow() throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void moveToInsertRow() throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public Statement getStatement() throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return null;
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return null;
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return null;
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return null;
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return null;
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return null;
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return null;
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return null;
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return null;
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return null;
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return null;
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return null;
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return null;
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return null;
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return null;
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return null;
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return null;
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return null;
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return null;
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return null;
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return null;
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public int getHoldability() throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return 0;
    }

    @Override
    public boolean isClosed() throws SQLException {
      return isClosed; // no exception
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return null;
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return null;
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return null;
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return null;
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return null;
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return null;
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return null;
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return null;
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length)
        throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length)
        throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length)
        throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length)
        throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length)
        throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length)
        throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length)
        throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return null;
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return null;
    }

    @Deprecated
    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return null;
    }

    @Override
    public InputStream getAsciiStream(String columnIndex) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return null;
    }

    @Deprecated
    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return null;
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return null;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
    }

    @Override
    public String getCursorName() throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return null;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return null;
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return null;
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return null;
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return 0;
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return null;
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return null;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
      raiseSQLExceptionIfResultSetIsClosed();
      return false;
    }
  }
}
