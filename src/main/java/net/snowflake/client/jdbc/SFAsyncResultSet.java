/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import java.math.BigDecimal;
import java.sql.*;
import java.util.List;
import java.util.TimeZone;
import java.util.regex.Pattern;
import net.snowflake.client.core.*;
import net.snowflake.common.core.SqlState;

/** SFAsyncResultSet implementation */
class SFAsyncResultSet extends SnowflakeBaseResultSet implements SnowflakeResultSet, ResultSet {
  private final SFBaseResultSet sfBaseResultSet;
  private ResultSet resultSetForNext = new SnowflakeResultSetV1.EmptyResultSet();
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
    this.session = (SFSession) sfBaseResultSet.getSession();
    this.extraStatement = statement;
    try {
      this.resultSetMetaData = new SnowflakeResultSetMetaDataV1(sfBaseResultSet.getMetaData());
      this.resultSetMetaData.setQueryIdForAsyncResults(this.queryID);
      this.resultSetMetaData.setQueryType(SnowflakeResultSetMetaDataV1.QueryType.ASYNC);
    } catch (SFException ex) {
      throw new SnowflakeSQLLoggedException(
          this.session, ex.getSqlState(), ex.getVendorCode(), ex.getCause(), ex.getParams());
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
      throw new SnowflakeSQLLoggedException(
          this.session, ex.getSqlState(), ex.getVendorCode(), ex.getCause(), ex.getParams());
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
    getMetaData();
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
    // if ResultSet is not initialized yet, this means neither next() nor getMetaData() has been
    // called.
    // If next() hasn't been called, we are at the beginning of the ResultSet so should return true.
    return !resultSetForNextInitialized || resultSetForNext.isBeforeFirst();
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
   * <p>Not currently supported for asynchronous result sets.
   */
  @Override
  public List<SnowflakeResultSetSerializable> getResultSetSerializables(long maxSizeInBytes)
      throws SQLException {
    raiseSQLExceptionIfResultSetIsClosed();
    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }
}
