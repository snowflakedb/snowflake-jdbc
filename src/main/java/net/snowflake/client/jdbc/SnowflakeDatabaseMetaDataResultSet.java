/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.List;
import java.util.TimeZone;
import net.snowflake.client.core.SFBaseSession;
import net.snowflake.client.core.SFResultSetMetaData;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.common.core.SqlState;

class SnowflakeDatabaseMetaDataResultSet extends SnowflakeBaseResultSet {
  ResultSet showObjectResultSet;
  Object[] nextRow;
  private boolean wasNull = false;
  protected Object[][] rows;
  protected int row = -1;

  private String queryId;

  static final SFLogger logger =
      SFLoggerFactory.getLogger(SnowflakeDatabaseMetaDataResultSet.class);

  /**
   * DatabaseMetadataResultSet based on result from show command
   *
   * @param columnNames column names
   * @param columnTypeNames column type names
   * @param columnTypes column types
   * @param showObjectResultSet result set after issuing a show command
   * @param statement show command statement
   * @throws SQLException if failed to construct snowflake database metadata result set
   */
  SnowflakeDatabaseMetaDataResultSet(
      final List<String> columnNames,
      final List<String> columnTypeNames,
      final List<Integer> columnTypes,
      final ResultSet showObjectResultSet,
      final Statement statement)
      throws SQLException {
    super(statement);
    this.showObjectResultSet = showObjectResultSet;

    SFBaseSession session =
        statement.getConnection().unwrap(SnowflakeConnectionV1.class).getSFBaseSession();

    SFResultSetMetaData sfset =
        new SFResultSetMetaData(
            columnNames.size(), columnNames, columnTypeNames, columnTypes, session);

    this.resultSetMetaData = new SnowflakeResultSetMetaDataV1(sfset);

    this.nextRow = new Object[columnNames.size()];
  }

  /**
   * DatabaseMetadataResultSet based on a constant rowset.
   *
   * @param columnNames column name
   * @param columnTypeNames column types name
   * @param columnTypes column type
   * @param rows returned value of database metadata
   * @param statement show command statement
   * @throws SQLException if failed to construct snowflake database metadata result set
   */
  SnowflakeDatabaseMetaDataResultSet(
      final List<String> columnNames,
      final List<String> columnTypeNames,
      final List<Integer> columnTypes,
      final Object[][] rows,
      final Statement statement)
      throws SQLException {
    super(statement);
    this.rows = rows;

    SFBaseSession session =
        statement.getConnection().unwrap(SnowflakeConnectionV1.class).getSFBaseSession();

    SFResultSetMetaData sfset =
        new SFResultSetMetaData(
            columnNames.size(), columnNames, columnTypeNames, columnTypes, session);

    this.resultSetMetaData = new SnowflakeResultSetMetaDataV1(sfset);

    this.nextRow = new Object[columnNames.size()];
  }

  protected SnowflakeDatabaseMetaDataResultSet(
      DBMetadataResultSetMetadata metadataType, Object[][] rows, Statement statement)
      throws SQLException {
    this(
        metadataType.getColumnNames(),
        metadataType.getColumnTypeNames(),
        metadataType.getColumnTypes(),
        rows,
        statement);
  }

  protected SnowflakeDatabaseMetaDataResultSet(
      DBMetadataResultSetMetadata metadataType,
      Object[][] rows,
      Statement statement,
      String queryId)
      throws SQLException {
    this(
        metadataType.getColumnNames(),
        metadataType.getColumnTypeNames(),
        metadataType.getColumnTypes(),
        rows,
        statement);
    this.queryId = queryId;
  }

  @Override
  public boolean isClosed() throws SQLException {
    // no exception is raised.
    return statement.isClosed();
  }

  @Override
  public boolean next() throws SQLException {
    logger.debug("public boolean next()");
    incrementRow();

    // no exception is raised even after the result set is closed.
    if (row < rows.length) {
      nextRow = rows[row];
      return true;
    }

    return false;
  }

  /**
   * Increments result set row pointer. Mainly used to check the result set isBeforeFirst or
   * isFirst.
   */
  protected void incrementRow() {
    ++row;
  }

  @Override
  public void close() throws SQLException {
    // no exception
    try {
      getStatement().close(); // should close both result set and statement.
    } catch (SQLException ex) {
      logger.debug("failed to close", ex);
    }
  }

  @Override
  public boolean isFirst() throws SQLException {
    logger.debug("public boolean isFirst()");
    raiseSQLExceptionIfResultSetIsClosed();
    return row == 0;
  }

  @Override
  public boolean isBeforeFirst() throws SQLException {
    logger.debug("public boolean isBeforeFirst()");
    raiseSQLExceptionIfResultSetIsClosed();
    return row == -1;
  }

  @Override
  public boolean isLast() throws SQLException {
    logger.debug("public boolean isLast()");
    raiseSQLExceptionIfResultSetIsClosed();
    return !isBeforeFirst() && row == rows.length - 1;
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    logger.debug("public boolean isAfterLast()");
    raiseSQLExceptionIfResultSetIsClosed();
    return row == rows.length;
  }

  @Override
  public int getRow() throws SQLException {
    logger.debug("public int getRow()");
    raiseSQLExceptionIfResultSetIsClosed();
    return row;
  }

  @Override
  public byte[] getBytes(int columnIndex) throws SQLException {
    raiseSQLExceptionIfResultSetIsClosed();
    String str = this.getString(columnIndex);
    if (str != null) {
      return str.getBytes(StandardCharsets.UTF_8);
    } else {
      throw new SQLException("Cannot get bytes on null column");
    }
  }

  @Override
  public Time getTime(int columnIndex) throws SQLException {
    raiseSQLExceptionIfResultSetIsClosed();
    Object obj = getObjectInternal(columnIndex);

    if (obj instanceof Time) {
      return (Time) obj;
    } else {
      throw new SnowflakeSQLException(
          ErrorCode.INVALID_VALUE_CONVERT, obj.getClass().getName(), "TIME", obj);
    }
  }

  @Override
  public Timestamp getTimestamp(int columnIndex, TimeZone tz) throws SQLException {
    raiseSQLExceptionIfResultSetIsClosed();
    Object obj = getObjectInternal(columnIndex);

    if (obj instanceof Timestamp) {
      return (Timestamp) obj;
    } else {
      throw new SnowflakeSQLException(
          ErrorCode.INVALID_VALUE_CONVERT, obj.getClass().getName(), "TIMESTAMP", obj);
    }
  }

  @Override
  public Date getDate(int columnIndex, TimeZone tz) throws SQLException {
    raiseSQLExceptionIfResultSetIsClosed();
    Object obj = getObjectInternal(columnIndex);

    if (obj instanceof Date) {
      return (Date) obj;
    } else {
      throw new SnowflakeSQLException(
          ErrorCode.INVALID_VALUE_CONVERT, obj.getClass().getName(), "DATE", obj);
    }
  }

  static ResultSet getEmptyResult(
      DBMetadataResultSetMetadata metadataType, Statement statement, String queryId)
      throws SQLException {
    return new SnowflakeDatabaseMetaDataResultSet(
        metadataType, new Object[][] {}, statement, queryId);
  }

  static ResultSet getEmptyResultSet(DBMetadataResultSetMetadata metadataType, Statement statement)
      throws SQLException {
    return new SnowflakeDatabaseMetaDataResultSet(metadataType, new Object[][] {}, statement);
  }

  Object getObjectInternal(int columnIndex) throws SQLException {
    logger.debug("public Object getObjectInternal(int columnIndex)");
    raiseSQLExceptionIfResultSetIsClosed();

    if (nextRow == null) {
      throw new SQLException("No row found.");
    }

    if (columnIndex > nextRow.length) {
      throw new SQLException("Invalid column index: " + columnIndex);
    }

    wasNull = nextRow[columnIndex - 1] == null;

    logger.debug("Returning column: " + columnIndex + ": " + nextRow[columnIndex - 1]);

    return nextRow[columnIndex - 1];
  }

  @Override
  public boolean wasNull() throws SQLException {
    logger.debug("public boolean wasNull() returning {}", wasNull);
    raiseSQLExceptionIfResultSetIsClosed();
    return wasNull;
  }

  @Override
  public String getString(int columnIndex) throws SQLException {
    logger.debug("public String getString(int columnIndex)");

    // Column index starts from 1, not 0.
    Object obj = getObjectInternal(columnIndex);

    return obj == null ? null : obj.toString();
  }

  @Override
  public boolean getBoolean(int columnIndex) throws SQLException {
    logger.debug("public boolean getBoolean(int columnIndex)");

    // Column index starts from 1, not 0.
    Object obj = getObjectInternal(columnIndex);

    if (obj == null) {
      return false;
    }

    if (obj instanceof String) {
      if (obj.toString().equals("1")) {
        return Boolean.TRUE;
      }
      return Boolean.FALSE;
    } else if (obj instanceof Integer) {
      int i = (Integer) obj;
      if (i > 0) {
        return Boolean.TRUE;
      }
      return Boolean.FALSE;
    } else {
      return ((Boolean) obj).booleanValue();
    }
  }

  @Override
  public byte getByte(int columnIndex) throws SQLException {
    logger.debug("public byte getByte(int columnIndex)");
    // Column index starts from 1, not 0.
    Object obj = getObjectInternal(columnIndex);

    if (obj == null) {
      return 0;
    }

    if (obj instanceof String) {
      return Byte.valueOf((String) obj);
    } else {
      return (Byte) obj;
    }
  }

  @Override
  public short getShort(int columnIndex) throws SQLException {
    logger.debug("public short getShort(int columnIndex)");

    // Column index starts from 1, not 0.
    Object obj = getObjectInternal(columnIndex);

    if (obj == null) {
      return 0;
    }

    if (obj instanceof String) {
      return (Short.valueOf((String) obj)).shortValue();
    } else {
      return ((Number) obj).shortValue();
    }
  }

  @Override
  public int getInt(int columnIndex) throws SQLException {
    logger.debug("public int getInt(int columnIndex)");

    // Column index starts from 1, not 0.
    Object obj = getObjectInternal(columnIndex);

    if (obj == null) {
      return 0;
    }

    if (obj instanceof String) {
      return (Integer.valueOf((String) obj)).intValue();
    } else {
      return ((Number) obj).intValue();
    }
  }

  @Override
  public long getLong(int columnIndex) throws SQLException {
    logger.debug("public long getLong(int columnIndex)");

    // Column index starts from 1, not 0.
    Object obj = getObjectInternal(columnIndex);

    if (obj == null) {
      return 0;
    }

    try {
      if (obj instanceof String) {
        return (Long.valueOf((String) obj)).longValue();
      } else {
        return ((Number) obj).longValue();
      }
    } catch (NumberFormatException nfe) {
      throw new SnowflakeSQLException(
          SqlState.INTERNAL_ERROR,
          ErrorCode.INTERNAL_ERROR.getMessageCode(),
          "Invalid long: " + (String) obj);
    }
  }

  @Override
  public float getFloat(int columnIndex) throws SQLException {
    logger.debug("public float getFloat(int columnIndex)");

    // Column index starts from 1, not 0.
    Object obj = getObjectInternal(columnIndex);

    if (obj == null) {
      return 0;
    }

    if (obj instanceof String) {
      return (Float.valueOf((String) obj)).floatValue();
    } else {
      return ((Number) obj).floatValue();
    }
  }

  @Override
  public double getDouble(int columnIndex) throws SQLException {
    logger.debug("public double getDouble(int columnIndex)");

    // Column index starts from 1, not 0.
    Object obj = getObjectInternal(columnIndex);

    // snow-11974: null for getDouble should return 0
    if (obj == null) {
      return 0;
    }

    if (obj instanceof String) {
      return (Double.valueOf((String) obj)).doubleValue();
    } else {
      return ((Number) obj).doubleValue();
    }
  }

  public String getQueryID() {
    return queryId;
  }

  /** @deprecated */
  @Deprecated
  @Override
  public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
    logger.debug("public BigDecimal getBigDecimal(int columnIndex, int scale)");

    BigDecimal value;

    // Column index starts from 1, not 0.
    Object obj = getObjectInternal(columnIndex);

    if (obj == null) {
      return null;
    }

    if (obj instanceof String) {
      value = new BigDecimal((String) obj);
    } else {
      value = new BigDecimal(obj.toString());
    }

    value = value.setScale(scale, RoundingMode.HALF_UP);

    return value;
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    logger.debug("public BigDecimal getBigDecimal(int columnIndex)");

    BigDecimal value = null;

    // Column index starts from 1, not 0.
    Object obj = getObjectInternal(columnIndex);

    if (obj == null) {
      return null;
    }

    if (obj instanceof String) {
      value = new BigDecimal((String) obj);
    } else {
      value = new BigDecimal(obj.toString());
    }

    return value;
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    logger.debug("public Object getObject(int columnIndex)");

    int type = resultSetMetaData.getColumnType(columnIndex);

    Object internalObj = getObjectInternal(columnIndex);
    if (internalObj == null) {
      return null;
    }

    switch (type) {
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
        throw new SnowflakeLoggedFeatureNotSupportedException(session);
    }
  }
}
