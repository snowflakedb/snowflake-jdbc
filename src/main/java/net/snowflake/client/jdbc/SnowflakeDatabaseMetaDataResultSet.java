/*
 * Copyright (c) 2012-2018 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import net.snowflake.client.core.SFSession;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.TimeZone;

class SnowflakeDatabaseMetaDataResultSet extends SnowflakeBaseResultSet
{
  ResultSet showObjectResultSet;
  protected Statement statement;
  protected Object[][] rows;

  static final SFLogger logger = SFLoggerFactory.getLogger(
      SnowflakeDatabaseMetaDataResultSet.class);

  /**
   * DatabaseMetadataResultSet based on result from show command
   *
   * @param columnNames         column names
   * @param columnTypeNames     column type names
   * @param columnTypes         column types
   * @param showObjectResultSet result set after issuing a show command
   * @param statement           show command statement
   * @throws SQLException if failed to construct snowflake database metadata result set
   */
  private SnowflakeDatabaseMetaDataResultSet(
      final List<String> columnNames,
      final List<String> columnTypeNames,
      final List<Integer> columnTypes,
      final ResultSet showObjectResultSet,
      final Statement statement)
      throws SQLException
  {
    this.showObjectResultSet = showObjectResultSet;

    SFSession session = ((SnowflakeConnectionV1) statement.getConnection()).getSfSession();

    this.resultSetMetaData = new SnowflakeResultSetMetaData(columnNames.size(),
        columnNames,
        columnTypeNames,
        columnTypes,
        session);

    this.nextRow = new Object[columnNames.size()];

    this.statement = statement;
  }

  /**
   * DatabaseMetadataResultSet based on a constant rowset.
   *
   * @param columnNames     column name
   * @param columnTypeNames column types name
   * @param columnTypes     column type
   * @param rows            returned value of database metadata
   * @param statement       show command statement
   * @throws SQLException if failed to construct snowflake database metadata result set
   */
  SnowflakeDatabaseMetaDataResultSet(
      final List<String> columnNames,
      final List<String> columnTypeNames,
      final List<Integer> columnTypes,
      final Object[][] rows,
      final Statement statement)
      throws SQLException
  {
    this.rows = rows;

    SFSession session = ((SnowflakeConnectionV1) statement.getConnection()).getSfSession();

    this.resultSetMetaData = new SnowflakeResultSetMetaData(columnNames.size(),
        columnNames,
        columnTypeNames,
        columnTypes,
        session);

    this.nextRow = new Object[columnNames.size()];

    this.statement = statement;
  }

  SnowflakeDatabaseMetaDataResultSet(
      DBMetadataResultSetMetadata metadataType,
      ResultSet resultSet,
      Statement statement) throws SQLException
  {
    this(metadataType.getColumnNames(), metadataType.getColumnTypeNames(),
        metadataType.getColumnTypes(), resultSet, statement);
  }

  private SnowflakeDatabaseMetaDataResultSet(
      DBMetadataResultSetMetadata metadataType,
      Object[][] rows,
      Statement statement) throws SQLException
  {
    this(metadataType.getColumnNames(), metadataType.getColumnTypeNames(),
        metadataType.getColumnTypes(), rows, statement);
  }

  @Override
  public boolean next() throws SQLException
  {
    logger.debug("public boolean next()");

    if (row < rows.length)
    {
      nextRow = rows[row++];
      return true;
    }

    if (statement != null)
    {
      statement.close();
      statement = null;
    }

    return false;
  }

  @Override
  public void close() throws SQLException
  {
    if (statement != null)
    {
      statement.close();
      statement = null;
    }
  }

  @Override
  public byte[] getBytes(int columnIndex) throws SQLException
  {
    String str = this.getString(columnIndex);
    if (str != null)
    {
      return str.getBytes(StandardCharsets.UTF_8);
    }
    else
    {
      throw new SQLException("Cannot get bytes on null column");
    }
  }

  @Override
  public Time getTime(int columnIndex) throws SQLException
  {
    Object obj = getObjectInternal(columnIndex);

    if (obj instanceof Time)
    {
      return (Time) obj;
    }
    else
    {
      throw new SnowflakeSQLException(ErrorCode.INVALID_VALUE_CONVERT,
          obj.getClass().getName(), "TIME", obj);
    }
  }

  @Override
  public Timestamp getTimestamp(int columnIndex, TimeZone tz) throws SQLException
  {
    Object obj = getObjectInternal(columnIndex);

    if (obj instanceof Timestamp)
    {
      return (Timestamp) obj;
    }
    else
    {
      throw new SnowflakeSQLException(ErrorCode.INVALID_VALUE_CONVERT,
          obj.getClass().getName(), "TIMESTAMP", obj);
    }
  }

  @Override
  public Date getDate(int columnIndex, TimeZone tz) throws SQLException
  {
    Object obj = getObjectInternal(columnIndex);

    if (obj instanceof Date)
    {
      return (Date) obj;
    }
    else
    {
      throw new SnowflakeSQLException(ErrorCode.INVALID_VALUE_CONVERT,
          obj.getClass().getName(), "DATE", obj);
    }
  }

  static ResultSet getEmptyResultSet(DBMetadataResultSetMetadata metadataType, Statement statement)
      throws SQLException
  {
    return new SnowflakeDatabaseMetaDataResultSet(metadataType, new Object[][]{}, statement);
  }
}
