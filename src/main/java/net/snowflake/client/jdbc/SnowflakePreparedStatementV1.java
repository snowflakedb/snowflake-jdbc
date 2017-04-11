/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package net.snowflake.client.jdbc;

import net.snowflake.client.core.SFStatementType;
import net.snowflake.client.core.ResultUtil;
import net.snowflake.common.core.SFBinary;
import net.snowflake.common.core.SqlState;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

/**
 *
 * @author jhuang
 */
public class SnowflakePreparedStatementV1 implements PreparedStatement
{
  static final SFLogger logger = SFLoggerFactory.getLogger(
      SnowflakePreparedStatementV1.class);

  private SnowflakeConnectionV1 connection;
  private String sql;
  private SnowflakeStatementV1 statement;

  // result metadata from describe phase
  private ResultSetMetaData resultMetadata;

  /**
   * map of bind values for single query execution
   *
   * bind variable name ->
   *                      value -> bind variable value
   *                      type -> bind variable type
   */
  Map<String, Map<String, Object>> parameterBindings =
      new HashMap<String, Map<String, Object>>();

  /**
   * map of bind values for batch query executions
   *
   * bind variable name ->
   *                      value -> list of bind variable values
   *                      type -> bind variable type
   */
  Map<String, Map<String, Object>> batchParameterBindings =
      new HashMap<String, Map<String, Object>>();

  private int batchSize = 0;

  public SnowflakePreparedStatementV1(SnowflakeConnectionV1 conn,
                                      String sql) throws SQLException
  {
    logger.debug(
        "SnowflakePreparedStatement(SnowflakeConnectionV1 conn,\n" +
        "String sql) throws SQLException");

    this.connection = conn;
    this.sql = sql;
    this.statement = new SnowflakeStatementV1(conn);
  }

  @Override
  public ResultSet executeQuery() throws SQLException
  {
    logger.debug("executeQuery() throws SQLException");

    ResultSet resultSet = statement.executeQueryInternal(this.sql,
        parameterBindings);

    resultMetadata = resultSet.getMetaData();

    return resultSet;
  }

  @Override
  public int executeUpdate() throws SQLException
  {
    logger.debug( "executeUpdate() throws SQLException");

    return statement.executeUpdateInternal(this.sql, parameterBindings);
  }

  @Override
  public void setNull(int parameterIndex, int sqlType) throws SQLException
  {
    logger.debug(
        "setNull(int parameterIndex, int sqlType) throws SQLException");

    Map<String, Object> binding = new HashMap<String, Object>();
    binding.put("value", null);
    binding.put("type", SnowflakeUtil.javaTypeToSFTypeString(sqlType));
    parameterBindings.put(String.valueOf(parameterIndex), binding);
  }

  @Override
  public void setBoolean(int parameterIndex, boolean x) throws SQLException
  {
    logger.debug(
               "setBoolean(int parameterIndex, boolean x) throws SQLException");
    Map<String, Object> binding = new HashMap<>();
    binding.put("value", String.valueOf(x));
    binding.put("type", SnowflakeUtil.javaTypeToSFTypeString(Types.BOOLEAN));
    parameterBindings.put(String.valueOf(parameterIndex), binding);
  }

  @Override
  public void setByte(int parameterIndex, byte x) throws SQLException
  {
    throw new UnsupportedOperationException(
        "setByte(int parameterIndex, byte x) Not supported yet.");
  }

  @Override
  public void setShort(int parameterIndex, short x) throws SQLException
  {
    logger.debug(
        "setShort(int parameterIndex, short x) throws SQLException");

    Map<String, Object> binding = new HashMap<String, Object>();
    binding.put("value", String.valueOf(x));
    binding.put("type", SnowflakeUtil.javaTypeToSFTypeString(Types.SMALLINT));
    parameterBindings.put(String.valueOf(parameterIndex), binding);
  }

  @Override
  public void setInt(int parameterIndex, int x) throws SQLException
  {
    logger.debug(
        "setInt(int parameterIndex, int x) throws SQLException");

    Map<String, Object> binding = new HashMap<String, Object>();
    binding.put("value", String.valueOf(x));
    binding.put("type", SnowflakeUtil.javaTypeToSFTypeString(Types.INTEGER));
    parameterBindings.put(String.valueOf(parameterIndex), binding);
  }

  @Override
  public void setLong(int parameterIndex, long x) throws SQLException
  {
    logger.debug(
        "setLong(int parameterIndex, long x) throws SQLException");

    Map<String, Object> binding = new HashMap<String, Object>();
    binding.put("value", String.valueOf(x));
    binding.put("type", SnowflakeUtil.javaTypeToSFTypeString(Types.BIGINT));
    parameterBindings.put(String.valueOf(parameterIndex), binding);
  }

  @Override
  public void setFloat(int parameterIndex, float x) throws SQLException
  {
    logger.debug(
        "setFloat(int parameterIndex, float x) throws SQLException");

    Map<String, Object> binding = new HashMap<String, Object>();
    binding.put("value", String.valueOf(x));
    binding.put("type", SnowflakeUtil.javaTypeToSFTypeString(Types.FLOAT));
    parameterBindings.put(String.valueOf(parameterIndex), binding);
  }

  @Override
  public void setDouble(int parameterIndex, double x) throws SQLException
  {
    logger.debug(
        "setDouble(int parameterIndex, double x) throws SQLException");

    Map<String, Object> binding = new HashMap<String, Object>();
    binding.put("value", String.valueOf(x));
    binding.put("type", SnowflakeUtil.javaTypeToSFTypeString(Types.DOUBLE));
    parameterBindings.put(String.valueOf(parameterIndex), binding);
  }

  @Override
  public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException
  {
    logger.debug(
        "setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException");

    if (x == null)
    {
      setNull(parameterIndex, Types.DECIMAL);
    }
    else
    {
      Map<String, Object> binding = new HashMap<String, Object>();
      binding.put("value", String.valueOf(x));
      binding.put("type", SnowflakeUtil.javaTypeToSFTypeString(Types.DECIMAL));
      parameterBindings.put(String.valueOf(parameterIndex), binding);
    }
  }

  @Override
  public void setString(int parameterIndex, String x) throws SQLException
  {
    logger.debug(
        "setString(int parameterIndex, String x) throws SQLException");

    Map<String, Object> binding = new HashMap<String, Object>();
    binding.put("value", x);
    binding.put("type", SnowflakeUtil.javaTypeToSFTypeString(Types.VARCHAR));
    parameterBindings.put(String.valueOf(parameterIndex), binding);
  }

  @Override
  public void setBytes(int parameterIndex, byte[] x) throws SQLException
  {
    logger.debug(
        "setBytes(int parameterIndex, byte[] x) throws SQLException");

    Map<String, Object> binding = new HashMap<String, Object>();
    binding.put("value", new SFBinary(x).toHex());
    binding.put("type", SnowflakeUtil.javaTypeToSFTypeString(Types.BINARY));
    parameterBindings.put(String.valueOf(parameterIndex), binding);
  }

  @Override
  public void setDate(int parameterIndex, Date x) throws SQLException
  {
    logger.debug(
        "setDate(int parameterIndex, Date x) throws SQLException");

    if (x == null)
    {
      setNull(parameterIndex, Types.DATE);
    }
    else
    {
      Map<String, Object> binding = new HashMap<String, Object>();

      // convert the date from being in local time zone to be in UTC timezone
      binding.put("value", String.valueOf(x.getTime() +
          TimeZone.getDefault().getOffset(x.getTime())));
      binding.put("type", SnowflakeUtil.javaTypeToSFTypeString(Types.DATE));
      parameterBindings.put(String.valueOf(parameterIndex), binding);
    }
  }

  @Override
  public void setTime(int parameterIndex, Time x) throws SQLException
  {
    logger.debug(
        "setTime(int parameterIndex, Time x) throws SQLException");

    if (x == null)
    {
      setNull(parameterIndex, Types.TIME);
    }
    else
    {
      Map<String, Object> binding = new HashMap<String, Object>();

      // Convert to nanoseconds since midnight using the input time mod 24 hours.
      final long MS_IN_DAY = 86400 * 1000;
      long msSinceEpoch = x.getTime();
      // Use % + % instead of just % to get the nonnegative remainder.
      // TODO(mkember): Change to use Math.floorMod when Client is on Java 8.
      long msSinceMidnight = (msSinceEpoch % MS_IN_DAY + MS_IN_DAY) % MS_IN_DAY;
      long nanosSinceMidnight = msSinceMidnight * 1000 * 1000;
      binding.put("value", String.valueOf(nanosSinceMidnight));
      binding.put("type", SnowflakeUtil.javaTypeToSFTypeString(Types.TIME));
      parameterBindings.put(String.valueOf(parameterIndex), binding);
    }
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException
  {
    logger.debug(
        "setTimestamp(int parameterIndex, Timestamp x) throws SQLException");

    Map<String, Object> binding = new HashMap<String, Object>();

    // convert the timestamp from being in local time zone to be in UTC timezone
    binding.put("value", x == null ? null :
        String.valueOf(
        BigDecimal.valueOf(x.getTime()/1000).
        scaleByPowerOfTen(9).add(BigDecimal.valueOf(x.getNanos()))));

    SnowflakeType sfType = SnowflakeUtil.javaTypeToSFType(Types.TIMESTAMP);

    if (sfType == SnowflakeType.TIMESTAMP)
    {
      sfType = connection.getSfSession().getTimestampMappedType();
    }

    binding.put("type", sfType.name());
    parameterBindings.put(String.valueOf(parameterIndex), binding);
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, int length)
          throws SQLException
  {
    throw new UnsupportedOperationException(
        "setAsciiStream(int parameterIndex, InputStream x, int length) Not supported yet.");
  }

  @Override
  @Deprecated
  public void setUnicodeStream(int parameterIndex, InputStream x, int length)
          throws SQLException
  {
    throw new UnsupportedOperationException(
        "setUnicodeStream(int parameterIndex, InputStream x, int length) Not supported yet.");
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, int length)
          throws SQLException
  {
    throw new UnsupportedOperationException(
        "setBinaryStream(int parameterIndex, InputStream x, int length) Not supported yet.");
  }

  @Override
  public void clearParameters() throws SQLException
  {
    parameterBindings.clear();
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException
  {
    logger.debug(
        "setObject(int parameterIndex, Object x, int targetSqlType)");

    if (x == null)
      setNull(parameterIndex, targetSqlType);
    else if (targetSqlType == Types.DATE)
      setDate(parameterIndex, (Date)x);
    else if (targetSqlType == Types.TIME)
      setTime(parameterIndex, (Time)x);
    else if (targetSqlType == Types.TIMESTAMP)
      setTimestamp(parameterIndex, (Timestamp)x);
    else
    {
      Map<String, Object> binding = new HashMap<String, Object>();
      binding.put("value", String.valueOf(x));
      binding.put("type", SnowflakeUtil.javaTypeToSFTypeString(targetSqlType));
      parameterBindings.put(String.valueOf(parameterIndex), binding);
    }
  }

  @Override
  public void setObject(int parameterIndex, Object x) throws SQLException
  {
    if (x == null)
      setNull(parameterIndex, Types.NULL);
    else if (x instanceof String)
      setString(parameterIndex, (String) x);
    else if (x instanceof BigDecimal)
      setBigDecimal(parameterIndex, (BigDecimal) x);
    else if (x instanceof Short)
      setShort(parameterIndex, ((Short) x).shortValue());
    else if (x instanceof Integer)
      setInt(parameterIndex, ((Integer) x).intValue());
    else if (x instanceof Long)
      setLong(parameterIndex, ((Long) x).longValue());
    else if (x instanceof Float)
      setFloat(parameterIndex, ((Float) x).floatValue());
    else if (x instanceof Double)
      setDouble(parameterIndex, ((Double) x).doubleValue());
    else if (x instanceof Date)
      setDate(parameterIndex, (Date)x);
    else if (x instanceof Time)
      setTime(parameterIndex, (Time)x);
    else if (x instanceof Timestamp)
      setTimestamp(parameterIndex, (Timestamp)x);
    else
    {
      throw new SnowflakeSQLException(SqlState.FEATURE_NOT_SUPPORTED,
          ErrorCode.DATA_TYPE_NOT_SUPPORTED
              .getMessageCode(),
          "Object type: " + x.getClass());
    }
  }

  @Override
  public boolean execute() throws SQLException
  {
    logger.debug( "execute: {}", sql);

    String trimmedSql = sql.trim();

    // snowflake specific client side commands
    if (statement.isFileTransfer(trimmedSql))
    {
      // PUT/GET command
      logger.debug( "Executing file transfer locally: {}", sql);

      ResultSet resultSet = statement.executeQuery(this.sql);
      resultMetadata = resultSet.getMetaData();

      return true;
    }
    else if (trimmedSql.length() >= 20
        && trimmedSql.toLowerCase().startsWith(
        "set-sf-property"))
    {
      statement.executeSetProperty(sql);
      return false;
    }
    else
    {
      SnowflakeResultSetV1 resultSet = (SnowflakeResultSetV1)statement.executeQueryInternal(this.sql,
          parameterBindings);

      if (connection.getSfSession().isExecuteReturnCountForDML())
      {
        if (SFStatementType.isDDL(resultSet.getStatementTypeId())
                || SFStatementType.isDML(resultSet.getStatementTypeId()))
        {
          statement.setUpdateCount(ResultUtil.calculateUpdateCount(resultSet, resultSet.getStatementTypeId()));
          return false;
        }
      }

      resultMetadata = resultSet.getMetaData();

      return true;
    }
  }

  @Override
  public void addBatch() throws SQLException
  {
    logger.debug( "addBatch() throws SQLException");

    // move bind variables from single execution binding to batch execution
    // binding
    for(Map.Entry<String, Map<String, Object>> binding :
        parameterBindings.entrySet())
    {
      // get the entry for the bind variable in the batch binding map
      Map<String, Object> bindingValueAndType =
          batchParameterBindings.get(binding.getKey());

      List<String> values;

      // create binding value and type for the first time
      if (bindingValueAndType == null)
      {
        // create the map for value and type
        bindingValueAndType = new HashMap<String, Object>();

        // create the value list
        values = new ArrayList<String>();

        bindingValueAndType.put("value", values);
        bindingValueAndType.put("type", (String)binding.getValue().get("type"));

        // put the new map into the batch
        batchParameterBindings.put(binding.getKey(),
            bindingValueAndType);
      }
      else
      {
        // make sure type matches except for null values
        String prevType = (String) bindingValueAndType.get("type");
        String newType = (String)binding.getValue().get("type");

        // if previous type is null, replace it with new type
        if (SnowflakeType.ANY.name().equalsIgnoreCase(prevType) &&
            !SnowflakeType.ANY.name().equalsIgnoreCase(newType))
          bindingValueAndType.put("type", newType);
        else if (binding.getValue().get("value") != null &&
            !prevType.equalsIgnoreCase(newType))
        {
          throw new SnowflakeSQLException(SqlState.FEATURE_NOT_SUPPORTED,
              ErrorCode.ARRAY_BIND_MIXED_TYPES_NOT_SUPPORTED.getMessageCode(),
              SnowflakeType.getJavaType(SnowflakeType.fromString(prevType)).name(),
              SnowflakeType.getJavaType(SnowflakeType.fromString(newType)).name());
        }

        // found the existing map so just get the value list
        values = (List<String>) bindingValueAndType.get("value");
      }

      // add the value to the list of values in batch binding map
      values.add((String)binding.getValue().get("value"));
    }
    batchSize++;
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, int length)
          throws SQLException
  {
    throw new UnsupportedOperationException(
        "setCharacterStream(int parameterIndex, Reader reader, int length) Not supported yet.");
  }

  @Override
  public void setRef(int parameterIndex, Ref x) throws SQLException
  {
    throw new UnsupportedOperationException(
        "setRef(int parameterIndex, Ref x) Not supported yet.");
  }

  @Override
  public void setBlob(int parameterIndex, Blob x) throws SQLException
  {
    throw new UnsupportedOperationException(
        "setBlob(int parameterIndex, Blob x) Not supported yet.");
  }

  @Override
  public void setClob(int parameterIndex, Clob x) throws SQLException
  {
    throw new UnsupportedOperationException(
        "setClob(int parameterIndex, Clob x) Not supported yet.");
  }

  @Override
  public void setArray(int parameterIndex, Array x) throws SQLException
  {
    throw new UnsupportedOperationException(
        "setArray(int parameterIndex, Array x) Not supported yet.");
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException
  {
    logger.debug( "getMetaData() throws SQLException");

    if (resultMetadata == null)
    {
      resultMetadata = statement.describeQueryInternal(this.sql,
          parameterBindings);
    }

    return resultMetadata;
  }

  @Override
  public void setDate(int parameterIndex, Date x, Calendar cal)
      throws SQLException
  {
    logger.debug( "setDate(int parameterIndex, Date x, Calendar cal)");

    if (x == null)
    {
      setNull(parameterIndex, Types.DATE);
    }
    else
    {
      Map<String, Object> binding = new HashMap<String, Object>();

      // convert the date from to be in local time zone to be in UTC
      binding.put("value", String.valueOf(x.getTime() +
          cal.getTimeZone().getOffset(x.getTime())));

      binding.put("type", SnowflakeUtil.javaTypeToSFTypeString(Types.DATE));
      parameterBindings.put(String.valueOf(parameterIndex), binding);
    }
  }

  @Override
  public void setTime(int parameterIndex, Time x, Calendar cal)
      throws SQLException
  {
    logger.debug( "setTime(int parameterIndex, Time x, Calendar cal)");
    setTime(parameterIndex, x);
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal)
      throws SQLException
  {
    logger.debug( "setTimestamp(int parameterIndex, Timestamp x, Calendar cal)");

    Map<String, Object> binding = new HashMap<String, Object>();

    // convert the time from being in UTC to be in local time zone
    if (x != null)
    {
      long milliSecSinceEpoch = x.getTime();
      milliSecSinceEpoch = milliSecSinceEpoch +
          cal.getTimeZone().getOffset(milliSecSinceEpoch);

      binding.put("value", String.valueOf(
          BigDecimal.valueOf(milliSecSinceEpoch / 1000).
              scaleByPowerOfTen(9).add(BigDecimal.valueOf(x.getNanos()))));
    }
    else
    {
      binding.put("value", null);
    }

    SnowflakeType sfType = SnowflakeUtil.javaTypeToSFType(Types.TIMESTAMP);

    if (sfType == SnowflakeType.TIMESTAMP)
    {
      sfType = connection.getSfSession().getTimestampMappedType();
    }

    binding.put("type", sfType.name());
    parameterBindings.put(String.valueOf(parameterIndex), binding);
  }

  @Override
  public void setNull(int parameterIndex, int sqlType, String typeName)
      throws SQLException
  {
    logger.debug( "setNull(int parameterIndex, int sqlType, String typeName)");

    setNull(parameterIndex, sqlType);
  }

  @Override
  public void setURL(int parameterIndex, URL x) throws SQLException
  {
    throw new UnsupportedOperationException(
        "setURL(int parameterIndex, URL x) Not supported yet.");
  }

  @Override
  public ParameterMetaData getParameterMetaData() throws SQLException
  {
    throw new UnsupportedOperationException(
        "getParameterMetaData() Not supported yet.");
  }

  @Override
  public void setRowId(int parameterIndex, RowId x) throws SQLException
  {
    throw new UnsupportedOperationException(
        "setRowId(int parameterIndex, RowId x) Not supported yet.");
  }

  @Override
  public void setNString(int parameterIndex, String value) throws SQLException
  {
    throw new UnsupportedOperationException(
        "setNString(int parameterIndex, String value) Not supported yet.");
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value, long length)
          throws SQLException
  {
    throw new UnsupportedOperationException(
        "setNCharacterStream(int parameterIndex, Reader value, long length) Not supported yet.");
  }

  @Override
  public void setNClob(int parameterIndex, NClob value) throws SQLException
  {
    throw new UnsupportedOperationException(
        "setNClob(int parameterIndex, NClob value) Not supported yet.");
  }

  @Override
  public void setClob(int parameterIndex, Reader reader, long length)
      throws SQLException
  {
    throw new UnsupportedOperationException(
        "setClob(int parameterIndex, Reader reader, long length) Not supported yet.");
  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream, long length)
          throws SQLException
  {
    throw new UnsupportedOperationException(
        "setBlob(int parameterIndex, InputStream inputStream, long length) Not supported yet.");
  }

  @Override
  public void setNClob(int parameterIndex, Reader reader, long length)
      throws SQLException
  {
    throw new UnsupportedOperationException(
        "setNClob(int parameterIndex, Reader reader, long length) Not supported yet.");
  }

  @Override
  public void setSQLXML(int parameterIndex, SQLXML xmlObject)
      throws SQLException
  {
    throw new UnsupportedOperationException(
        "setSQLXML(int parameterIndex, SQLXML xmlObject) Not supported yet.");
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType,
                        int scaleOrLength) throws SQLException
  {
    logger.debug(
        "setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength)");

    if (x == null)
    {
      setNull(parameterIndex, targetSqlType);
    }
    else if (targetSqlType == Types.DECIMAL ||
        targetSqlType == Types.NUMERIC)
    {
      BigDecimal decimalObj = new BigDecimal(String.valueOf(x));
      decimalObj.setScale(scaleOrLength);
      setBigDecimal(parameterIndex, decimalObj);
    }
    else
    {
      setObject(parameterIndex, x, targetSqlType);
    }
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, long length)
          throws SQLException
  {
    throw new UnsupportedOperationException(
        "setAsciiStream(int parameterIndex, InputStream x, long length) Not supported yet.");
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, long length)
          throws SQLException
  {
    throw new UnsupportedOperationException(
        "setBinaryStream(int parameterIndex, InputStream x, long length) Not supported yet.");
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, long length)
          throws SQLException
  {
    throw new UnsupportedOperationException(
        "setCharacterStream(int parameterIndex, Reader reader, long length) Not supported yet.");
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x)
      throws SQLException
  {
    throw new UnsupportedOperationException(
        "setAsciiStream(int parameterIndex, InputStream x) Not supported yet.");
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x)
      throws SQLException
  {
    throw new UnsupportedOperationException(
        "setBinaryStream(int parameterIndex, InputStream x) Not supported yet.");
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader)
      throws SQLException
  {
    throw new UnsupportedOperationException(
        "setCharacterStream(int parameterIndex, Reader reader) Not supported yet.");
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value)
      throws SQLException
  {
    throw new UnsupportedOperationException(
        "setNCharacterStream(int parameterIndex, Reader value) Not supported yet.");
  }

  @Override
  public void setClob(int parameterIndex, Reader reader) throws SQLException
  {
    throw new UnsupportedOperationException(
        "setClob(int parameterIndex, Reader reader) Not supported yet.");
  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream)
      throws SQLException
  {
    throw new UnsupportedOperationException(
        "setBlob(int parameterIndex, InputStream inputStream) Not supported yet.");
  }

  @Override
  public void setNClob(int parameterIndex, Reader reader) throws SQLException
  {
    throw new UnsupportedOperationException(
        "setNClob(int parameterIndex, Reader reader) Not supported yet.");
  }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException
  {
    logger.debug( "executeQuery(String sql) throws SQLException");

    return statement.executeQuery(sql);
  }

  @Override
  public int executeUpdate(String sql) throws SQLException
  {
    logger.debug( "executeUpdate(String sql) throws SQLException");

    return statement.executeUpdate(sql);
  }

  @Override
  public void close() throws SQLException
  {
    logger.debug( "close() throws SQLException");

    statement.close();
  }

  @Override
  public int getMaxFieldSize() throws SQLException
  {
    logger.debug( "getMaxFieldSize() throws SQLException");

    return statement.getMaxFieldSize();
  }

  @Override
  public void setMaxFieldSize(int max) throws SQLException
  {
    throw new UnsupportedOperationException(
        "setMaxFieldSize(int max) Not supported yet.");
  }

  @Override
  public int getMaxRows() throws SQLException
  {
    logger.debug( "getMaxRows() throws SQLException");

    return statement.getMaxRows();
  }

  @Override
  public void setMaxRows(int max) throws SQLException
  {
    logger.debug( "setMaxRows(int max) throws SQLException");

    statement.setMaxRows(max);
  }

  @Override
  public void setEscapeProcessing(boolean enable) throws SQLException
  {
    throw new UnsupportedOperationException(
        "setEscapeProcessing(boolean enable) Not supported yet.");
  }

  @Override
  public int getQueryTimeout() throws SQLException
  {
    logger.debug( "getQueryTimeout() throws SQLException");

    return statement.getQueryTimeout();
  }

  @Override
  public void setQueryTimeout(int seconds) throws SQLException
  {
    logger.debug( "setQueryTimeout(int seconds) throws SQLException");

    statement.setQueryTimeout(seconds);
  }

  @Override
  public void cancel() throws SQLException
  {
    logger.debug( "cancel() throws SQLException");

    statement.cancel();
  }

  @Override
  public SQLWarning getWarnings() throws SQLException
  {
    logger.debug( "getWarnings() throws SQLException");

    return statement.getWarnings();
  }

  @Override
  public void clearWarnings() throws SQLException
  {
    logger.debug( "clearWarnings() throws SQLException");

    statement.clearWarnings();
  }

  @Override
  public void setCursorName(String name) throws SQLException
  {
    throw new UnsupportedOperationException(
        "setCursorName(String name) Not supported yet.");
  }

  @Override
  public boolean execute(String sql) throws SQLException
  {
    logger.debug( "execute(String sql) throws SQLException");

    boolean hasResult = statement.execute(sql);

    if (hasResult)
    {
      ResultSet resultSet = statement.getResultSet();
      resultMetadata = resultSet.getMetaData();
      return true;
    }
    else
      return false;
  }

  @Override
  public ResultSet getResultSet() throws SQLException
  {
    logger.debug( "public ResultSet getResultSet()");

    return statement.getResultSet();
  }

  @Override
  public int getUpdateCount() throws SQLException
  {
    logger.debug( "getUpdateCount() throws SQLException");

    return statement.getUpdateCount();
  }

  @Override
  public boolean getMoreResults() throws SQLException
  {
    logger.debug( "getMoreResults() throws SQLException");

    return statement.getMoreResults();
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException
  {
    logger.debug( "setFetchDirection(int direction) throws SQLException");

    if (direction != ResultSet.FETCH_FORWARD)
      throw new UnsupportedOperationException(
          "setFetchDirection(int direction) Not supported yet.");
  }

  @Override
  public int getFetchDirection() throws SQLException
  {
    logger.debug( "getFetchDirection() throws SQLException");

    return ResultSet.FETCH_FORWARD;
  }

  @Override
  public void setFetchSize(int rows) throws SQLException
  {
    logger.debug( "setFetchSize(int rows) throws SQLException");

    // don't throw exception, but we don't really support fetch size
  }

  @Override
  public int getFetchSize() throws SQLException
  {
    logger.debug( "getFetchSize() throws SQLException");

    return statement.getFetchSize();
  }

  @Override
  public int getResultSetConcurrency() throws SQLException
  {
    throw new UnsupportedOperationException(
        "getResultSetConcurrency() Not supported yet.");
  }

  @Override
  public int getResultSetType() throws SQLException
  {
    logger.debug( "getResultSetType() throws SQLException");

    return statement.getResultSetType();
  }

  @Override
  public void addBatch(String sql) throws SQLException
  {
    throw new UnsupportedOperationException(
        "addBatch(String sql) Not supported yet.");
  }

  @Override
  public void clearBatch() throws SQLException
  {
    batchParameterBindings.clear();
    batchSize = 0;
  }

  @Override
  public int[] executeBatch() throws SQLException
  {
    logger.debug( "executeBatch() throws SQLException");

    int[] updateCounts = null;
    try
    {
      int updateCount = statement.executeUpdateInternal(this.sql,
          batchParameterBindings);

      // when update count is the same as the number of bindings in the batch,
      // expand the update count into an array (SNOW-14034)
      if (updateCount == batchSize)
      {
        updateCounts = new int[updateCount];
        for (int idx = 0; idx < updateCount; idx++)
          updateCounts[idx] = 1;
      }
      else
      {
        updateCounts = new int[]{updateCount};
      }
    }
    finally
    {
      this.clearBatch();
    }

    return updateCounts;
  }

  @Override
  public Connection getConnection() throws SQLException
  {
    logger.debug( "getConnection() throws SQLException");

    return connection;
  }

  @Override
  public boolean getMoreResults(int current) throws SQLException
  {
    throw new UnsupportedOperationException(
        "getMoreResults(int current) Not supported yet.");
  }

  @Override
  public ResultSet getGeneratedKeys() throws SQLException
  {
    throw new UnsupportedOperationException(
        "getGeneratedKeys() Not supported yet.");
  }

  @Override
  public int executeUpdate(String sql, int autoGeneratedKeys)
      throws SQLException
  {
    throw new UnsupportedOperationException(
        "executeUpdate(String sql, int autoGeneratedKeys) Not supported yet.");
  }

  @Override
  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException
  {
    throw new UnsupportedOperationException(
        "executeUpdate(String sql, int[] columnIndexes) Not supported yet.");
  }

  @Override
  public int executeUpdate(String sql, String[] columnNames) throws SQLException
  {
    throw new UnsupportedOperationException(
        "executeUpdate(String sql, String[] columnNames) Not supported yet.");
  }

  @Override
  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException
  {
    throw new UnsupportedOperationException(
        "execute(String sql, int autoGeneratedKeys) Not supported yet.");
  }

  @Override
  public boolean execute(String sql, int[] columnIndexes) throws SQLException
  {
    throw new UnsupportedOperationException(
        "execute(String sql, int[] columnIndexes) Not supported yet.");
  }

  @Override
  public boolean execute(String sql, String[] columnNames) throws SQLException
  {
    throw new UnsupportedOperationException(
        "execute(String sql, String[] columnNames) Not supported yet.");
  }

  @Override
  public int getResultSetHoldability() throws SQLException
  {
    throw new UnsupportedOperationException(
        "getResultSetHoldability() Not supported yet.");
  }

  @Override
  public boolean isClosed() throws SQLException
  {
    logger.debug( "isClosed() throws SQLException");

    return statement.isClosed();
  }

  @Override
  public void setPoolable(boolean poolable) throws SQLException
  {
    throw new UnsupportedOperationException(
        "setPoolable(boolean poolable) Not supported yet.");
  }

  @Override
  public boolean isPoolable() throws SQLException
  {
    throw new UnsupportedOperationException(
        "isPoolable() Not supported yet.");
  }

  @Override
  public void closeOnCompletion() throws SQLException
  {
    throw new UnsupportedOperationException(
        "closeOnCompletion() Not supported yet.");
  }

  @Override
  public boolean isCloseOnCompletion() throws SQLException
  {
    throw new UnsupportedOperationException(
        "isCloseOnCompletion() Not supported yet.");
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException
  {
    throw new UnsupportedOperationException(
        "unwrap(Class<T> iface)  Not supported yet.");
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException
  {
    throw new UnsupportedOperationException(
        "isWrapperFor(Class<?> iface)  Not supported yet.");
  }
}
