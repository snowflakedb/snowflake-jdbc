/*
 * Copyright (c) 2012-2018 Snowflake Computing Inc. All rights reserved.
 */

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package net.snowflake.client.jdbc;

import net.snowflake.client.core.ParameterBindingDTO;
import net.snowflake.client.core.ResultUtil;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SFStatementMetaData;
import net.snowflake.common.core.SFBinary;
import net.snowflake.common.core.SqlState;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

/**
 *
 * @author jhuang
 */
final class SnowflakePreparedStatementV1 extends SnowflakeStatementV1
                                         implements PreparedStatement
{
  static final SFLogger logger = SFLoggerFactory.getLogger(
      SnowflakePreparedStatementV1.class);

  private final String sql;

  /** statement and result metadata from describe phase */
  private SFStatementMetaData statementMetaData;

  /**
   * map of bind name to bind values for single query execution
   *
   * Currently, bind name is just value index
   */
  private Map<String, ParameterBindingDTO> parameterBindings = new HashMap<>();

  /**
   * map of bind values for batch query executions
   *
   */
  private Map<String, ParameterBindingDTO> batchParameterBindings =
      new HashMap<>();

  /**
   * Counter for batch size if we are executing a statement with array bind
   * supported
   */
  private int batchSize = 0;

  /** Error code returned when describing a statement that is binding table name*/
  private final Integer ERROR_CODE_TABLE_BIND_VARIABLE_NOT_SET = 2128;

  /** Error code when preparing statement with binding object names */
  private final Integer ERROR_CODE_OBJECT_BIND_NOT_SET = 2129;

  /** Error code returned when describing a ddl command */
  private final Integer ERROR_CODE_STATEMENT_CANNOT_BE_PREPARED = 7;

  /**
   * A hash set that contains the error code that will not lead to exception
   * in describe mode
   */
  private final Set<Integer> errorCodesIgnoredInDescribeMode
      = new HashSet<>(Arrays.asList(new Integer[]
      {ERROR_CODE_TABLE_BIND_VARIABLE_NOT_SET,
       ERROR_CODE_STATEMENT_CANNOT_BE_PREPARED,
       ERROR_CODE_OBJECT_BIND_NOT_SET}));

  SnowflakePreparedStatementV1(SnowflakeConnectionV1 connection,
                               String sql, boolean skipParsing) throws SQLException
  {
    super(connection);
    this.sql = sql;

    if (!skipParsing)
    {
      try
      {
        this.statementMetaData = sfStatement.describe(sql);
      }
      catch (SFException e)
      {
        throw new SnowflakeSQLException(e);
      }
      catch (SnowflakeSQLException e)
      {
        if (!errorCodesIgnoredInDescribeMode.contains(e.getErrorCode()))
        {
          throw e;
        }
        else
        {
          statementMetaData = SFStatementMetaData.emptyMetaData();
        }
      }
    }
    else
    {
      statementMetaData = SFStatementMetaData.emptyMetaData();
    }
  }

  @Override
  public ResultSet executeQuery() throws SQLException
  {
    logger.debug("executeQuery() throws SQLException");

    return executeQueryInternal(sql, parameterBindings);
  }

  @Override
  public int executeUpdate() throws SQLException
  {
    logger.debug( "executeUpdate() throws SQLException");

    return executeUpdateInternal(sql, parameterBindings);
  }

  @Override
  public void setNull(int parameterIndex, int sqlType) throws SQLException
  {
    logger.debug(
        "setNull(int parameterIndex, int sqlType) throws SQLException");

    ParameterBindingDTO binding = new ParameterBindingDTO(
                        SnowflakeUtil.javaTypeToSFTypeString(sqlType), null);
    parameterBindings.put(String.valueOf(parameterIndex), binding);
  }

  @Override
  public void setBoolean(int parameterIndex, boolean x) throws SQLException
  {
    logger.debug(
               "setBoolean(int parameterIndex, boolean x) throws SQLException");

    ParameterBindingDTO binding = new ParameterBindingDTO(
        SnowflakeUtil.javaTypeToSFTypeString(Types.BOOLEAN), String.valueOf(x));
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

    ParameterBindingDTO binding = new ParameterBindingDTO(SnowflakeUtil
        .javaTypeToSFTypeString(Types.SMALLINT), String.valueOf(x));
    parameterBindings.put(String.valueOf(parameterIndex), binding);
  }

  @Override
  public void setInt(int parameterIndex, int x) throws SQLException
  {
    logger.debug(
        "setInt(int parameterIndex, int x) throws SQLException");

    ParameterBindingDTO binding = new ParameterBindingDTO(SnowflakeUtil
                  .javaTypeToSFTypeString(Types.INTEGER), String.valueOf(x));
    parameterBindings.put(String.valueOf(parameterIndex), binding);
  }

  @Override
  public void setLong(int parameterIndex, long x) throws SQLException
  {
    logger.debug(
        "setLong(int parameterIndex, long x) throws SQLException");

    ParameterBindingDTO binding = new ParameterBindingDTO(
         SnowflakeUtil.javaTypeToSFTypeString(Types.BIGINT), String.valueOf(x));
    parameterBindings.put(String.valueOf(parameterIndex), binding);
  }

  @Override
  public void setFloat(int parameterIndex, float x) throws SQLException
  {
    logger.debug(
        "setFloat(int parameterIndex, float x) throws SQLException");

    ParameterBindingDTO binding = new ParameterBindingDTO(
        SnowflakeUtil.javaTypeToSFTypeString(Types.FLOAT), String.valueOf(x));
    parameterBindings.put(String.valueOf(parameterIndex), binding);
  }

  @Override
  public void setDouble(int parameterIndex, double x) throws SQLException
  {
    logger.debug(
        "setDouble(int parameterIndex, double x) throws SQLException");

    ParameterBindingDTO binding = new ParameterBindingDTO(
        SnowflakeUtil.javaTypeToSFTypeString(Types.DOUBLE), String.valueOf(x));
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
      ParameterBindingDTO binding = new ParameterBindingDTO(
        SnowflakeUtil.javaTypeToSFTypeString(Types.DECIMAL), String.valueOf(x));
      parameterBindings.put(String.valueOf(parameterIndex), binding);
    }
  }

  @Override
  public void setString(int parameterIndex, String x) throws SQLException
  {
    logger.debug(
        "setString(int parameterIndex, String x) throws SQLException");

    ParameterBindingDTO binding = new ParameterBindingDTO(
        SnowflakeUtil.javaTypeToSFTypeString(Types.VARCHAR), x);
    parameterBindings.put(String.valueOf(parameterIndex), binding);
  }

  @Override
  public void setBytes(int parameterIndex, byte[] x) throws SQLException
  {
    logger.debug(
        "setBytes(int parameterIndex, byte[] x) throws SQLException");

    ParameterBindingDTO binding = new ParameterBindingDTO(SnowflakeUtil
        .javaTypeToSFTypeString(Types.BINARY), new SFBinary(x).toHex());
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
      ParameterBindingDTO binding = new ParameterBindingDTO(
          SnowflakeUtil.javaTypeToSFTypeString(Types.DATE),
          String.valueOf(x.getTime() +
          TimeZone.getDefault().getOffset(x.getTime())-
          ResultUtil.msDiffJulianToGregorian(x)));

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
      // Convert to nanoseconds since midnight using the input time mod 24 hours.
      final long MS_IN_DAY = 86400 * 1000;
      long msSinceEpoch = x.getTime();
      // Use % + % instead of just % to get the nonnegative remainder.
      // TODO(mkember): Change to use Math.floorMod when Client is on Java 8.
      long msSinceMidnight = (msSinceEpoch % MS_IN_DAY + MS_IN_DAY) % MS_IN_DAY;
      long nanosSinceMidnight = msSinceMidnight * 1000 * 1000;

      ParameterBindingDTO binding = new ParameterBindingDTO(
                      SnowflakeUtil.javaTypeToSFTypeString(Types.TIME),
                      String.valueOf(nanosSinceMidnight));

      parameterBindings.put(String.valueOf(parameterIndex), binding);
      sfStatement.setHasUnsupportedStageBind(true);
    }
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException
  {
    logger.debug(
        "setTimestamp(int parameterIndex, Timestamp x) throws SQLException");

    // convert the timestamp from being in local time zone to be in UTC timezone
    String value =  x == null ? null :
        String.valueOf(
        BigDecimal.valueOf((x.getTime() - ResultUtil.msDiffJulianToGregorian(x))/1000).
        scaleByPowerOfTen(9).add(BigDecimal.valueOf(x.getNanos())));

    SnowflakeType sfType = SnowflakeUtil.javaTypeToSFType(Types.TIMESTAMP);

    if (sfType == SnowflakeType.TIMESTAMP)
    {
      sfType = connection.getSfSession().getTimestampMappedType();
    }

    ParameterBindingDTO binding = new ParameterBindingDTO(sfType.name(), value);
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
    if (batchParameterBindings.isEmpty())
    {
      sfStatement.setHasUnsupportedStageBind(false);
    }
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException
  {
    logger.debug(
        "setObject(int parameterIndex, Object x, int targetSqlType)");

    if (x == null)
    {
      setNull(parameterIndex, targetSqlType);
    }
    else if (targetSqlType == Types.DATE)
    {
      setDate(parameterIndex, (Date) x);
    }
    else if (targetSqlType == Types.TIME)
    {
      setTime(parameterIndex, (Time) x);
    }
    else if (targetSqlType == Types.TIMESTAMP)
    {
      setTimestamp(parameterIndex, (Timestamp) x);
    }
    else
    {
      ParameterBindingDTO binding = new ParameterBindingDTO(
                        SnowflakeUtil.javaTypeToSFTypeString(targetSqlType),
                        String.valueOf(x));
      parameterBindings.put(String.valueOf(parameterIndex), binding);
    }
  }

  @Override
  public void setObject(int parameterIndex, Object x) throws SQLException
  {
    if (x == null)
    {
      setNull(parameterIndex, Types.NULL);
    }
    else if (x instanceof String)
    {
      setString(parameterIndex, (String) x);
    }
    else if (x instanceof BigDecimal)
    {
      setBigDecimal(parameterIndex, (BigDecimal) x);
    }
    else if (x instanceof Short)
    {
      setShort(parameterIndex, (Short)x);
    }
    else if (x instanceof Integer)
    {
      setInt(parameterIndex, (Integer) x);
    }
    else if (x instanceof Long)
    {
      setLong(parameterIndex, (Long) x);
    }
    else if (x instanceof Float)
    {
      setFloat(parameterIndex, (Float) x);
    }
    else if (x instanceof Double)
    {
      setDouble(parameterIndex, (Double) x);
    }
    else if (x instanceof Date)
    {
      setDate(parameterIndex, (Date) x);
    }
    else if (x instanceof Time)
    {
      setTime(parameterIndex, (Time) x);
    }
    else if (x instanceof Timestamp)
    {
      setTimestamp(parameterIndex, (Timestamp) x);
    }
    else if (x instanceof Boolean)
    {
      setBoolean(parameterIndex, (Boolean)x);
    }
    else
    {
      throw new SnowflakeSQLException(SqlState.FEATURE_NOT_SUPPORTED,
          ErrorCode.DATA_TYPE_NOT_SUPPORTED.getMessageCode(),
          "Object type: " + x.getClass());
    }
  }

  @Override
  public boolean execute() throws SQLException
  {
    logger.debug( "execute: {}", sql);

    return executeInternal(sql, parameterBindings);
  }

  @Override
  public void addBatch() throws SQLException
  {
    logger.debug( "addBatch() throws SQLException");

    if (isClosed())
    {
      throw new SnowflakeSQLException(ErrorCode.STATEMENT_CLOSED);
    }

    if (statementMetaData.isArrayBindSupported())
    {
      for(Map.Entry<String, ParameterBindingDTO> binding :
          parameterBindings.entrySet())
      {
        // get the entry for the bind variable in the batch binding map
        ParameterBindingDTO bindingValueAndType =
            batchParameterBindings.get(binding.getKey());

        List<String> values;

        // create binding value and type for the first time
        if (bindingValueAndType == null)
        {
          // create the value list
          values = new ArrayList<String>();

          bindingValueAndType = new ParameterBindingDTO(
              binding.getValue().getType(), values);

          // put the new map into the batch
          batchParameterBindings.put(binding.getKey(),
              bindingValueAndType);
        }
        else
        {
          // make sure type matches except for null values
          String prevType = bindingValueAndType.getType();
          String newType = binding.getValue().getType();

          // if previous type is null, replace it with new type
          if (SnowflakeType.ANY.name().equalsIgnoreCase(prevType) &&
              !SnowflakeType.ANY.name().equalsIgnoreCase(newType))
          {
            bindingValueAndType.setType(newType);
          }
          else if (binding.getValue().getValue() != null &&
              !prevType.equalsIgnoreCase(newType))
          {
            throw new SnowflakeSQLException(SqlState.FEATURE_NOT_SUPPORTED,
                ErrorCode.ARRAY_BIND_MIXED_TYPES_NOT_SUPPORTED.getMessageCode(),
                SnowflakeType.getJavaType(SnowflakeType.fromString(prevType)).name(),
                SnowflakeType.getJavaType(SnowflakeType.fromString(newType)).name());
          }

          // found the existing map so just get the value list
          values = (List<String>) bindingValueAndType.getValue();
        }

        // add the value to the list of values in batch binding map
        values.add((String)binding.getValue().getValue());
      }
      batchSize++;
    }
    else
    {
      batch.add(new BatchEntry(this.sql, parameterBindings));
      parameterBindings = new HashMap<>();
    }
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

    return new SnowflakeResultSetMetaDataV1(
        this.statementMetaData.getResultSetMetaData());
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
      // convert the date from to be in local time zone to be in UTC
      String value = String.valueOf(x.getTime() +
                             cal.getTimeZone().getOffset(x.getTime()));

      ParameterBindingDTO binding = new ParameterBindingDTO(
          SnowflakeUtil.javaTypeToSFTypeString(Types.DATE), value);
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

    // convert the time from being in UTC to be in local time zone
    String value = null;
    if (x != null)
    {
      long milliSecSinceEpoch = x.getTime();
      milliSecSinceEpoch = milliSecSinceEpoch +
          cal.getTimeZone().getOffset(milliSecSinceEpoch);

      value = String.valueOf(
          BigDecimal.valueOf(milliSecSinceEpoch / 1000).
              scaleByPowerOfTen(9).add(BigDecimal.valueOf(x.getNanos())));
    }

    SnowflakeType sfType = SnowflakeUtil.javaTypeToSFType(Types.TIMESTAMP);

    if (sfType == SnowflakeType.TIMESTAMP)
    {
      sfType = connection.getSfSession().getTimestampMappedType();
    }

    ParameterBindingDTO binding = new ParameterBindingDTO(sfType.name(), value);
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

    throw new SnowflakeSQLException(ErrorCode.
        UNSUPPORTED_STATEMENT_TYPE_IN_EXECUTION_API,
        sql.length() > 20 ? sql.substring(0, 20) + "..." : sql);
  }

  @Override
  public int executeUpdate(String sql) throws SQLException
  {
    logger.debug( "executeUpdate(String sql) throws SQLException");

    throw new SnowflakeSQLException(ErrorCode.
        UNSUPPORTED_STATEMENT_TYPE_IN_EXECUTION_API,
        sql.length() > 20 ? sql.substring(0, 20) + "..." : sql);
  }

  @Override
  public boolean execute(String sql) throws SQLException
  {
    logger.debug( "execute(String sql) throws SQLException");

    throw new SnowflakeSQLException(ErrorCode.
        UNSUPPORTED_STATEMENT_TYPE_IN_EXECUTION_API,
        sql.length() > 20 ? sql.substring(0, 20) + "..." : sql);
  }

  @Override
  public void addBatch(String sql) throws SQLException
  {
    throw new SnowflakeSQLException(ErrorCode.
        UNSUPPORTED_STATEMENT_TYPE_IN_EXECUTION_API,
        sql.length() > 20 ? sql.substring(0, 20) + "..." : sql);
  }

  @Override
  public void clearBatch() throws SQLException
  {
    super.clearBatch();
    batchParameterBindings.clear();
    batchSize = 0;
    sfStatement.setHasUnsupportedStageBind(false);
  }

  @Override
  public int[] executeBatch() throws SQLException
  {
    logger.debug( "executeBatch() throws SQLException");

    if (this.statementMetaData.getStatementType().isGenerateResultSet())
    {
      throw new SnowflakeSQLException(ErrorCode.
          UNSUPPORTED_STATEMENT_TYPE_IN_EXECUTION_API,
          sql.length() > 20 ? sql.substring(0, 20) + "..." : sql);
    }

    int[] updateCounts = null;
    try
    {
      if (this.statementMetaData.isArrayBindSupported())
      {
        int updateCount = executeUpdateInternal(
                              this.sql, batchParameterBindings);

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
      else
      {
        updateCounts = executeBatchInternal();
      }
    }
    finally
    {
      this.clearBatch();
    }

    return updateCounts;
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

  // For testing use only
  Map<String, ParameterBindingDTO> getBatchParameterBindings()
  {
    return batchParameterBindings;
  }
}
