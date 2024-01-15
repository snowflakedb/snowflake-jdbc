/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import net.snowflake.client.core.*;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.client.util.VariableTypeArray;
import net.snowflake.common.core.SFBinary;
import net.snowflake.common.core.SqlState;

class SnowflakePreparedStatementV1 extends SnowflakeStatementV1
    implements PreparedStatement, SnowflakePreparedStatement {
  static final SFLogger logger = SFLoggerFactory.getLogger(SnowflakePreparedStatementV1.class);
  /** Error code returned when describing a statement that is binding table name */
  private static final Integer ERROR_CODE_TABLE_BIND_VARIABLE_NOT_SET = 2128;
  /** Error code when preparing statement with binding object names */
  private static final Integer ERROR_CODE_OBJECT_BIND_NOT_SET = 2129;
  /** Error code returned when describing a ddl command */
  private static final Integer ERROR_CODE_STATEMENT_CANNOT_BE_PREPARED = 7;
  /** snow-44393 Workaround for compiler cannot prepare to_timestamp(?, 3) */
  private static final Integer ERROR_CODE_FORMAT_ARGUMENT_NOT_STRING = 1026;
  /** A hash set that contains the error code that will not lead to exception in describe mode */
  private static final Set<Integer> errorCodesIgnoredInDescribeMode =
      new HashSet<>(
          Arrays.asList(
              ERROR_CODE_TABLE_BIND_VARIABLE_NOT_SET,
              ERROR_CODE_STATEMENT_CANNOT_BE_PREPARED,
              ERROR_CODE_OBJECT_BIND_NOT_SET,
              ERROR_CODE_FORMAT_ARGUMENT_NOT_STRING));

  private final String sql;

  private SFPreparedStatementMetaData preparedStatementMetaData;

  /** statement and result metadata from describe phase */
  private boolean showStatementParameters;

  /**
   * map of bind name to bind values for single query execution
   *
   * <p>Currently, bind name is just value index
   */
  private Map<String, ParameterBindingDTO> parameterBindings = new HashMap<>();
  /** map of bind values for batch query executions */
  private Map<String, ParameterBindingDTO> batchParameterBindings = new HashMap<>();

  private Map<String, Boolean> wasPrevValueNull = new HashMap<>();
  /** Counter for batch size if we are executing a statement with array bind supported */
  private int batchSize = 0;

  private boolean alreadyDescribed = false;

  /**
   * Construct SnowflakePreparedStatementV1
   *
   * @param connection connection object
   * @param sql sql
   * @param skipParsing true if the applications want to skip parsing to get metadata. false by
   *     default.
   * @param resultSetType result set type: ResultSet.TYPE_FORWARD_ONLY.
   * @param resultSetConcurrency result set concurrency: ResultSet.CONCUR_READ_ONLY.
   * @param resultSetHoldability result set holdability: ResultSet.CLOSE_CURSORS_AT_COMMIT
   * @throws SQLException if any SQL error occurs.
   */
  SnowflakePreparedStatementV1(
      SnowflakeConnectionV1 connection,
      String sql,
      boolean skipParsing,
      int resultSetType,
      int resultSetConcurrency,
      int resultSetHoldability)
      throws SQLException {
    super(connection, resultSetType, resultSetConcurrency, resultSetHoldability);
    this.sql = sql;
    this.preparedStatementMetaData = SFPreparedStatementMetaData.emptyMetaData();
    showStatementParameters = connection.getShowStatementParameters();
  }

  /**
   * This method will check alreadyDescribed flag. And if it is false, it will try to issue a
   * describe request to server. If true, it will skip describe request.
   *
   * @throws SQLException
   */
  private void describeSqlIfNotTried() throws SQLException {
    if (!alreadyDescribed) {
      try {
        this.preparedStatementMetaData = sfBaseStatement.describe(sql);
      } catch (SFException e) {
        throw new SnowflakeSQLLoggedException(connection.getSFBaseSession(), e);
      } catch (SnowflakeSQLException e) {
        if (!errorCodesIgnoredInDescribeMode.contains(e.getErrorCode())) {
          throw e;
        } else {
          preparedStatementMetaData = SFPreparedStatementMetaData.emptyMetaData();
        }
      }
      alreadyDescribed = true;
    }
  }

  @Override
  public ResultSet executeQuery() throws SQLException {
    ExecTimeTelemetryData execTimeData =
        new ExecTimeTelemetryData("ResultSet PreparedStatement.executeQuery(String)", this.batchID);
    if (showStatementParameters) {
      logger.info("executeQuery()", false);
    } else {
      logger.debug("executeQuery()", false);
    }
    ResultSet rs = executeQueryInternal(sql, false, parameterBindings, execTimeData);
    execTimeData.setQueryEnd();
    execTimeData.generateTelemetry();
    return rs;
  }

  /**
   * Execute a query asynchronously
   *
   * @return ResultSet containing results
   * @throws SQLException
   */
  public ResultSet executeAsyncQuery() throws SQLException {
    ExecTimeTelemetryData execTimeData =
        new ExecTimeTelemetryData(
            "ResultSet PreparedStatement.executeAsyncQuery(String)", this.batchID);
    if (showStatementParameters) {
      logger.info("executeAsyncQuery()", false);
    } else {
      logger.debug("executeAsyncQuery()", false);
    }
    ResultSet rs = executeQueryInternal(sql, true, parameterBindings, execTimeData);
    execTimeData.setQueryEnd();
    execTimeData.generateTelemetry();
    return rs;
  }

  @Override
  public long executeLargeUpdate() throws SQLException {
    ExecTimeTelemetryData execTimeTelemetryData =
        new ExecTimeTelemetryData("long PreparedStatement.executeLargeUpdate()", this.batchID);
    logger.debug("executeLargeUpdate()", false);
    long updates = executeUpdateInternal(sql, parameterBindings, true, execTimeTelemetryData);
    return updates;
  }

  @Override
  public int executeUpdate() throws SQLException {
    logger.debug("executeUpdate()", false);
    return (int) executeLargeUpdate();
  }

  @Override
  public void setNull(int parameterIndex, int sqlType) throws SQLException {
    logger.debug(
        "setNull(parameterIndex: {}, sqlType: {})",
        parameterIndex,
        SnowflakeType.JavaSQLType.find(sqlType));
    raiseSQLExceptionIfStatementIsClosed();

    ParameterBindingDTO binding = new ParameterBindingDTO(SnowflakeType.ANY.toString(), null);
    parameterBindings.put(String.valueOf(parameterIndex), binding);
  }

  @Override
  public void setBoolean(int parameterIndex, boolean x) throws SQLException {
    logger.debug("setBoolean(parameterIndex: {}, boolean x)", parameterIndex);
    ParameterBindingDTO binding =
        new ParameterBindingDTO(
            SnowflakeUtil.javaTypeToSFTypeString(Types.BOOLEAN, connection.getSFBaseSession()),
            String.valueOf(x));
    parameterBindings.put(String.valueOf(parameterIndex), binding);
  }

  @Override
  public void setByte(int parameterIndex, byte x) throws SQLException {
    logger.debug("setByte(parameterIndex: {}, byte x)", parameterIndex);
    ParameterBindingDTO binding =
        new ParameterBindingDTO(
            SnowflakeUtil.javaTypeToSFTypeString(Types.TINYINT, connection.getSFBaseSession()),
            String.valueOf(x));
    parameterBindings.put(String.valueOf(parameterIndex), binding);
  }

  @Override
  public void setShort(int parameterIndex, short x) throws SQLException {
    logger.debug("setShort(parameterIndex: {}, short x)", parameterIndex);

    ParameterBindingDTO binding =
        new ParameterBindingDTO(
            SnowflakeUtil.javaTypeToSFTypeString(Types.SMALLINT, connection.getSFBaseSession()),
            String.valueOf(x));
    parameterBindings.put(String.valueOf(parameterIndex), binding);
  }

  @Override
  public void setInt(int parameterIndex, int x) throws SQLException {
    logger.debug("setInt(parameterIndex: {}, int x)", parameterIndex);

    ParameterBindingDTO binding =
        new ParameterBindingDTO(
            SnowflakeUtil.javaTypeToSFTypeString(Types.INTEGER, connection.getSFBaseSession()),
            String.valueOf(x));
    parameterBindings.put(String.valueOf(parameterIndex), binding);
  }

  @Override
  public void setLong(int parameterIndex, long x) throws SQLException {
    logger.debug("setLong(parameterIndex: {}, long x)", parameterIndex);

    ParameterBindingDTO binding =
        new ParameterBindingDTO(
            SnowflakeUtil.javaTypeToSFTypeString(Types.BIGINT, connection.getSFBaseSession()),
            String.valueOf(x));
    parameterBindings.put(String.valueOf(parameterIndex), binding);
  }

  @Override
  public void setBigInteger(int parameterIndex, BigInteger x) throws SQLException {
    logger.debug("setBigInteger(parameterIndex: {}, BigInteger x)", parameterIndex);

    ParameterBindingDTO binding =
        new ParameterBindingDTO(
            SnowflakeUtil.javaTypeToSFTypeString(Types.BIGINT, connection.getSFBaseSession()),
            String.valueOf(x));
    parameterBindings.put(String.valueOf(parameterIndex), binding);
  }

  @Override
  public void setFloat(int parameterIndex, float x) throws SQLException {
    logger.debug("setFloat(parameterIndex: {}, float x)", parameterIndex);

    ParameterBindingDTO binding =
        new ParameterBindingDTO(
            SnowflakeUtil.javaTypeToSFTypeString(Types.FLOAT, connection.getSFBaseSession()),
            String.valueOf(x));
    parameterBindings.put(String.valueOf(parameterIndex), binding);
  }

  @Override
  public void setDouble(int parameterIndex, double x) throws SQLException {
    logger.debug("setDouble(parameterIndex: {}, double x)", parameterIndex);

    ParameterBindingDTO binding =
        new ParameterBindingDTO(
            SnowflakeUtil.javaTypeToSFTypeString(Types.DOUBLE, connection.getSFBaseSession()),
            String.valueOf(x));
    parameterBindings.put(String.valueOf(parameterIndex), binding);
  }

  @Override
  public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
    logger.debug("setBigDecimal(parameterIndex: {}, BigDecimal x)", parameterIndex);

    if (x == null) {
      setNull(parameterIndex, Types.DECIMAL);
    } else {
      ParameterBindingDTO binding =
          new ParameterBindingDTO(
              SnowflakeUtil.javaTypeToSFTypeString(Types.DECIMAL, connection.getSFBaseSession()),
              String.valueOf(x));
      parameterBindings.put(String.valueOf(parameterIndex), binding);
    }
  }

  @Override
  public void setString(int parameterIndex, String x) throws SQLException {
    logger.debug("setString(parameterIndex: {}, String x)", parameterIndex);

    ParameterBindingDTO binding =
        new ParameterBindingDTO(
            SnowflakeUtil.javaTypeToSFTypeString(Types.VARCHAR, connection.getSFBaseSession()), x);
    parameterBindings.put(String.valueOf(parameterIndex), binding);
  }

  @Override
  public void setBytes(int parameterIndex, byte[] x) throws SQLException {
    logger.debug("setBytes(parameterIndex: {}, byte[] x)", parameterIndex);

    ParameterBindingDTO binding =
        new ParameterBindingDTO(
            SnowflakeUtil.javaTypeToSFTypeString(Types.BINARY, connection.getSFBaseSession()),
            new SFBinary(x).toHex());
    parameterBindings.put(String.valueOf(parameterIndex), binding);
  }

  @Override
  public void setDate(int parameterIndex, Date x) throws SQLException {
    logger.debug("setDate(parameterIndex: {}, Date x)", parameterIndex);

    if (x == null) {
      setNull(parameterIndex, Types.DATE);
    } else {
      ParameterBindingDTO binding =
          new ParameterBindingDTO(
              SnowflakeUtil.javaTypeToSFTypeString(Types.DATE, connection.getSFBaseSession()),
              String.valueOf(
                  x.getTime()
                      + TimeZone.getDefault().getOffset(x.getTime())
                      - ResultUtil.msDiffJulianToGregorian(x)));

      parameterBindings.put(String.valueOf(parameterIndex), binding);
    }
  }

  @Override
  public void setTime(int parameterIndex, Time x) throws SQLException {
    logger.debug("setTime(parameterIndex: {}, Time x)", parameterIndex);

    if (x == null) {
      setNull(parameterIndex, Types.TIME);
    } else {
      // Convert to nanoseconds since midnight using the input time mod 24 hours.
      final long MS_IN_DAY = 86400 * 1000;
      long msSinceEpoch = x.getTime();
      // Use % + % instead of just % to get the nonnegative remainder.
      // TODO(mkember): Change to use Math.floorMod when Client is on Java 8.
      long msSinceMidnight = (msSinceEpoch % MS_IN_DAY + MS_IN_DAY) % MS_IN_DAY;
      long nanosSinceMidnight = msSinceMidnight * 1000 * 1000;

      ParameterBindingDTO binding =
          new ParameterBindingDTO(
              SnowflakeUtil.javaTypeToSFTypeString(Types.TIME, connection.getSFBaseSession()),
              String.valueOf(nanosSinceMidnight));

      parameterBindings.put(String.valueOf(parameterIndex), binding);
    }
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
    logger.debug("setTimestamp(parameterIndex: {}, Timestamp x)", parameterIndex);

    setTimestampWithType(parameterIndex, x, Types.TIMESTAMP);
  }

  private void setTimestampWithType(int parameterIndex, Timestamp x, int snowflakeType)
      throws SQLException {
    // convert the timestamp from being in local time zone to be in UTC timezone
    String value =
        x == null
            ? null
            : String.valueOf(
                BigDecimal.valueOf((x.getTime() - ResultUtil.msDiffJulianToGregorian(x)) / 1000)
                    .scaleByPowerOfTen(9)
                    .add(BigDecimal.valueOf(x.getNanos())));
    String bindingTypeName;
    switch (snowflakeType) {
      case SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_LTZ:
        bindingTypeName = SnowflakeType.TIMESTAMP_LTZ.name();
        break;
      case SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_NTZ:
        bindingTypeName = SnowflakeType.TIMESTAMP_NTZ.name();
        break;
      default:
        bindingTypeName = connection.getSFBaseSession().getTimestampMappedType().name();
        break;
    }

    ParameterBindingDTO binding = new ParameterBindingDTO(bindingTypeName, value);
    parameterBindings.put(String.valueOf(parameterIndex), binding);
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(connection.getSFBaseSession());
  }

  @Override
  @Deprecated
  public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(connection.getSFBaseSession());
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(connection.getSFBaseSession());
  }

  @Override
  public void clearParameters() throws SQLException {
    parameterBindings.clear();
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
    if (x == null) {
      setNull(parameterIndex, targetSqlType);
    } else if (targetSqlType == Types.DATE) {
      setDate(parameterIndex, (Date) x);
    } else if (targetSqlType == Types.TIME) {
      setTime(parameterIndex, (Time) x);
    } else if (targetSqlType == Types.TIMESTAMP) {
      setTimestamp(parameterIndex, (Timestamp) x);
    } else if (targetSqlType == SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_LTZ
        || targetSqlType == SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_NTZ) {
      setTimestampWithType(parameterIndex, (Timestamp) x, targetSqlType);
    } else {
      logger.debug(
          "setObject(parameterIndex: {}, Object x, sqlType: {})",
          parameterIndex,
          SnowflakeType.JavaSQLType.find(targetSqlType));

      ParameterBindingDTO binding =
          new ParameterBindingDTO(
              SnowflakeUtil.javaTypeToSFTypeString(targetSqlType, connection.getSFBaseSession()),
              String.valueOf(x));
      parameterBindings.put(String.valueOf(parameterIndex), binding);
    }
  }

  @Override
  public void setObject(int parameterIndex, Object x) throws SQLException {
    if (x == null) {
      setNull(parameterIndex, Types.NULL);
    } else if (x instanceof String) {
      setString(parameterIndex, (String) x);
    } else if (x instanceof BigDecimal) {
      setBigDecimal(parameterIndex, (BigDecimal) x);
    } else if (x instanceof Short) {
      setShort(parameterIndex, (Short) x);
    } else if (x instanceof Integer) {
      setInt(parameterIndex, (Integer) x);
    } else if (x instanceof Long) {
      setLong(parameterIndex, (Long) x);
    } else if (x instanceof BigInteger) {
      setBigInteger(parameterIndex, (BigInteger) x);
    } else if (x instanceof Float) {
      setFloat(parameterIndex, (Float) x);
    } else if (x instanceof Double) {
      setDouble(parameterIndex, (Double) x);
    } else if (x instanceof Date) {
      setDate(parameterIndex, (Date) x);
    } else if (x instanceof Time) {
      setTime(parameterIndex, (Time) x);
    } else if (x instanceof Timestamp) {
      setTimestamp(parameterIndex, (Timestamp) x);
    } else if (x instanceof Boolean) {
      setBoolean(parameterIndex, (Boolean) x);
    } else if (x instanceof byte[]) {
      setBytes(parameterIndex, (byte[]) x);
    } else {
      throw new SnowflakeSQLLoggedException(
          connection.getSFBaseSession(),
          ErrorCode.DATA_TYPE_NOT_SUPPORTED.getMessageCode(),
          SqlState.FEATURE_NOT_SUPPORTED,
          "Object type: " + x.getClass());
    }
  }

  @Override
  public boolean execute() throws SQLException {
    ExecTimeTelemetryData execTimeData =
        new ExecTimeTelemetryData("boolean PreparedStatement.execute(String)", this.batchID);
    logger.debug("execute: {}", sql);
    boolean success = executeInternal(sql, parameterBindings, execTimeData);

    execTimeData.setQueryEnd();
    execTimeData.generateTelemetry();
    return success;
  }

  @Override
  public void addBatch() throws SQLException {
    logger.debug("addBatch()", false);

    raiseSQLExceptionIfStatementIsClosed();

    describeSqlIfNotTried();
    if (preparedStatementMetaData.isArrayBindSupported()) {
      for (Map.Entry<String, ParameterBindingDTO> binding : parameterBindings.entrySet()) {
        // get the entry for the bind variable in the batch binding map
        ParameterBindingDTO bindingValueAndType = batchParameterBindings.get(binding.getKey());

        List<String> values;

        Object newValue = binding.getValue().getValue();
        // create binding value and type for the first time
        if (bindingValueAndType == null) {
          // create the value list
          values = new ArrayList<>();

          bindingValueAndType = new ParameterBindingDTO(binding.getValue().getType(), values);

          // put the new map into the batch
          batchParameterBindings.put(binding.getKey(), bindingValueAndType);

          wasPrevValueNull.put(binding.getKey(), binding.getValue().getValue() == null);
        } else {
          // make sure type matches except for null values
          String prevType = bindingValueAndType.getType();
          String newType = binding.getValue().getType();

          if (wasPrevValueNull.get(binding.getKey()) && newValue != null) {
            // if previous value is null and the current value is not null
            // override the data type.
            bindingValueAndType = batchParameterBindings.remove(binding.getKey());
            bindingValueAndType.setType(newType);
            batchParameterBindings.put(binding.getKey(), bindingValueAndType);
            prevType = newType;
            wasPrevValueNull.put(binding.getKey(), false);
          }

          // if previous type is null, replace it with new type
          if (SnowflakeType.ANY.name().equalsIgnoreCase(prevType)
              && !SnowflakeType.ANY.name().equalsIgnoreCase(newType)) {
            bindingValueAndType.setType(newType);
          } else if (binding.getValue().getValue() != null && !prevType.equalsIgnoreCase(newType)) {
            String row = "Unknown";
            if (bindingValueAndType.getValue() instanceof Collection) {
              final List<String> typeCheckedList = (List<String>) bindingValueAndType.getValue();
              values = typeCheckedList;
              row = Integer.toString(values.size() + 1);
            }
            throw new SnowflakeSQLLoggedException(
                connection.getSFBaseSession(),
                ErrorCode.ARRAY_BIND_MIXED_TYPES_NOT_SUPPORTED.getMessageCode(),
                SqlState.FEATURE_NOT_SUPPORTED,
                SnowflakeType.getJavaType(SnowflakeType.fromString(prevType)).name(),
                SnowflakeType.getJavaType(SnowflakeType.fromString(newType)).name(),
                binding.getKey(),
                row);
          }

          // found the existing map so just get the value list
          final List<String> typeCheckedList = (List<String>) bindingValueAndType.getValue();
          values = typeCheckedList;
        }

        // add the value to the list of values in batch binding map
        values.add((String) newValue);
        bindingValueAndType.setValue(values);
      }
      batchSize++;
    } else {
      batch.add(new BatchEntry(this.sql, parameterBindings));
      parameterBindings = new HashMap<>();
    }
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, int length)
      throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(connection.getSFBaseSession());
  }

  @Override
  public void setRef(int parameterIndex, Ref x) throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(connection.getSFBaseSession());
  }

  @Override
  public void setBlob(int parameterIndex, Blob x) throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(connection.getSFBaseSession());
  }

  @Override
  public void setClob(int parameterIndex, Clob x) throws SQLException {
    setString(parameterIndex, x == null ? null : x.toString());
  }

  @Override
  public void setArray(int parameterIndex, Array x) throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(connection.getSFBaseSession());
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    logger.debug("getMetaData()", false);

    raiseSQLExceptionIfStatementIsClosed();

    describeSqlIfNotTried();
    return new SnowflakeResultSetMetaDataV1(this.preparedStatementMetaData.getResultSetMetaData());
  }

  @Override
  public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
    logger.debug("setDate(int parameterIndex, Date x, Calendar cal)", false);

    raiseSQLExceptionIfStatementIsClosed();
    if (x == null) {
      setNull(parameterIndex, Types.DATE);
    } else {
      // convert the date from to be in local time zone to be in UTC
      String value =
          String.valueOf(
              x.getTime()
                  + cal.getTimeZone().getOffset(x.getTime())
                  - ResultUtil.msDiffJulianToGregorian(x));

      ParameterBindingDTO binding =
          new ParameterBindingDTO(
              SnowflakeUtil.javaTypeToSFTypeString(Types.DATE, connection.getSFBaseSession()),
              value);
      parameterBindings.put(String.valueOf(parameterIndex), binding);
    }
  }

  @Override
  public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
    logger.debug("setTime(int parameterIndex, Time x, Calendar cal)", false);
    raiseSQLExceptionIfStatementIsClosed();
    setTime(parameterIndex, x);
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
    logger.debug("setTimestamp(int parameterIndex, Timestamp x, Calendar cal)", false);
    raiseSQLExceptionIfStatementIsClosed();

    // convert the time from being in UTC to be in local time zone
    String value = null;
    SnowflakeType sfType =
        SnowflakeUtil.javaTypeToSFType(Types.TIMESTAMP, connection.getSFBaseSession());
    if (x != null) {
      long milliSecSinceEpoch = x.getTime();

      if (sfType == SnowflakeType.TIMESTAMP) {
        sfType = connection.getSFBaseSession().getTimestampMappedType();
      }
      // if type is timestamp_tz, keep the offset and the time value separate.
      // store the offset, in minutes, as amount it's off from UTC
      if (sfType == SnowflakeType.TIMESTAMP_TZ) {
        value =
            String.valueOf(
                BigDecimal.valueOf(milliSecSinceEpoch / 1000)
                    .scaleByPowerOfTen(9)
                    .add(BigDecimal.valueOf(x.getNanos())));

        int offset = cal.getTimeZone().getOffset(milliSecSinceEpoch) / 60000 + 1440;
        value += " " + offset;
      } else {
        milliSecSinceEpoch = milliSecSinceEpoch + cal.getTimeZone().getOffset(milliSecSinceEpoch);

        value =
            String.valueOf(
                BigDecimal.valueOf(milliSecSinceEpoch / 1000)
                    .scaleByPowerOfTen(9)
                    .add(BigDecimal.valueOf(x.getNanos())));
      }
    }

    ParameterBindingDTO binding = new ParameterBindingDTO(sfType.name(), value);
    parameterBindings.put(String.valueOf(parameterIndex), binding);
  }

  @Override
  public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
    logger.debug("setNull(int parameterIndex, int sqlType, String typeName)", false);

    setNull(parameterIndex, sqlType);
  }

  @Override
  public void setURL(int parameterIndex, URL x) throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(connection.getSFBaseSession());
  }

  @Override
  public ParameterMetaData getParameterMetaData() throws SQLException {
    describeSqlIfNotTried();
    return new SnowflakeParameterMetadata(preparedStatementMetaData, connection.getSFBaseSession());
  }

  @Override
  public void setRowId(int parameterIndex, RowId x) throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(connection.getSFBaseSession());
  }

  @Override
  public void setNString(int parameterIndex, String value) throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(connection.getSFBaseSession());
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value, long length)
      throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(connection.getSFBaseSession());
  }

  @Override
  public void setNClob(int parameterIndex, NClob value) throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(connection.getSFBaseSession());
  }

  @Override
  public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(connection.getSFBaseSession());
  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream, long length)
      throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(connection.getSFBaseSession());
  }

  @Override
  public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(connection.getSFBaseSession());
  }

  @Override
  public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(connection.getSFBaseSession());
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength)
      throws SQLException {
    logger.debug(
        "setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength)", false);

    raiseSQLExceptionIfStatementIsClosed();
    if (x == null) {
      setNull(parameterIndex, targetSqlType);
    } else if (targetSqlType == Types.DECIMAL || targetSqlType == Types.NUMERIC) {
      BigDecimal decimalObj = new BigDecimal(String.valueOf(x));
      decimalObj.setScale(scaleOrLength);
      setBigDecimal(parameterIndex, decimalObj);
    } else {
      setObject(parameterIndex, x, targetSqlType);
    }
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(connection.getSFBaseSession());
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(connection.getSFBaseSession());
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, long length)
      throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(connection.getSFBaseSession());
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(connection.getSFBaseSession());
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(connection.getSFBaseSession());
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(connection.getSFBaseSession());
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(connection.getSFBaseSession());
  }

  @Override
  public void setClob(int parameterIndex, Reader reader) throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(connection.getSFBaseSession());
  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(connection.getSFBaseSession());
  }

  @Override
  public void setNClob(int parameterIndex, Reader reader) throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(connection.getSFBaseSession());
  }

  @Override
  public int executeUpdate(String sql) throws SQLException {
    logger.debug("executeUpdate(String sql)", false);

    throw new SnowflakeSQLException(
        ErrorCode.UNSUPPORTED_STATEMENT_TYPE_IN_EXECUTION_API, StmtUtil.truncateSQL(sql));
  }

  @Override
  public boolean execute(String sql) throws SQLException {
    logger.debug("execute(String sql)", false);

    throw new SnowflakeSQLException(
        ErrorCode.UNSUPPORTED_STATEMENT_TYPE_IN_EXECUTION_API, StmtUtil.truncateSQL(sql));
  }

  @Override
  public void addBatch(String sql) throws SQLException {
    throw new SnowflakeSQLException(
        ErrorCode.UNSUPPORTED_STATEMENT_TYPE_IN_EXECUTION_API, StmtUtil.truncateSQL(sql));
  }

  @Override
  public void clearBatch() throws SQLException {
    super.clearBatch();
    batchParameterBindings.clear();
    parameterBindings.clear();
    wasPrevValueNull.clear();
    batchSize = 0;
  }

  @Override
  public int[] executeBatch() throws SQLException {
    logger.debug("executeBatch()", false);
    return executeBatchInternalWithArrayBind(false).intArr;
  }

  @Override
  public long[] executeLargeBatch() throws SQLException {
    logger.debug("executeLargeBatch()", false);
    return executeBatchInternalWithArrayBind(true).longArr;
  }

  VariableTypeArray executeBatchInternalWithArrayBind(boolean isLong) throws SQLException {
    raiseSQLExceptionIfStatementIsClosed();

    describeSqlIfNotTried();

    if (this.preparedStatementMetaData.getStatementType().isGenerateResultSet()) {
      throw new SnowflakeSQLException(
          ErrorCode.UNSUPPORTED_STATEMENT_TYPE_IN_EXECUTION_API, StmtUtil.truncateSQL(sql));
    }

    VariableTypeArray updateCounts;
    if (isLong) {
      long[] arr = new long[batch.size()];
      updateCounts = new VariableTypeArray(null, arr);
    } else {
      int size = batch.size();
      int[] arr = new int[size];
      updateCounts = new VariableTypeArray(arr, null);
    }

    try {
      if (this.preparedStatementMetaData.isArrayBindSupported()) {
        if (batchSize <= 0) {
          if (isLong) {
            logger.debug(
                "executeLargeBatch() using array bind with no batch data. Return long[0] directly",
                false);
            return new VariableTypeArray(null, new long[0]);
          } else {
            logger.debug(
                "executeBatch() using array bind with no batch data. Return int[0] directly",
                false);
            return new VariableTypeArray(new int[0], null);
          }
        }

        int updateCount =
            (int)
                executeUpdateInternal(
                    this.sql, batchParameterBindings, false, new ExecTimeTelemetryData());

        // when update count is the same as the number of bindings in the batch,
        // expand the update count into an array (SNOW-14034)
        if (updateCount == batchSize) {
          if (isLong) {
            updateCounts = new VariableTypeArray(null, new long[updateCount]);
            for (int idx = 0; idx < updateCount; idx++) updateCounts.longArr[idx] = 1;
          } else {
            updateCounts = new VariableTypeArray(new int[updateCount], null);
            for (int idx = 0; idx < updateCount; idx++) updateCounts.intArr[idx] = 1;
          }
        } else {
          if (isLong) {
            updateCounts.longArr = new long[] {updateCount};
          } else {
            updateCounts.intArr = new int[] {updateCount};
          }
        }
      } else {
        // Array binding is not supported
        if (isLong) {
          updateCounts.longArr = executeBatchInternal(true).longArr;
        } else {
          updateCounts.intArr = executeBatchInternal(false).intArr;
        }
      }
    } finally {
      this.clearBatch();
    }

    return updateCounts;
  }

  @Override
  public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(connection.getSFBaseSession());
  }

  @Override
  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(connection.getSFBaseSession());
  }

  @Override
  public int executeUpdate(String sql, String[] columnNames) throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(connection.getSFBaseSession());
  }

  @Override
  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(connection.getSFBaseSession());
  }

  @Override
  public boolean execute(String sql, int[] columnIndexes) throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(connection.getSFBaseSession());
  }

  @Override
  public boolean execute(String sql, String[] columnNames) throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(connection.getSFBaseSession());
  }

  // For testing use only
  Map<String, ParameterBindingDTO> getBatchParameterBindings() {
    return batchParameterBindings;
  }

  // package private for testing purpose only
  Map<String, ParameterBindingDTO> getParameterBindings() {
    return parameterBindings;
  }

  // For testing use only
  public boolean isAlreadyDescribed() {
    return this.alreadyDescribed;
  }

  // For testing use only
  public boolean isArrayBindSupported() {
    return this.preparedStatementMetaData.isArrayBindSupported();
  }

  @Override
  public void resultSetMetadataHandler(SFBaseResultSet resultSet) throws SQLException {
    if (!this.preparedStatementMetaData.isValidMetaData()) {
      this.preparedStatementMetaData =
          new SFPreparedStatementMetaData(
              resultSet.getMetaData(),
              resultSet.getStatementType(),
              resultSet.getNumberOfBinds(),
              resultSet.isArrayBindSupported(),
              resultSet.getMetaDataOfBinds(),
              true);

      alreadyDescribed = true;
    }
  }

  public String toString() {
    return (this.sql != null) ? this.sql + " - Query ID: " + this.getQueryID() : super.toString();
  }
}
