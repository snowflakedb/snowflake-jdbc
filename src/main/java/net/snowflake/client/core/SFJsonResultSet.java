/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.TimeZone;
import net.snowflake.client.core.arrow.ArrowResultUtil;
import net.snowflake.client.jdbc.*;
import net.snowflake.client.log.ArgSupplier;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.common.core.SFBinary;
import net.snowflake.common.core.SFBinaryFormat;
import net.snowflake.common.core.SFTime;
import net.snowflake.common.core.SFTimestamp;
import org.apache.arrow.vector.Float8Vector;

/** Abstract class used to represent snowflake result set in json format */
public abstract class SFJsonResultSet extends SFBaseResultSet {
  private static final SFLogger logger = SFLoggerFactory.getLogger(SFJsonResultSet.class);

  // Timezone used for TimestampNTZ
  private static final TimeZone timeZoneUTC = TimeZone.getTimeZone("UTC");

  TimeZone sessionTimeZone;

  // Precision of maximum long value in Java (2^63-1). Precision is 19
  private static final int LONG_PRECISION = 19;

  private static final BigDecimal MAX_LONG_VAL = new BigDecimal(Long.MAX_VALUE);
  private static final BigDecimal MIN_LONG_VAL = new BigDecimal(Long.MIN_VALUE);

  /**
   * Given a column index, get current row's value as an object
   *
   * @param columnIndex index of columns
   * @return an object
   * @throws SFException raises if any error occurs
   */
  protected abstract Object getObjectInternal(int columnIndex) throws SFException;

  public Object getObject(int columnIndex) throws SFException {

    int type = resultSetMetaData.getColumnType(columnIndex);

    Object obj = getObjectInternal(columnIndex);
    if (obj == null) {
      return null;
    }

    switch (type) {
      case Types.VARCHAR:
      case Types.CHAR:
        return getString(columnIndex);

      case Types.BINARY:
        return getBytes(columnIndex);

      case Types.INTEGER:
        return getInt(columnIndex);

      case Types.DECIMAL:
        return getBigDecimal(columnIndex);

      case Types.BIGINT:
        return getBigInt(columnIndex, obj);

      case Types.DOUBLE:
        return getDouble(columnIndex);

      case Types.TIMESTAMP:
        return getTimestamp(columnIndex);

      case Types.DATE:
        return getDate(columnIndex);

      case Types.TIME:
        return getTime(columnIndex);

      case Types.BOOLEAN:
        return getBoolean(columnIndex);

      default:
        throw (SFException)
            IncidentUtil.generateIncidentV2WithException(
                session,
                new SFException(ErrorCode.FEATURE_UNSUPPORTED, "data type: " + type),
                null,
                null);
    }
  }

  /**
   * Sometimes large BIGINTS overflow the java Long type. In these cases, return a BigDecimal type
   * instead.
   *
   * @param columnIndex the column index
   * @return an object of type long or BigDecimal depending on number size
   * @throws SFException
   */
  private Object getBigInt(int columnIndex, Object obj) throws SFException {
    // If precision is < precision of max long precision, we can automatically convert to long.
    // Otherwise, do a check to ensure it doesn't overflow max long value.
    String numberAsString = obj.toString();
    if (numberAsString.length() >= LONG_PRECISION) {
      BigDecimal bigNum = getBigDecimal(columnIndex);
      if (bigNum.compareTo(MAX_LONG_VAL) == 1 || bigNum.compareTo(MIN_LONG_VAL) == -1) {
        return bigNum;
      }
    }
    return getLong(columnIndex);
  }

  @Override
  public String getString(int columnIndex) throws SFException {
    logger.debug("public String getString(int columnIndex)");

    // Column index starts from 1, not 0.
    Object obj = getObjectInternal(columnIndex);
    if (obj == null) {
      return null;
    }

    // print timestamp in string format
    int columnType = resultSetMetaData.getInternalColumnType(columnIndex);
    switch (columnType) {
      case Types.BOOLEAN:
        return ResultUtil.getBooleanAsString(ResultUtil.getBoolean(obj.toString()));

      case Types.TIMESTAMP:
      case SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_LTZ:
      case SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_TZ:
        SFTimestamp sfTS = getSFTimestamp(columnIndex);
        int columnScale = resultSetMetaData.getScale(columnIndex);

        String timestampStr =
            ResultUtil.getSFTimestampAsString(
                sfTS,
                columnType,
                columnScale,
                timestampNTZFormatter,
                timestampLTZFormatter,
                timestampTZFormatter,
                session);

        logger.debug(
            "Converting timestamp to string from: {} to: {}",
            (ArgSupplier) obj::toString,
            timestampStr);

        return timestampStr;

      case Types.DATE:
        Date date = getDate(columnIndex);

        if (dateFormatter == null) {
          throw (SFException)
              IncidentUtil.generateIncidentV2WithException(
                  session,
                  new SFException(ErrorCode.INTERNAL_ERROR, "missing date formatter"),
                  null,
                  null);
        }

        String dateStr = ResultUtil.getDateAsString(date, dateFormatter);

        logger.debug(
            "Converting date to string from: {} to: {}", (ArgSupplier) obj::toString, dateStr);

        return dateStr;

      case Types.TIME:
        SFTime sfTime = getSFTime(columnIndex);

        if (timeFormatter == null) {
          throw (SFException)
              IncidentUtil.generateIncidentV2WithException(
                  session,
                  new SFException(ErrorCode.INTERNAL_ERROR, "missing time formatter"),
                  null,
                  null);
        }

        int scale = resultSetMetaData.getScale(columnIndex);
        String timeStr = ResultUtil.getSFTimeAsString(sfTime, scale, timeFormatter);

        logger.debug(
            "Converting time to string from: {} to: {}", (ArgSupplier) obj::toString, timeStr);

        return timeStr;

      case Types.BINARY:
        if (binaryFormatter == null) {
          throw (SFException)
              IncidentUtil.generateIncidentV2WithException(
                  session,
                  new SFException(ErrorCode.INTERNAL_ERROR, "missing binary formatter"),
                  null,
                  null);
        }

        if (binaryFormatter == SFBinaryFormat.HEX) {
          // Shortcut: the values are already passed with hex encoding, so just
          // return the string unchanged rather than constructing an SFBinary.
          return obj.toString();
        }

        SFBinary sfb = new SFBinary(getBytes(columnIndex));
        return binaryFormatter.format(sfb);

      default:
        break;
    }

    return obj.toString();
  }

  @Override
  public boolean getBoolean(int columnIndex) throws SFException {
    logger.debug("public boolean getBoolean(int columnIndex)");
    Object obj = getObjectInternal(columnIndex);
    if (obj == null) {
      return false;
    }
    if (obj instanceof Boolean) {
      return (Boolean) obj;
    }
    int columnType = resultSetMetaData.getColumnType(columnIndex);
    // if type is an approved type that can be converted to Boolean, do this
    if (columnType == Types.BOOLEAN
        || columnType == Types.INTEGER
        || columnType == Types.SMALLINT
        || columnType == Types.TINYINT
        || columnType == Types.BIGINT
        || columnType == Types.BIT
        || columnType == Types.VARCHAR
        || columnType == Types.CHAR) {
      String type = obj.toString();
      if ("1".equals(type) || Boolean.TRUE.toString().equalsIgnoreCase(type)) {
        return true;
      }
      if ("0".equals(type) || Boolean.FALSE.toString().equalsIgnoreCase(type)) {
        return false;
      }
    }
    throw new SFException(
        ErrorCode.INVALID_VALUE_CONVERT, columnType, SnowflakeUtil.BOOLEAN_STR, obj);
  }

  @Override
  public byte getByte(int columnIndex) throws SFException {
    logger.debug("public short getByte(int columnIndex)");

    // Column index starts from 1, not 0.
    Object obj = getObjectInternal(columnIndex);

    if (obj == null) {
      return 0;
    }

    if (obj instanceof String) {
      return Byte.parseByte((String) obj);
    } else {
      return ((Number) obj).byteValue();
    }
  }

  @Override
  public short getShort(int columnIndex) throws SFException {
    logger.debug("public short getShort(int columnIndex)");

    // Column index starts from 1, not 0.
    Object obj = getObjectInternal(columnIndex);

    if (obj == null) {
      return 0;
    }
    int columnType = resultSetMetaData.getColumnType(columnIndex);
    try {

      if (obj instanceof String) {
        String objString = (String) obj;
        if (objString.contains(".") && (columnType == Types.FLOAT || columnType == Types.DOUBLE)) {
          objString = objString.substring(0, objString.indexOf("."));
        }
        return Short.parseShort(objString);
      } else {
        return ((Number) obj).shortValue();
      }
    } catch (NumberFormatException ex) {
      throw new SFException(
          ErrorCode.INVALID_VALUE_CONVERT, columnType, SnowflakeUtil.SHORT_STR, obj);
    }
  }

  @Override
  public int getInt(int columnIndex) throws SFException {
    logger.debug("public int getInt(int columnIndex)");

    // Column index starts from 1, not 0.
    Object obj = getObjectInternal(columnIndex);

    if (obj == null) {
      return 0;
    }
    int columnType = resultSetMetaData.getColumnType(columnIndex);
    try {
      if (obj instanceof String) {
        String objString = (String) obj;
        if (objString.contains(".") && (columnType == Types.FLOAT || columnType == Types.DOUBLE)) {
          objString = objString.substring(0, objString.indexOf("."));
        }
        return Integer.parseInt(objString);
      } else {
        return ((Number) obj).intValue();
      }
    } catch (NumberFormatException ex) {
      throw new SFException(
          ErrorCode.INVALID_VALUE_CONVERT, columnType, SnowflakeUtil.INT_STR, obj);
    }
  }

  @Override
  public long getLong(int columnIndex) throws SFException {
    logger.debug("public long getLong(int columnIndex)");

    // Column index starts from 1, not 0.
    Object obj = getObjectInternal(columnIndex);

    if (obj == null) {
      return 0;
    }
    int columnType = resultSetMetaData.getColumnType(columnIndex);
    try {
      if (obj instanceof String) {
        String objString = (String) obj;
        if (objString.contains(".") && (columnType == Types.FLOAT || columnType == Types.DOUBLE)) {
          objString = objString.substring(0, objString.indexOf("."));
        }
        return Long.parseLong(objString);
      } else {
        return ((Number) obj).longValue();
      }
    } catch (NumberFormatException nfe) {

      if (Types.INTEGER == columnType || Types.SMALLINT == columnType) {
        throw (SFException)
            IncidentUtil.generateIncidentV2WithException(
                session,
                new SFException(
                    ErrorCode.INTERNAL_ERROR, SnowflakeUtil.LONG_STR + ": " + obj.toString()),
                null,
                null);
      } else {
        throw new SFException(
            ErrorCode.INVALID_VALUE_CONVERT, columnType, SnowflakeUtil.LONG_STR, obj);
      }
    }
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex) throws SFException {
    logger.debug("public BigDecimal getBigDecimal(int columnIndex)");

    // Column index starts from 1, not 0.
    Object obj = getObjectInternal(columnIndex);

    if (obj == null) {
      return null;
    }
    int columnType = resultSetMetaData.getColumnType(columnIndex);
    try {
      if (columnType != Types.TIME
          && columnType != Types.TIMESTAMP
          && columnType != Types.TIMESTAMP_WITH_TIMEZONE) {
        return new BigDecimal(obj.toString());
      }
      throw new SFException(
          ErrorCode.INVALID_VALUE_CONVERT, columnType, SnowflakeUtil.BIG_DECIMAL_STR, obj);

    } catch (NumberFormatException ex) {
      throw new SFException(
          ErrorCode.INVALID_VALUE_CONVERT, columnType, SnowflakeUtil.BIG_DECIMAL_STR, obj);
    }
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex, int scale) throws SFException {
    logger.debug("public BigDecimal getBigDecimal(int columnIndex)");

    Object obj = getObjectInternal(columnIndex);

    if (obj == null) {
      return null;
    }
    BigDecimal value = new BigDecimal(obj.toString());

    value = value.setScale(scale, RoundingMode.HALF_UP);

    return value;
  }

  private TimeZone adjustTimezoneForTimestampTZ(int columnIndex) throws SFException {
    // If the timestamp is of type timestamp_tz, use the associated offset timezone instead of the
    // session timezone for formatting
    Object obj = getObjectInternal(columnIndex);
    int subType = resultSetMetaData.getInternalColumnType(columnIndex);
    if (obj != null && subType == SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_TZ && resultVersion > 0) {
      String timestampStr = obj.toString();
      int indexForSeparator = timestampStr.indexOf(' ');
      String timezoneIndexStr = timestampStr.substring(indexForSeparator + 1);
      return SFTimestamp.convertTimezoneIndexToTimeZone(Integer.parseInt(timezoneIndexStr));
    }
    // By default, return session timezone
    return sessionTimeZone;
  }

  private SFTimestamp getSFTimestamp(int columnIndex) throws SFException {
    logger.debug("public Timestamp getTimestamp(int columnIndex)");

    Object obj = getObjectInternal(columnIndex);

    if (obj == null) {
      return null;
    }

    return ResultUtil.getSFTimestamp(
        obj.toString(),
        resultSetMetaData.getScale(columnIndex),
        resultSetMetaData.getInternalColumnType(columnIndex),
        resultVersion,
        sessionTimeZone,
        session);
  }

  @Override
  public Time getTime(int columnIndex) throws SFException {
    logger.debug("public Time getTime(int columnIndex)");

    int columnType = resultSetMetaData.getColumnType(columnIndex);
    if (Types.TIME == columnType) {
      SFTime sfTime = getSFTime(columnIndex);
      if (sfTime == null) {
        return null;
      }
      return new SnowflakeTimeWithTimezone(
          sfTime.getFractionalSeconds(ResultUtil.DEFAULT_SCALE_OF_SFTIME_FRACTION_SECONDS),
          sfTime.getNanosecondsWithinSecond(),
          resultSetSerializable.getUseSessionTimezone());
    } else if (Types.TIMESTAMP == columnType || Types.TIMESTAMP_WITH_TIMEZONE == columnType) {
      Timestamp ts = getTimestamp(columnIndex);
      if (ts == null) {
        return null;
      }
      if (resultSetSerializable.getUseSessionTimezone()) {
        ts = getTimestamp(columnIndex, sessionTimeZone);
        TimeZone sessionTimeZone = adjustTimezoneForTimestampTZ(columnIndex);
        return new SnowflakeTimeWithTimezone(
            ts, sessionTimeZone, resultSetSerializable.getUseSessionTimezone());
      }
      return new Time(ts.getTime());
    } else {
      throw new SFException(
          ErrorCode.INVALID_VALUE_CONVERT,
          columnType,
          SnowflakeUtil.TIME_STR,
          getObjectInternal(columnIndex));
    }
  }

  @Override
  public Timestamp getTimestamp(int columnIndex, TimeZone tz) throws SFException {
    int columnType = resultSetMetaData.getColumnType(columnIndex);
    if (Types.TIMESTAMP == columnType || Types.TIMESTAMP_WITH_TIMEZONE == columnType) {
      if (tz == null) {
        tz = TimeZone.getDefault();
      }
      SFTimestamp sfTS = getSFTimestamp(columnIndex);

      if (sfTS == null) {
        return null;
      }
      Timestamp res = sfTS.getTimestamp();
      if (res == null) {
        return null;
      }
      int subType = resultSetMetaData.getInternalColumnType(columnIndex);
      // If we want to display format with no session offset, we have to use session timezone for
      // ltz and tz types but UTC timezone for ntz type.
      if (resultSetSerializable.getUseSessionTimezone()) {
        if (subType == SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_LTZ
            || subType == SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_TZ) {
          TimeZone specificSessionTimezone = adjustTimezoneForTimestampTZ(columnIndex);
          res = new SnowflakeTimestampWithTimezone(res, specificSessionTimezone);
        } else {
          res = new SnowflakeTimestampWithTimezone(res);
        }
      }
      // If timestamp type is NTZ and JDBC_TREAT_TIMESTAMP_NTZ_AS_UTC=true, keep
      // timezone in UTC to avoid daylight savings errors
      else if (resultSetSerializable.getTreatNTZAsUTC()
          && resultSetMetaData.getInternalColumnType(columnIndex) == Types.TIMESTAMP) {
        res = new SnowflakeTimestampWithTimezone(res);
      }
      // If JDBC_TREAT_TIMESTAMP_NTZ_AS_UTC=false, default behavior is to honor
      // client timezone for NTZ time. Move NTZ timestamp offset to correspond to
      // client's timezone. JDBC_USE_SESSION_TIMEZONE overrides other params.
      if (resultSetMetaData.getInternalColumnType(columnIndex) == Types.TIMESTAMP
          && ((!resultSetSerializable.getTreatNTZAsUTC() && honorClientTZForTimestampNTZ)
              || resultSetSerializable.getUseSessionTimezone())) {
        res = sfTS.moveToTimeZone(tz).getTimestamp();
      }
      // Adjust time if date happens before year 1582 for difference between
      // Julian and Gregorian calendars
      return ResultUtil.adjustTimestamp(res);
    } else if (Types.DATE == columnType) {
      Date d = getDate(columnIndex, tz);
      if (d == null) {
        return null;
      }
      return new Timestamp(d.getTime());
    } else if (Types.TIME == columnType) {
      Time t = getTime(columnIndex);
      if (t == null) {
        return null;
      }
      if (resultSetSerializable.getUseSessionTimezone()) {
        SFTime sfTime = getSFTime(columnIndex);
        return new SnowflakeTimestampWithTimezone(
            sfTime.getFractionalSeconds(ResultUtil.DEFAULT_SCALE_OF_SFTIME_FRACTION_SECONDS),
            sfTime.getNanosecondsWithinSecond(),
            TimeZone.getTimeZone("UTC"));
      }
      return new Timestamp(t.getTime());
    } else {
      throw new SFException(
          ErrorCode.INVALID_VALUE_CONVERT,
          columnType,
          SnowflakeUtil.TIMESTAMP_STR,
          getObjectInternal(columnIndex));
    }
  }

  @Override
  public float getFloat(int columnIndex) throws SFException {
    logger.debug("public float getFloat(int columnIndex)");

    // Column index starts from 1, not 0.
    Object obj = getObjectInternal(columnIndex);

    if (obj == null) {
      return 0;
    }

    int columnType = resultSetMetaData.getColumnType(columnIndex);
    try {
      if (obj instanceof String) {
        if (columnType != Types.TIME
            && columnType != Types.TIMESTAMP
            && columnType != Types.TIMESTAMP_WITH_TIMEZONE) {
          if ("inf".equals(obj)) {
            return Float.POSITIVE_INFINITY;
          } else if ("-inf".equals(obj)) {
            return Float.NEGATIVE_INFINITY;
          } else {
            return Float.parseFloat((String) obj);
          }
        }
        throw new SFException(
            ErrorCode.INVALID_VALUE_CONVERT,
            columnType,
            SnowflakeUtil.FLOAT_STR,
            getObjectInternal(columnIndex));
      } else {
        return ((Number) obj).floatValue();
      }
    } catch (NumberFormatException ex) {
      throw new SFException(
          ErrorCode.INVALID_VALUE_CONVERT,
          columnType,
          SnowflakeUtil.FLOAT_STR,
          getObjectInternal(columnIndex));
    }
  }

  @Override
  public double getDouble(int columnIndex) throws SFException {
    logger.debug("public double getDouble(int columnIndex)");

    // Column index starts from 1, not 0.
    Object obj = getObjectInternal(columnIndex);

    // snow-11974: null for getDouble should return 0
    if (obj == null) {
      return 0;
    }
    int columnType = resultSetMetaData.getColumnType(columnIndex);
    try {
      if (obj instanceof String) {
        if (columnType != Types.TIME
            && columnType != Types.TIMESTAMP
            && columnType != Types.TIMESTAMP_WITH_TIMEZONE) {
          if ("inf".equals(obj)) {
            return Double.POSITIVE_INFINITY;
          } else if ("-inf".equals(obj)) {
            return Double.NEGATIVE_INFINITY;
          } else {
            return Double.parseDouble((String) obj);
          }
        }
        throw new SFException(
            ErrorCode.INVALID_VALUE_CONVERT,
            columnType,
            SnowflakeUtil.DOUBLE_STR,
            getObjectInternal(columnIndex));
      } else {
        return ((Number) obj).doubleValue();
      }
    } catch (NumberFormatException ex) {
      throw new SFException(
          ErrorCode.INVALID_VALUE_CONVERT,
          columnType,
          SnowflakeUtil.DOUBLE_STR,
          getObjectInternal(columnIndex));
    }
  }

  @Override
  public byte[] getBytes(int columnIndex) throws SFException {
    logger.debug("public byte[] getBytes(int columnIndex)");

    // Column index starts from 1, not 0.
    Object obj = getObjectInternal(columnIndex);
    int columnType = resultSetMetaData.getColumnType(columnIndex);

    if (obj == null) {
      return null;
    }

    try {
      // For all types except time/date/timestamp data, convert data into byte array. Different
      // methods are needed
      // for different types.
      switch (columnType) {
        case Types.FLOAT:
        case Types.DOUBLE:
          return ByteBuffer.allocate(Float8Vector.TYPE_WIDTH)
              .putDouble(0, getDouble(columnIndex))
              .array();
        case Types.NUMERIC:
        case Types.INTEGER:
        case Types.SMALLINT:
        case Types.TINYINT:
        case Types.BIGINT:
          return getBigDecimal(columnIndex).toBigInteger().toByteArray();
        case Types.VARCHAR:
        case Types.CHAR:
          return getString(columnIndex).getBytes();
        case Types.BOOLEAN:
          return getBoolean(columnIndex) ? new byte[] {1} : new byte[] {0};
        case Types.TIMESTAMP:
        case Types.TIME:
        case Types.DATE:
        case Types.DECIMAL:
          throw new SFException(
              ErrorCode.INVALID_VALUE_CONVERT,
              columnType,
              SnowflakeUtil.BYTES_STR,
              getObjectInternal(columnIndex));
        default:
          return SFBinary.fromHex(obj.toString()).getBytes();
      }
    } catch (IllegalArgumentException ex) {
      throw new SFException(
          ErrorCode.INVALID_VALUE_CONVERT,
          columnType,
          SnowflakeUtil.BYTES_STR,
          getObjectInternal(columnIndex));
    }
  }

  public Date getDate(int columnIndex) throws SFException {
    return getDate(columnIndex, TimeZone.getDefault());
  }

  @Override
  public Date getDate(int columnIndex, TimeZone tz) throws SFException {
    logger.debug("public Date getDate(int columnIndex)");

    // Column index starts from 1, not 0.
    Object obj = getObjectInternal(columnIndex);

    if (obj == null) {
      return null;
    }

    int columnType = resultSetMetaData.getColumnType(columnIndex);

    if (Types.TIMESTAMP == columnType || Types.TIMESTAMP_WITH_TIMEZONE == columnType) {
      if (tz == null) {
        tz = TimeZone.getDefault();
      }
      int subType = resultSetMetaData.getInternalColumnType(columnIndex);
      if (subType == SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_TZ
          || subType == SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_LTZ) {
        TimeZone specificSessionTimeZone = adjustTimezoneForTimestampTZ(columnIndex);
        return new SnowflakeDateWithTimezone(
            getTimestamp(columnIndex, tz).getTime(),
            specificSessionTimeZone,
            resultSetSerializable.getUseSessionTimezone());
      }
      return new Date(getTimestamp(columnIndex, tz).getTime());

    } else if (Types.DATE == columnType) {
      if (tz == null || !resultSetSerializable.getFormatDateWithTimeZone()) {
        return ArrowResultUtil.getDate(Integer.parseInt((String) obj));
      }
      return ArrowResultUtil.getDate(Integer.parseInt((String) obj), tz, sessionTimeZone);
    }
    // for Types.TIME and all other type, throw user error
    else {
      throw new SFException(
          ErrorCode.INVALID_VALUE_CONVERT, columnType, SnowflakeUtil.DATE_STR, obj);
    }
  }

  private SFTime getSFTime(int columnIndex) throws SFException {
    Object obj = getObjectInternal(columnIndex);

    if (obj == null) {
      return null;
    }

    int scale = resultSetMetaData.getScale(columnIndex);
    return ResultUtil.getSFTime(obj.toString(), scale, session);
  }

  private Timestamp getTimestamp(int columnIndex) throws SFException {
    return getTimestamp(columnIndex, TimeZone.getDefault());
  }
}
