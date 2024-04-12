package net.snowflake.client.core.json;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Types;
import net.snowflake.client.core.SFException;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeUtil;

public class NumberConverter {
  // Precision of maximum long value in Java (2^63-1). Precision is 19
  private static final int LONG_PRECISION = 19;

  private static final BigDecimal MAX_LONG_VAL = new BigDecimal(Long.MAX_VALUE);
  private static final BigDecimal MIN_LONG_VAL = new BigDecimal(Long.MIN_VALUE);

  public byte getByte(Object obj) {
    if (obj == null) {
      return 0;
    }

    if (obj instanceof String) {
      return Byte.parseByte((String) obj);
    } else {
      return ((Number) obj).byteValue();
    }
  }

  public short getShort(Object obj, int columnType) throws SFException {
    if (obj == null) {
      return 0;
    }
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

  public int getInt(Object obj, int columnType) throws SFException {
    if (obj == null) {
      return 0;
    }
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

  public long getLong(Object obj, int columnType) throws SFException {
    if (obj == null) {
      return 0;
    }
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
        throw new SFException(
            ErrorCode.INTERNAL_ERROR, SnowflakeUtil.LONG_STR + ": " + obj.toString());
      } else {
        throw new SFException(
            ErrorCode.INVALID_VALUE_CONVERT, columnType, SnowflakeUtil.LONG_STR, obj);
      }
    }
  }

  public BigDecimal getBigDecimal(Object obj, int columnType) throws SFException {
    if (obj == null) {
      return null;
    }
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

  public BigDecimal getBigDecimal(Object obj, int columnType, Integer scale) throws SFException {
    if (obj == null) {
      return null;
    }
    BigDecimal value = getBigDecimal(obj.toString(), columnType);
    value = value.setScale(scale, RoundingMode.HALF_UP);
    return value;
  }

  public float getFloat(Object obj, int columnType) throws SFException {
    if (obj == null) {
      return 0;
    }

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
            ErrorCode.INVALID_VALUE_CONVERT, columnType, SnowflakeUtil.FLOAT_STR, obj);
      } else {
        return ((Number) obj).floatValue();
      }
    } catch (NumberFormatException ex) {
      throw new SFException(
          ErrorCode.INVALID_VALUE_CONVERT, columnType, SnowflakeUtil.FLOAT_STR, obj);
    }
  }

  public double getDouble(Object obj, int columnType) throws SFException {
    if (obj == null) {
      return 0;
    }
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
            ErrorCode.INVALID_VALUE_CONVERT, columnType, SnowflakeUtil.DOUBLE_STR, obj);
      } else {
        return ((Number) obj).doubleValue();
      }
    } catch (NumberFormatException ex) {
      throw new SFException(
          ErrorCode.INVALID_VALUE_CONVERT, columnType, SnowflakeUtil.DOUBLE_STR, obj);
    }
  }

  public Object getBigInt(Object obj, int columnType) throws SFException {
    // If precision is < precision of max long precision, we can automatically convert to long.
    // Otherwise, do a check to ensure it doesn't overflow max long value.
    if (obj == null) {
      return null;
    }
    String numberAsString = obj.toString();
    if (numberAsString.length() >= LONG_PRECISION) {
      BigDecimal bigNum = getBigDecimal(obj, columnType);
      if (bigNum.compareTo(MAX_LONG_VAL) == 1 || bigNum.compareTo(MIN_LONG_VAL) == -1) {
        return bigNum;
      }
    }
    return getLong(obj, columnType);
  }
}
