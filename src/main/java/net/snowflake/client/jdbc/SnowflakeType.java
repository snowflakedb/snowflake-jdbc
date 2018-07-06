/*
 * Copyright (c) 2012-2018 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import net.snowflake.common.core.SFBinary;
import net.snowflake.common.core.SqlState;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

/**
 * Type converters
 */
public enum SnowflakeType {

  TEXT, CHAR, INTEGER, FIXED, REAL, TIMESTAMP, TIMESTAMP_LTZ, TIMESTAMP_NTZ, TIMESTAMP_TZ,
  DATE, TIME, BOOLEAN, ARRAY, OBJECT, VARIANT, BINARY, ANY;

  private static final Object LOCK_CALENDAR = new Object();
  private static GregorianCalendar CALENDAR_LOCAL;

  /**
   * Resets local calendar with the default timezone. This is used in tests.
   */
  public static void resetLocalCalendar() {
    CALENDAR_LOCAL = new GregorianCalendar(TimeZone.getDefault());
    CALENDAR_LOCAL.clear();
  }

  private final static GregorianCalendar CALENDAR_UTC = new GregorianCalendar(
      TimeZone.getTimeZone("UTC"));

  static {
    // no need to adjust date before 1582 because JDBC will adjust
    // when fetching data from server side
    CALENDAR_UTC.clear();
    resetLocalCalendar();
  }

  private static final String TIMESTAMP_FORMAT_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";
  private static final String TIME_FORMAT_PATTERN = "HH:MM:ss.SSS";

  public static SnowflakeType fromString(String name) {
    return SnowflakeType.valueOf(name.toUpperCase());
  }

  public static JavaDataType getJavaType(SnowflakeType type) {
    switch (type) {
      case TEXT:
        return JavaDataType.JAVA_STRING;
      case CHAR:
        return JavaDataType.JAVA_STRING;
      case INTEGER:
        return JavaDataType.JAVA_LONG;
      case FIXED:
        return JavaDataType.JAVA_BIGDECIMAL;
      case REAL:
        return JavaDataType.JAVA_DOUBLE;
      case TIMESTAMP:
      case TIME:
      case TIMESTAMP_LTZ:
      case TIMESTAMP_NTZ:
      case TIMESTAMP_TZ:
      case DATE:
        return JavaDataType.JAVA_TIMESTAMP;
      case BOOLEAN:
        return JavaDataType.JAVA_BOOLEAN;
      case ARRAY:
      case VARIANT:
        return JavaDataType.JAVA_STRING;
      case OBJECT:
        return JavaDataType.JAVA_STRING;
      case BINARY:
        return JavaDataType.JAVA_BYTES;
      case ANY:
        return JavaDataType.JAVA_OBJECT;
      default:
        // Those are not supported, but no reason to panic
        return JavaDataType.JAVA_STRING;
    }
  }

  public enum JavaDataType {

    JAVA_STRING(String.class),
    JAVA_LONG(Long.class),
    JAVA_DOUBLE(Double.class),
    JAVA_BIGDECIMAL(BigDecimal.class),
    JAVA_TIMESTAMP(Timestamp.class),
    JAVA_BYTES(byte[].class),
    JAVA_BOOLEAN(Boolean.class),
    JAVA_OBJECT(Object.class);

    JavaDataType(Class c) {
      this._class = c;
    }

    private Class _class;

    public Class getClazz() {
      return _class;
    }

    public String lexicalValue(Object o) {
      if (o == null)
      {
        return null;
      }
      switch (this) {
        case JAVA_STRING:
        case JAVA_BIGDECIMAL:
          return (String) o;
        case JAVA_LONG:
          return Long.toString((Long) o);
        case JAVA_DOUBLE:
          return Double.toHexString((Double) o);
        case JAVA_TIMESTAMP:
          SimpleDateFormat sdf = new SimpleDateFormat(TIMESTAMP_FORMAT_PATTERN);
          return sdf.format((Timestamp) o);
        case JAVA_BYTES:
          return new SFBinary((byte[]) o).toHex();
        case JAVA_BOOLEAN:
          return Boolean.toString((Boolean) o);
        case JAVA_OBJECT:
          return o.toString();

        default:
          throw new RuntimeException("Invalid method");
      }
    }
  }

  /**
   * Returns a lexical value of an object that is suitable for Snowflake import
   * serialization
   *
   * @param o                  Java object representing value in Snowflake.
   * @param useLocalTimezone   use local timezone instead of UTC.
   * @param mapTimeToTimestamp map TIME to TIMESTAMP.
   * @return String representation of it that can be used for creating a load file
   */
  public static String lexicalValue(
      Object o, boolean useLocalTimezone, boolean mapTimeToTimestamp) {
    if (o == null)
    {
      return null;
    }

    Class c = o.getClass();

    if (Date.class.isAssignableFrom(c)) {
      String fmt = TIMESTAMP_FORMAT_PATTERN;
      if (!mapTimeToTimestamp && c == java.sql.Time.class) {
        fmt = TIME_FORMAT_PATTERN;
      }
      SimpleDateFormat sdf = new SimpleDateFormat(fmt);
      if (!useLocalTimezone) {
        sdf.setCalendar(CALENDAR_UTC);
      } else {
        sdf.setCalendar(CALENDAR_LOCAL);
      }
      synchronized(LOCK_CALENDAR)
      {
        return sdf.format(o);
      }
    }

    if (c == Double.class) {
      return Double.toHexString((Double) o);
    }

    if (c == Float.class) {
      return Float.toHexString((Float) o);
    }

    if (c == Integer.class) {
      return o.toString();
    }

    if (c == BigDecimal.class) {
      return o.toString();
    }

    if (c == byte[].class) {
      return new SFBinary((byte[]) o).toHex();
    }

    String value = String.valueOf(o);

    return value;
  }

  public static String escapeForCSV(String value) {
    if (value == null)
    {
      return ""; // null => an empty string without quotes
    }
    if (value.isEmpty())
    {
      return "\"\""; // an empty string => an empty string with quotes
    }
    if (value.indexOf('"') >= 0 || value.indexOf('\n') >= 0
        || value.indexOf(',') >= 0 || value.indexOf('\\') >= 0)
    {
      // anything else including double quotes or commas will have quotes
      return '"' + value.replaceAll("\"", "\"\"") + '"';
    }
    else
    {
      return value;
    }
  }

  public static SnowflakeType javaTypeToSFType(int javaType) throws SnowflakeSQLException {

    switch (javaType) {
      case Types.INTEGER:
      case Types.BIGINT:
      case Types.DECIMAL:
      case Types.NUMERIC:
      case Types.SMALLINT:
      case Types.TINYINT:
        return FIXED;

      case Types.CHAR:
      case Types.VARCHAR:
        return TEXT;

      case Types.BINARY:
        return BINARY;

      case Types.FLOAT:
      case Types.DOUBLE:
        return REAL;

      case Types.DATE:
        return DATE;

      case Types.TIME:
        return TIME;

      case Types.TIMESTAMP:
        return TIMESTAMP;

      case Types.BOOLEAN:
        return BOOLEAN;

      case Types.NULL:
        return ANY;

      default:
        throw new SnowflakeSQLException(SqlState.FEATURE_NOT_SUPPORTED,
            ErrorCode.DATA_TYPE_NOT_SUPPORTED
                .getMessageCode(),
            javaType);
    }
  }
}
