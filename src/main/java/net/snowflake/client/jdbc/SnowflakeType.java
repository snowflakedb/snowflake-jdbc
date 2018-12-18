/*
 * Copyright (c) 2012-2018 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import net.snowflake.common.core.SFBinary;
import net.snowflake.common.core.SqlState;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.DateFormat;
import java.util.Date;

/**
 * Type converters
 */
public enum SnowflakeType
{

  TEXT, CHAR, INTEGER, FIXED, REAL, TIMESTAMP, TIMESTAMP_LTZ, TIMESTAMP_NTZ, TIMESTAMP_TZ,
  DATE, TIME, BOOLEAN, ARRAY, OBJECT, VARIANT, BINARY, ANY;

  public static final String DATE_OR_TIME_FORMAT_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";
  public static final String TIMESTAMP_FORMAT_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.";
  public static final String TIMESTAMP_FORMAT_TZ_PATTERN = "XXX";
  public static final String TIME_FORMAT_PATTERN = "HH:mm:ss.SSS";

  public static SnowflakeType fromString(String name)
  {
    return SnowflakeType.valueOf(name.toUpperCase());
  }

  public static JavaDataType getJavaType(SnowflakeType type)
  {
    switch (type)
    {
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

  public enum JavaDataType
  {

    JAVA_STRING(String.class),
    JAVA_LONG(Long.class),
    JAVA_DOUBLE(Double.class),
    JAVA_BIGDECIMAL(BigDecimal.class),
    JAVA_TIMESTAMP(Timestamp.class),
    JAVA_BYTES(byte[].class),
    JAVA_BOOLEAN(Boolean.class),
    JAVA_OBJECT(Object.class);

    JavaDataType(Class c)
    {
      this._class = c;
    }

    private Class _class;
  }

  /**
   * Returns a lexical value of an object that is suitable for Snowflake import
   * serialization
   *
   * @param o                 Java object representing value in Snowflake.
   * @param dateFormat        java.sql.Date or java.sqlTime format
   * @param timeFormat        java.sql.Time format
   * @param timestampFormat   first part of java.sql.Timestamp format
   * @param timestampTzFormat last part of java.sql.Timestamp format
   * @return String representation of it that can be used for creating a load file
   */
  public static String lexicalValue(
      Object o,
      DateFormat dateFormat,
      DateFormat timeFormat,
      DateFormat timestampFormat,
      DateFormat timestampTzFormat)
  {
    if (o == null)
    {
      return null;
    }

    Class c = o.getClass();

    if (c == Date.class || c == java.sql.Date.class)
    {
      return synchronizeFormat(o, dateFormat);
    }

    if (c == java.sql.Time.class)
    {
      return synchronizeFormat(o, timeFormat);
    }

    if (c == java.sql.Timestamp.class)
    {
      String stdFmt = o.toString();
      String nanos = stdFmt.substring(stdFmt.indexOf('.') + 1);
      String ret1 = synchronizeFormat(o, timestampFormat);
      String ret2 = synchronizeFormat(o, timestampTzFormat);
      return ret1 + nanos + ret2;
    }
    if (c == Double.class)
    {
      return Double.toHexString((Double) o);
    }

    if (c == Float.class)
    {
      return Float.toHexString((Float) o);
    }

    if (c == Integer.class)
    {
      return o.toString();
    }

    if (c == BigDecimal.class)
    {
      return o.toString();
    }

    if (c == byte[].class)
    {
      return new SFBinary((byte[]) o).toHex();
    }

    return String.valueOf(o);
  }

  private static synchronized String synchronizeFormat(
      Object o, DateFormat sdf)
  {
    return sdf.format(o);
  }

  public static String escapeForCSV(String value)
  {
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

  public static SnowflakeType javaTypeToSFType(int javaType) throws SnowflakeSQLException
  {

    switch (javaType)
    {
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
