package net.snowflake.client.internal.jdbc.util;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.time.Duration;
import java.time.Period;
import java.util.Date;
import net.snowflake.client.api.exception.ErrorCode;
import net.snowflake.client.api.resultset.SnowflakeType;
import net.snowflake.client.internal.common.core.SFBinary;
import net.snowflake.client.internal.core.SFBaseSession;
import net.snowflake.client.internal.exception.SnowflakeSQLLoggedException;
import net.snowflake.common.core.SqlState;

/**
 * Internal utility class for SnowflakeType conversions and formatting. These methods are used
 * internally by the driver and should not be considered part of the public API.
 */
public class SnowflakeTypeUtil {

  /**
   * Converts a string to a SnowflakeType enum value.
   *
   * @param name the type name
   * @return the corresponding SnowflakeType
   */
  public static SnowflakeType fromString(String name) {
    return SnowflakeType.valueOf(name.toUpperCase());
  }

  /**
   * Gets the Java data type for a Snowflake type.
   *
   * @param type the Snowflake type
   * @return the corresponding Java data type
   */
  public static SnowflakeTypeHelper.JavaDataType getJavaType(SnowflakeType type) {
    return getJavaType(type, false);
  }

  /**
   * Gets the Java data type for a Snowflake type.
   *
   * @param type the Snowflake type
   * @param isStructuredType whether this is a structured type
   * @return the corresponding Java data type
   */
  public static SnowflakeTypeHelper.JavaDataType getJavaType(
      SnowflakeType type, boolean isStructuredType) {
    // TODO structuredType fill for Array and Map: SNOW-1234216, SNOW-1234214
    switch (type) {
      case TEXT:
        return SnowflakeTypeHelper.JavaDataType.JAVA_STRING;
      case CHAR:
        return SnowflakeTypeHelper.JavaDataType.JAVA_STRING;
      case INTEGER:
        return SnowflakeTypeHelper.JavaDataType.JAVA_LONG;
      case FIXED:
      case DECFLOAT:
        return SnowflakeTypeHelper.JavaDataType.JAVA_BIGDECIMAL;
      case REAL:
        return SnowflakeTypeHelper.JavaDataType.JAVA_DOUBLE;
      case TIMESTAMP:
      case TIME:
      case TIMESTAMP_LTZ:
      case TIMESTAMP_NTZ:
      case TIMESTAMP_TZ:
      case DATE:
        return SnowflakeTypeHelper.JavaDataType.JAVA_TIMESTAMP;
      case INTERVAL_YEAR_MONTH:
        return SnowflakeTypeHelper.JavaDataType.JAVA_PERIOD;
      case INTERVAL_DAY_TIME:
        return SnowflakeTypeHelper.JavaDataType.JAVA_DURATION;
      case BOOLEAN:
        return SnowflakeTypeHelper.JavaDataType.JAVA_BOOLEAN;
      case ARRAY:
      case VARIANT:
      case VECTOR:
        return SnowflakeTypeHelper.JavaDataType.JAVA_STRING;
      case BINARY:
        return SnowflakeTypeHelper.JavaDataType.JAVA_BYTES;
      case ANY:
        return SnowflakeTypeHelper.JavaDataType.JAVA_OBJECT;
      case OBJECT:
        if (isStructuredType) {
          return SnowflakeTypeHelper.JavaDataType.JAVA_OBJECT;
        } else {
          return SnowflakeTypeHelper.JavaDataType.JAVA_STRING;
        }
      default:
        // Those are not supported, but no reason to panic
        return SnowflakeTypeHelper.JavaDataType.JAVA_STRING;
    }
  }

  /**
   * Returns a lexical value of an object that is suitable for Snowflake import serialization
   *
   * @param o Java object representing value in Snowflake.
   * @param dateFormat java.sql.Date or java.sqlTime format
   * @param timeFormat java.sql.Time format
   * @param timestampFormat first part of java.sql.Timestamp format
   * @param timestampTzFormat last part of java.sql.Timestamp format
   * @return String representation of it that can be used for creating a load file
   */
  public static String lexicalValue(
      Object o,
      DateFormat dateFormat,
      DateFormat timeFormat,
      DateFormat timestampFormat,
      DateFormat timestampTzFormat) {
    if (o == null) {
      return null;
    }

    Class<?> c = o.getClass();

    if (c == Date.class || c == java.sql.Date.class) {
      return synchronizeFormat(o, dateFormat);
    }

    if (c == java.sql.Time.class) {
      return synchronizeFormat(o, timeFormat);
    }

    if (c == java.sql.Timestamp.class) {
      String stdFmt = o.toString();
      String nanos = stdFmt.substring(stdFmt.indexOf('.') + 1);
      String ret1 = synchronizeFormat(o, timestampFormat);
      String ret2 = synchronizeFormat(o, timestampTzFormat);
      return ret1 + nanos + ret2;
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

    if (c == Period.class) {
      return o.toString();
    }

    if (c == Duration.class) {
      return o.toString();
    }

    if (c == BigDecimal.class) {
      return o.toString();
    }

    if (c == byte[].class) {
      return new SFBinary((byte[]) o).toHex();
    }

    return String.valueOf(o);
  }

  private static synchronized String synchronizeFormat(Object o, DateFormat sdf) {
    return sdf.format(o);
  }

  /**
   * Escapes a string value for CSV format.
   *
   * @param value the value to escape
   * @return the escaped value
   */
  public static String escapeForCSV(String value) {
    if (value == null) {
      return ""; // null => an empty string without quotes
    }
    if (value.isEmpty()) {
      return "\"\""; // an empty string => an empty string with quotes
    }
    if (value.indexOf('"') >= 0
        || value.indexOf('\n') >= 0
        || value.indexOf(',') >= 0
        || value.indexOf('\\') >= 0) {
      // anything else including double quotes or commas will have quotes
      return '"' + value.replaceAll("\"", "\"\"") + '"';
    } else {
      return value;
    }
  }

  /**
   * Converts a Java SQL type to a Snowflake type.
   *
   * @param javaType the Java SQL type (from {@link java.sql.Types})
   * @param session the session object
   * @return the corresponding Snowflake type
   * @throws net.snowflake.client.api.exception.SnowflakeSQLException if the type is not supported
   */
  public static SnowflakeType javaTypeToSFType(int javaType, SFBaseSession session)
      throws net.snowflake.client.api.exception.SnowflakeSQLException {

    switch (javaType) {
      case java.sql.Types.INTEGER:
      case java.sql.Types.BIGINT:
      case java.sql.Types.DECIMAL:
      case java.sql.Types.NUMERIC:
      case java.sql.Types.SMALLINT:
      case java.sql.Types.TINYINT:
        return SnowflakeType.FIXED;

      case java.sql.Types.CHAR:
      case java.sql.Types.VARCHAR:
        return SnowflakeType.TEXT;

      case java.sql.Types.BINARY:
        return SnowflakeType.BINARY;

      case java.sql.Types.FLOAT:
      case java.sql.Types.DOUBLE:
        return SnowflakeType.REAL;

      case java.sql.Types.DATE:
        return SnowflakeType.DATE;

      case java.sql.Types.TIME:
        return SnowflakeType.TIME;

      case java.sql.Types.TIMESTAMP:
        return SnowflakeType.TIMESTAMP;

      case java.sql.Types.BOOLEAN:
        return SnowflakeType.BOOLEAN;

      case java.sql.Types.STRUCT:
        return SnowflakeType.OBJECT;

      case java.sql.Types.ARRAY:
        return SnowflakeType.ARRAY;

      case java.sql.Types.NULL:
        return SnowflakeType.ANY;

      default:
        throw new SnowflakeSQLLoggedException(
            session,
            ErrorCode.DATA_TYPE_NOT_SUPPORTED.getMessageCode(),
            SqlState.FEATURE_NOT_SUPPORTED,
            javaType);
    }
  }

  /**
   * Converts a Java SQL type to a Java class name.
   *
   * @param type the Java SQL type (from {@link java.sql.Types})
   * @return the corresponding Java class name
   * @throws SQLException if the type is not supported
   */
  public static String javaTypeToClassName(int type) throws SQLException {
    switch (type) {
      case java.sql.Types.VARCHAR:
      case java.sql.Types.CHAR:
      case java.sql.Types.STRUCT:
      case java.sql.Types.ARRAY:
        return String.class.getName();

      case java.sql.Types.BINARY:
        return SnowflakeTypeHelper.BINARY_CLASS_NAME;

      case java.sql.Types.INTEGER:
        return Integer.class.getName();

      case java.sql.Types.DECIMAL:
        return BigDecimal.class.getName();

      case java.sql.Types.DOUBLE:
        return Double.class.getName();

      case java.sql.Types.TIMESTAMP:
      case java.sql.Types.TIMESTAMP_WITH_TIMEZONE:
        return Timestamp.class.getName();

      case java.sql.Types.DATE:
        return java.sql.Date.class.getName();

      case java.sql.Types.TIME:
        return Time.class.getName();

      case java.sql.Types.BOOLEAN:
        return Boolean.class.getName();

      case java.sql.Types.BIGINT:
        return Long.class.getName();

      case java.sql.Types.SMALLINT:
        return Short.class.getName();

      default:
        throw new java.sql.SQLFeatureNotSupportedException(
            String.format("No corresponding Java type is found for java.sql.Type: %d", type));
    }
  }
}
