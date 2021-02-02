/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import java.math.BigDecimal;
import java.sql.*;
import java.text.DateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import net.snowflake.client.core.SFBaseSession;
import net.snowflake.common.core.SFBinary;
import net.snowflake.common.core.SqlState;

/** Type converters */
public enum SnowflakeType {
  ANY,
  ARRAY,
  BINARY,
  BOOLEAN,
  CHAR,
  DATE,
  FIXED,
  INTEGER,
  OBJECT,
  REAL,
  TEXT,
  TIME,
  TIMESTAMP,
  TIMESTAMP_LTZ,
  TIMESTAMP_NTZ,
  TIMESTAMP_TZ,
  VARIANT,
  GEOGRAPHY;

  public static final String DATE_OR_TIME_FORMAT_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";
  public static final String TIMESTAMP_FORMAT_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.";
  public static final String TIMESTAMP_FORMAT_TZ_PATTERN = "XXX";
  public static final String TIME_FORMAT_PATTERN = "HH:mm:ss.SSS";
  private static final byte[] BYTE_ARRAY = new byte[0];
  public static final String BINARY_CLASS_NAME = BYTE_ARRAY.getClass().getName();

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

  /** Converts text of data type (returned from SQL query) into Types type, represented by an int */
  public static int convertStringToType(String typeName) {
    int retval = Types.NULL;
    if (typeName == null || typeName.trim().isEmpty()) {
      return retval;
    }
    // Trim all whitespace and extra information off typeName so it can be interpreted by switch
    // statement. Ex: turns
    // "NUMBER(38,0)" -> "NUMBER" and "FLOAT NOT NULL" ->" FLOAT"
    String typeNameTrimmed = typeName.trim();
    if (typeNameTrimmed.contains("(")) {
      typeNameTrimmed = typeNameTrimmed.substring(0, typeNameTrimmed.indexOf('('));
    }
    if (typeNameTrimmed.contains(" ")) {
      typeNameTrimmed = typeNameTrimmed.substring(0, typeNameTrimmed.indexOf(' '));
    }
    switch (typeNameTrimmed.toLowerCase()) {
      case "number":
      case "numeric":
        retval = Types.NUMERIC;
        break;
      case "decimal":
        retval = Types.DECIMAL;
        break;
      case "int":
      case "integer":
      case "byteint":
        retval = Types.INTEGER;
        break;
      case "tinyint":
        retval = Types.TINYINT;
        break;
      case "smallint":
        retval = Types.SMALLINT;
        break;
      case "bigint":
        retval = Types.BIGINT;
        break;
      case "float":
      case "float4":
      case "float8":
        retval = Types.FLOAT;
        break;
      case "double":
      case "double precision":
        retval = Types.DOUBLE;
        break;
      case "real":
        retval = Types.REAL;
        break;
      case "char":
      case "character":
        retval = Types.CHAR;
        break;
      case "varchar":
      case "string":
      case "text":
        retval = Types.VARCHAR;
        break;
      case "binary":
        retval = Types.BINARY;
        break;
      case "varbinary":
        retval = Types.VARBINARY;
        break;
      case "boolean":
        retval = Types.BOOLEAN;
        break;
      case "date":
        retval = Types.DATE;
        break;
      case "time":
        retval = Types.TIME;
        break;
      case "timestamp":
      case "datetime":
      case "timestamp_ntz":
        retval = Types.TIMESTAMP;
        break;
      case "timestamp_ltz":
      case "timestamp_tz":
        retval = Types.TIMESTAMP_WITH_TIMEZONE;
        break;
      case "variant":
        retval = Types.OTHER;
        break;
      case "object":
        retval = Types.JAVA_OBJECT;
        break;
      case "array":
        retval = Types.ARRAY;
        break;
      default:
        retval = Types.OTHER;
        break;
    }
    return retval;
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

    JavaDataType(Class<?> c) {
      this._class = c;
    }

    private Class<?> _class;
  }

  public enum JavaSQLType {
    ARRAY(Types.ARRAY),
    DATALINK(Types.DATALINK),
    BIGINT(Types.BIGINT),
    BINARY(Types.BINARY),
    BIT(Types.BIT),
    BLOB(Types.BLOB),
    BOOLEAN(Types.BOOLEAN),
    CHAR(Types.CHAR),
    CLOB(Types.CLOB),
    DATE(Types.DATE),
    DECIMAL(Types.DECIMAL),
    DISTINCT(Types.DISTINCT),
    DOUBLE(Types.DOUBLE),
    FLOAT(Types.FLOAT),
    INTEGER(Types.INTEGER),
    JAVA_OBJECT(Types.JAVA_OBJECT),
    LONGNVARCHAR(Types.LONGNVARCHAR),
    LONGVARBINARY(Types.LONGVARBINARY),
    LONGVARCHAR(Types.LONGVARCHAR),
    NCHAR(Types.NCHAR),
    NCLOB(Types.NCLOB),
    NULL(Types.NULL),
    NUMERIC(Types.NUMERIC),
    NVARCHAR(Types.NVARCHAR),
    OTHER(Types.OTHER),
    REAL(Types.REAL),
    REF(Types.REF),
    REF_CURSOR(Types.REF_CURSOR),
    ROWID(Types.ROWID),
    SMALLINT(Types.SMALLINT),
    SQLXML(Types.SQLXML),
    STRUCT(Types.STRUCT),
    TIME(Types.TIME),
    TIME_WITH_TIMEZONE(Types.TIME_WITH_TIMEZONE),
    TIMESTAMP(Types.TIMESTAMP),
    TIMESTAMP_WITH_TIMEZONE(Types.TIMESTAMP_WITH_TIMEZONE),
    TINYINT(Types.TINYINT),
    VARBINARY(Types.VARBINARY),
    VARCHAR(Types.VARCHAR);

    private final int type;
    public static final Set<JavaSQLType> ALL_TYPES = new HashSet<>();

    static {
      ALL_TYPES.add(ARRAY);
      ALL_TYPES.add(DATALINK);
      ALL_TYPES.add(BIGINT);
      ALL_TYPES.add(BINARY);
      ALL_TYPES.add(BIT);
      ALL_TYPES.add(BLOB);
      ALL_TYPES.add(BOOLEAN);
      ALL_TYPES.add(CHAR);
      ALL_TYPES.add(CLOB);
      ALL_TYPES.add(DATE);
      ALL_TYPES.add(DECIMAL);
      ALL_TYPES.add(DISTINCT);
      ALL_TYPES.add(DOUBLE);
      ALL_TYPES.add(FLOAT);
      ALL_TYPES.add(INTEGER);
      ALL_TYPES.add(JAVA_OBJECT);
      ALL_TYPES.add(LONGNVARCHAR);
      ALL_TYPES.add(LONGVARBINARY);
      ALL_TYPES.add(LONGVARCHAR);
      ALL_TYPES.add(NCHAR);
      ALL_TYPES.add(NCLOB);
      ALL_TYPES.add(NULL);
      ALL_TYPES.add(NUMERIC);
      ALL_TYPES.add(NVARCHAR);
      ALL_TYPES.add(OTHER);
      ALL_TYPES.add(REAL);
      ALL_TYPES.add(REF);
      ALL_TYPES.add(REF_CURSOR);
      ALL_TYPES.add(ROWID);
      ALL_TYPES.add(SMALLINT);
      ALL_TYPES.add(SQLXML);
      ALL_TYPES.add(STRUCT);
      ALL_TYPES.add(TIME);
      ALL_TYPES.add(TIME_WITH_TIMEZONE);
      ALL_TYPES.add(TIMESTAMP);
      ALL_TYPES.add(TIMESTAMP_WITH_TIMEZONE);
      ALL_TYPES.add(TINYINT);
      ALL_TYPES.add(VARBINARY);
      ALL_TYPES.add(VARCHAR);
    }

    JavaSQLType(int type) {
      this.type = type;
    }

    int getType() {
      return type;
    }

    public static JavaSQLType find(int type) {
      for (JavaSQLType t : ALL_TYPES) {
        if (t.type == type) {
          return t;
        }
      }
      return null;
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

  public static SnowflakeType javaTypeToSFType(int javaType, SFBaseSession session)
      throws SnowflakeSQLException {

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
        throw new SnowflakeSQLLoggedException(
            session,
            ErrorCode.DATA_TYPE_NOT_SUPPORTED.getMessageCode(),
            SqlState.FEATURE_NOT_SUPPORTED,
            javaType);
    }
  }

  public static String javaTypeToClassName(int type) throws SQLException {
    switch (type) {
      case Types.VARCHAR:
      case Types.CHAR:
        return String.class.getName();

      case Types.BINARY:
        return BINARY_CLASS_NAME;

      case Types.INTEGER:
        return Integer.class.getName();

      case Types.DECIMAL:
        return BigDecimal.class.getName();

      case Types.DOUBLE:
        return Double.class.getName();

      case Types.TIMESTAMP:
        return Timestamp.class.getName();

      case Types.DATE:
        return java.sql.Date.class.getName();

      case Types.TIME:
        return Time.class.getName();

      case Types.BOOLEAN:
        return Boolean.class.getName();

      case Types.BIGINT:
        return Long.class.getName();

      case Types.SMALLINT:
        return Short.class.getName();

      default:
        throw new SQLFeatureNotSupportedException(
            String.format("No corresponding Java type is found for java.sql.Type: %d", type));
    }
  }

  public static boolean isJavaTypeSigned(int type) {
    return type == Types.INTEGER || type == Types.DECIMAL || type == Types.DOUBLE;
  }
}
