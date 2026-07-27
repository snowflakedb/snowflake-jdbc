package net.snowflake.client.internal.jdbc.util;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.time.Period;
import java.util.HashSet;
import java.util.Set;
import net.snowflake.client.api.resultset.SnowflakeType;

/**
 * Internal helper class for SnowflakeType conversions and utilities. This class contains nested
 * enums, constants, and utility methods that are used internally by the JDBC driver but should not
 * be part of the public API.
 *
 * <p><b>Note:</b> This is an internal API and should not be used by customers.
 */
public final class SnowflakeTypeHelper {

  private SnowflakeTypeHelper() {
    // Prevent instantiation
  }

  public static final String DATE_OR_TIME_FORMAT_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";
  public static final String TIMESTAMP_FORMAT_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.";
  public static final String TIMESTAMP_FORMAT_TZ_PATTERN = "XXX";
  public static final String TIME_FORMAT_PATTERN = "HH:mm:ss.SSS";
  private static final byte[] BYTE_ARRAY = new byte[0];
  public static final String BINARY_CLASS_NAME = BYTE_ARRAY.getClass().getName();

  /**
   * Converts text of data type (returned from SQL query) into Types type, represented by an int.
   *
   * @param typeName type name
   * @return int representation of type from {@link java.sql.Types}
   */
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
      case "decfloat":
        retval = SnowflakeType.EXTRA_TYPES_DECFLOAT;
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
      case "interval_year_month":
        retval = SnowflakeType.EXTRA_TYPES_YEAR_MONTH_INTERVAL;
        break;
      case "interval_day_time":
        retval = SnowflakeType.EXTRA_TYPES_DAY_TIME_INTERVAL;
        break;
      case "variant":
        retval = Types.OTHER;
        break;
      case "object":
        retval = Types.JAVA_OBJECT;
        break;
      case "vector":
        retval = SnowflakeType.EXTRA_TYPES_VECTOR;
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

  /**
   * Determines if a Java SQL type is signed.
   *
   * @param type the Java SQL type from {@link java.sql.Types}
   * @return true if the type is signed (INTEGER, DECIMAL, or DOUBLE)
   */
  public static boolean isJavaTypeSigned(int type) {
    return type == Types.INTEGER || type == Types.DECIMAL || type == Types.DOUBLE;
  }

  /** Internal enum representing Java data types for Snowflake columns. */
  public enum JavaDataType {
    JAVA_STRING(String.class),
    JAVA_LONG(Long.class),
    JAVA_DOUBLE(Double.class),
    JAVA_BIGDECIMAL(BigDecimal.class),
    JAVA_TIMESTAMP(Timestamp.class),
    JAVA_PERIOD(Period.class),
    JAVA_DURATION(Duration.class),
    JAVA_BYTES(byte[].class),
    JAVA_BOOLEAN(Boolean.class),
    JAVA_OBJECT(Object.class);

    JavaDataType(Class<?> c) {
      this._class = c;
    }

    private Class<?> _class;
  }

  /** Internal enum representing Java SQL types with convenient lookup methods. */
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
    VARCHAR(Types.VARCHAR),
    VECTOR(Types.ARRAY);

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
      ALL_TYPES.add(VECTOR);
    }

    JavaSQLType(int type) {
      this.type = type;
    }

    public int getType() {
      return type;
    }

    /**
     * Find a JavaSQLType by its integer type value.
     *
     * @param type the integer type from {@link java.sql.Types}
     * @return the corresponding JavaSQLType, or null if not found
     */
    public static JavaSQLType find(int type) {
      for (JavaSQLType t : ALL_TYPES) {
        if (t.type == type) {
          return t;
        }
      }
      return null;
    }
  }
}
