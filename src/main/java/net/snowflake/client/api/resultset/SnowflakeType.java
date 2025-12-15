package net.snowflake.client.api.resultset;

/**
 * Enumeration of Snowflake data types.
 *
 * <p>This enum represents the various data types supported by Snowflake. These type values are used
 * in metadata operations such as {@link FieldMetadata#getBase()} to describe the underlying type of
 * a field in structured types (OBJECT, ARRAY, MAP).
 *
 * <h2 id="usage-example">Usage Example</h2>
 *
 * <pre>{@code
 * import net.snowflake.client.api.resultset.FieldMetadata;
 * import net.snowflake.client.api.resultset.SnowflakeType;
 *
 * // Get field metadata from a structured type column
 * FieldMetadata field = resultSetMetaData.getColumnFields(1).get(0);
 * SnowflakeType baseType = field.getBase();
 *
 * if (baseType == SnowflakeType.TEXT) {
 *     System.out.println("Field is a text type");
 * } else if (baseType == SnowflakeType.INTEGER) {
 *     System.out.println("Field is an integer type");
 * }
 * }</pre>
 *
 * @since 4.0.0
 * @see FieldMetadata
 */
public enum SnowflakeType {
  /** Represents an ANY type (unspecified/dynamic) */
  ANY,
  /** Represents an ARRAY type */
  ARRAY,
  /** Represents a BINARY type */
  BINARY,
  /** Represents a BOOLEAN type */
  BOOLEAN,
  /** Represents a CHAR type */
  CHAR,
  /** Represents a DATE type */
  DATE,
  /** Represents a DECFLOAT (decimal floating-point) type */
  DECFLOAT,
  /** Represents a FIXED-point numeric type (NUMBER with scale) */
  FIXED,
  /** Represents an INTEGER type */
  INTEGER,
  /** Represents an OBJECT type (structured) */
  OBJECT,
  /** Represents a MAP type */
  MAP,
  /** Represents a REAL (floating-point) type */
  REAL,
  /** Represents a TEXT/VARCHAR type */
  TEXT,
  /** Represents a TIME type */
  TIME,
  /** Represents a TIMESTAMP type (no timezone) */
  TIMESTAMP,
  /** Represents a TIMESTAMP with local timezone */
  TIMESTAMP_LTZ,
  /** Represents a TIMESTAMP with no timezone */
  TIMESTAMP_NTZ,
  /** Represents a TIMESTAMP with timezone */
  TIMESTAMP_TZ,
  /** Represents an INTERVAL YEAR TO MONTH type */
  INTERVAL_YEAR_MONTH,
  /** Represents an INTERVAL DAY TO TIME type */
  INTERVAL_DAY_TIME,
  /** Represents a VARIANT (semi-structured) type */
  VARIANT,
  /** Represents a GEOGRAPHY type */
  GEOGRAPHY,
  /** Represents a GEOMETRY type */
  GEOMETRY,
  /** Represents a VECTOR type */
  VECTOR
}
