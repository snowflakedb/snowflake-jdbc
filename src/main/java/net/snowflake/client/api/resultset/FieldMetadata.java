package net.snowflake.client.api.resultset;

import java.util.List;

/**
 * Metadata describing a field in a structured type (OBJECT, ARRAY, MAP). This interface provides
 * read-only access to field information including name, type, precision, scale, and nested fields.
 */
public interface FieldMetadata {

  /**
   * Gets the name of the field.
   *
   * @return the field name
   */
  String getName();

  /**
   * Gets the type name of the field.
   *
   * @return the type name
   */
  String getTypeName();

  /**
   * Gets the SQL type code of the field.
   *
   * @return the SQL type code as defined in {@link java.sql.Types}
   */
  int getType();

  /**
   * Checks if the field is nullable.
   *
   * @return true if the field can contain null values, false otherwise
   */
  boolean isNullable();

  /**
   * Gets the byte length of the field.
   *
   * @return the byte length
   */
  int getByteLength();

  /**
   * Gets the precision of the field.
   *
   * @return the precision
   */
  int getPrecision();

  /**
   * Gets the scale of the field.
   *
   * @return the scale
   */
  int getScale();

  /**
   * Checks if the field has a fixed size.
   *
   * @return true if the field has a fixed size, false otherwise
   */
  boolean isFixed();

  /**
   * Gets the base Snowflake type of the field.
   *
   * @return the base {@link SnowflakeType}
   */
  SnowflakeType getBase();

  /**
   * Gets the nested field metadata for structured types (OBJECT, ARRAY, MAP).
   *
   * @return list of nested field metadata, or empty list if no nested fields
   */
  List<FieldMetadata> getFields();
}
