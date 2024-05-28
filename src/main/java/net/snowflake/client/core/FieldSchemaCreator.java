package net.snowflake.client.core;

import java.sql.SQLException;
import java.sql.Types;
import java.util.Optional;
import net.snowflake.client.jdbc.BindingParameterMetadata;
import net.snowflake.client.jdbc.SnowflakeColumn;
import net.snowflake.client.jdbc.SnowflakeType;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

@SnowflakeJdbcInternalApi
public class FieldSchemaCreator {
  static final SFLogger logger = SFLoggerFactory.getLogger(FieldSchemaCreator.class);
  public static final int MAX_TEXT_COLUMN_SIZE = 134217728;
  public static final int MAX_BINARY_COLUMN_SIZE = 67108864;

  public static BindingParameterMetadata buildSchemaForText(
      String fieldName, Optional<SnowflakeColumn> maybeColumn) {
    return BindingParameterMetadata.BindingParameterMetadataBuilder.bindingParameterMetadata()
        .withType(maybeColumn.map(cl -> cl.type()).filter(str -> !str.isEmpty()).orElse("text"))
        .withLength(maybeColumn.map(cl -> cl.length()).orElse(MAX_TEXT_COLUMN_SIZE))
        .withName(maybeColumn.map(cl -> cl.name()).filter(str -> !str.isEmpty()).orElse(fieldName))
        .build();
  }

  public static BindingParameterMetadata buildSchemaForBytesType(
      String fieldName, Optional<SnowflakeColumn> maybeColumn) {
    return BindingParameterMetadata.BindingParameterMetadataBuilder.bindingParameterMetadata()
        .withType(maybeColumn.map(cl -> cl.type()).filter(str -> !str.isEmpty()).orElse("binary"))
        .withName(maybeColumn.map(cl -> cl.name()).filter(str -> !str.isEmpty()).orElse(fieldName))
        .withLength(maybeColumn.map(cl -> cl.precision()).orElse(MAX_TEXT_COLUMN_SIZE))
        .withByteLength(maybeColumn.map(cl -> cl.byteLength()).orElse(MAX_BINARY_COLUMN_SIZE))
        .build();
  }

  public static BindingParameterMetadata buildSchemaTypeAndNameOnly(
      String fieldName, String type, Optional<SnowflakeColumn> maybeColumn) {
    return BindingParameterMetadata.BindingParameterMetadataBuilder.bindingParameterMetadata()
        .withType(maybeColumn.map(cl -> cl.type()).filter(str -> !str.isEmpty()).orElse(type))
        .withName(maybeColumn.map(cl -> cl.name()).filter(str -> !str.isEmpty()).orElse(fieldName))
        .build();
  }

  public static BindingParameterMetadata buildSchemaWithScaleAndPrecision(
      String fieldName,
      String type,
      int scale,
      int precision,
      Optional<SnowflakeColumn> maybeColumn) {
    return BindingParameterMetadata.BindingParameterMetadataBuilder.bindingParameterMetadata()
        .withType(maybeColumn.map(cl -> cl.type()).filter(str -> !str.isEmpty()).orElse(type))
        .withScale(maybeColumn.map(cl -> cl.scale()).filter(i -> i > 0).orElse(scale))
        .withName(maybeColumn.map(cl -> cl.name()).filter(str -> !str.isEmpty()).orElse(fieldName))
        .withPrecision(maybeColumn.map(cl -> cl.precision()).filter(i -> i > 0).orElse(precision))
        .build();
  }

  public static BindingParameterMetadata buildBindingSchemaForType(int baseType)
      throws SQLException {
    return buildBindingSchemaForType(baseType, true);
  }

  public static BindingParameterMetadata buildBindingSchemaForType(int baseType, boolean addName)
      throws SQLException {
    String name = addName ? SnowflakeType.javaTypeToSFType(baseType, null).name() : null;
    switch (baseType) {
      case Types.VARCHAR:
      case Types.CHAR:
        return FieldSchemaCreator.buildSchemaForText(name, Optional.empty());
      case Types.FLOAT:
      case Types.DOUBLE:
      case Types.DECIMAL:
        return FieldSchemaCreator.buildSchemaWithScaleAndPrecision(
            name, "real", 9, 38, Optional.empty());
      case Types.NUMERIC:
      case Types.INTEGER:
      case Types.SMALLINT:
      case Types.TINYINT:
      case Types.BIGINT:
        return FieldSchemaCreator.buildSchemaWithScaleAndPrecision(
            null, "fixed", 0, 38, Optional.empty());
      case Types.BOOLEAN:
        return FieldSchemaCreator.buildSchemaTypeAndNameOnly(name, "boolean", Optional.empty());
      case Types.DATE:
        return FieldSchemaCreator.buildSchemaTypeAndNameOnly(name, "date", Optional.empty());
      case Types.TIMESTAMP:
      case Types.TIME:
        return FieldSchemaCreator.buildSchemaWithScaleAndPrecision(
            name, "timestamp", 9, 0, Optional.empty());
      default:
        logger.error("Could not create schema for type : " + baseType);
        throw new SQLException("Could not create schema for type : " + baseType);
    }
  }
}
