/*
 * Copyright (c) 2012-2024 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.core;

import static net.snowflake.client.jdbc.SnowflakeUtil.mapExceptions;

import com.fasterxml.jackson.databind.JsonNode;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Iterator;
import java.util.List;
import java.util.TimeZone;
import net.snowflake.client.core.json.Converters;
import net.snowflake.client.core.structs.SQLDataCreationHelper;
import net.snowflake.client.jdbc.FieldMetadata;
import net.snowflake.client.jdbc.SnowflakeLoggedFeatureNotSupportedException;
import net.snowflake.client.util.ThrowingTriFunction;
import net.snowflake.common.core.SFTimestamp;
import net.snowflake.common.core.SnowflakeDateTimeFormat;

@SnowflakeJdbcInternalApi
public class JsonSqlInput extends BaseSqlInput {
  private final JsonNode input;
  private final Iterator<JsonNode> elements;
  private final TimeZone sessionTimeZone;
  private int currentIndex = 0;

  public JsonSqlInput(
      JsonNode input,
      SFBaseSession session,
      Converters converters,
      List<FieldMetadata> fields,
      TimeZone sessionTimeZone) {
    super(session, converters, fields);
    this.input = input;
    this.elements = input.elements();
    this.sessionTimeZone = sessionTimeZone;
  }

  public JsonNode getInput() {
    return input;
  }

  @Override
  public String readString() throws SQLException {
    return withNextValue(
        ((value, jsonNode, fieldMetadata) -> {
          int columnType = ColumnTypeHelper.getColumnType(fieldMetadata.getType(), session);
          int columnSubType = fieldMetadata.getType();
          int scale = fieldMetadata.getScale();
          return mapExceptions(
              () ->
                  converters
                      .getStringConverter()
                      .getString(value, columnType, columnSubType, scale));
        }));
  }

  @Override
  public boolean readBoolean() throws SQLException {
    return withNextValue(
        (value, jsonNode, fieldMetadata) -> {
          int columnType = ColumnTypeHelper.getColumnType(fieldMetadata.getType(), session);
          return mapExceptions(
              () -> converters.getBooleanConverter().getBoolean(value, columnType));
        });
  }

  @Override
  public byte readByte() throws SQLException {
    return withNextValue(
        (value, jsonNode, fieldMetadata) ->
            mapExceptions(() -> converters.getNumberConverter().getByte(value)));
  }

  @Override
  public short readShort() throws SQLException {
    return withNextValue(
        (value, jsonNode, fieldMetadata) -> {
          int columnType = ColumnTypeHelper.getColumnType(fieldMetadata.getType(), session);
          return mapExceptions(() -> converters.getNumberConverter().getShort(value, columnType));
        });
  }

  @Override
  public int readInt() throws SQLException {
    return withNextValue(
        (value, jsonNode, fieldMetadata) -> {
          int columnType = ColumnTypeHelper.getColumnType(fieldMetadata.getType(), session);
          return mapExceptions(() -> converters.getNumberConverter().getInt(value, columnType));
        });
  }

  @Override
  public long readLong() throws SQLException {
    return withNextValue(
        (value, jsonNode, fieldMetadata) -> {
          int columnType = ColumnTypeHelper.getColumnType(fieldMetadata.getType(), session);
          return mapExceptions(() -> converters.getNumberConverter().getLong(value, columnType));
        });
  }

  @Override
  public float readFloat() throws SQLException {
    return withNextValue(
        (value, jsonNode, fieldMetadata) -> {
          int columnType = ColumnTypeHelper.getColumnType(fieldMetadata.getType(), session);
          return mapExceptions(() -> converters.getNumberConverter().getFloat(value, columnType));
        });
  }

  @Override
  public double readDouble() throws SQLException {
    return withNextValue(
        (value, jsonNode, fieldMetadata) -> {
          int columnType = ColumnTypeHelper.getColumnType(fieldMetadata.getType(), session);
          return mapExceptions(() -> converters.getNumberConverter().getDouble(value, columnType));
        });
  }

  @Override
  public BigDecimal readBigDecimal() throws SQLException {
    return withNextValue(
        (value, jsonNode, fieldMetadata) -> {
          int columnType = ColumnTypeHelper.getColumnType(fieldMetadata.getType(), session);
          return mapExceptions(
              () -> converters.getNumberConverter().getBigDecimal(value, columnType));
        });
  }

  @Override
  public byte[] readBytes() throws SQLException {
    return withNextValue(
        (value, jsonNode, fieldMetadata) -> {
          int columnType = ColumnTypeHelper.getColumnType(fieldMetadata.getType(), session);
          int columnSubType = fieldMetadata.getType();
          int scale = fieldMetadata.getScale();
          return mapExceptions(
              () ->
                  converters.getBytesConverter().getBytes(value, columnType, columnSubType, scale));
        });
  }

  @Override
  public Date readDate() throws SQLException {
    return withNextValue(
        (value, jsonNode, fieldMetadata) -> {
          SnowflakeDateTimeFormat formatter = getFormat(session, "DATE_OUTPUT_FORMAT");
          SFTimestamp timestamp = formatter.parse((String) value);
          return Date.valueOf(
              Instant.ofEpochMilli(timestamp.getTime()).atZone(ZoneOffset.UTC).toLocalDate());
        });
  }

  @Override
  public Time readTime() throws SQLException {
    return withNextValue(
        (value, jsonNode, fieldMetadata) -> {
          SnowflakeDateTimeFormat formatter = getFormat(session, "TIME_OUTPUT_FORMAT");
          SFTimestamp timestamp = formatter.parse((String) value);
          return Time.valueOf(
              Instant.ofEpochMilli(timestamp.getTime()).atZone(ZoneOffset.UTC).toLocalTime());
        });
  }

  @Override
  public Timestamp readTimestamp(TimeZone tz) throws SQLException {
    return withNextValue(
        (value, jsonNode, fieldMetadata) -> {
          if (value == null) {
            return null;
          }
          int columnType = ColumnTypeHelper.getColumnType(fieldMetadata.getType(), session);
          int columnSubType = fieldMetadata.getType();
          int scale = fieldMetadata.getScale();
          Timestamp result =
              SqlInputTimestampUtil.getTimestampFromType(
                  columnSubType, (String) value, session, sessionTimeZone, tz);
          if (result != null) {
            return result;
          }
          return mapExceptions(
              () ->
                  converters
                      .getDateTimeConverter()
                      .getTimestamp(value, columnType, columnSubType, tz, scale));
        });
  }

  @Override
  public Object readObject() throws SQLException {
    // TODO structuredType return map - SNOW-974575
    throw new SnowflakeLoggedFeatureNotSupportedException(session, "readObject");
  }

  @Override
  public <T> T readObject(Class<T> type) throws SQLException {
    return withNextValue(
        (__, jsonNode, fieldMetadata) -> {
          SQLData instance = (SQLData) SQLDataCreationHelper.create(type);
          instance.readSQL(
              new JsonSqlInput(
                  jsonNode, session, converters, fieldMetadata.getFields(), sessionTimeZone),
              null);
          return (T) instance;
        });
  }

  private <T> T withNextValue(
      ThrowingTriFunction<Object, JsonNode, FieldMetadata, T, SQLException> action)
      throws SQLException {
    JsonNode jsonNode = elements.next();
    Object value = getValue(jsonNode);
    return action.apply(value, jsonNode, fields.get(currentIndex++));
  }

  private Object getValue(JsonNode jsonNode) {
    if (jsonNode.isTextual()) {
      return jsonNode.textValue();
    } else if (jsonNode.isBoolean()) {
      return jsonNode.booleanValue();
    } else if (jsonNode.isNumber()) {
      return jsonNode.numberValue();
    }
    return null;
  }

  private static SnowflakeDateTimeFormat getFormat(SFBaseSession session, String format) {
    return SnowflakeDateTimeFormat.fromSqlFormat(
        (String) session.getCommonParameters().get(format));
  }
}
