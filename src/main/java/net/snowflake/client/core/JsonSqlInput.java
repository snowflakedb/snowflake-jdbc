/*
 * Copyright (c) 2012-2024 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.core;

import static net.snowflake.client.core.SFBaseResultSet.OBJECT_MAPPER;
import static net.snowflake.client.core.SFResultSet.logger;
import static net.snowflake.client.jdbc.SnowflakeUtil.mapSFExceptionToSQLException;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLData;
import java.sql.SQLException;
import java.sql.SQLInput;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import net.snowflake.client.core.json.Converters;
import net.snowflake.client.core.structs.SQLDataCreationHelper;
import net.snowflake.client.jdbc.FieldMetadata;
import net.snowflake.client.util.ThrowingTriFunction;
import net.snowflake.common.core.SFTimestamp;
import net.snowflake.common.core.SnowflakeDateTimeFormat;

@SnowflakeJdbcInternalApi
public class JsonSqlInput extends BaseSqlInput {
  private final JsonNode input;
  private final Iterator<JsonNode> elements;
  private final TimeZone sessionTimeZone;
  private int currentIndex = 0;
  private boolean wasNull = false;

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
          return mapSFExceptionToSQLException(
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
          return mapSFExceptionToSQLException(
              () -> converters.getBooleanConverter().getBoolean(value, columnType));
        });
  }

  @Override
  public byte readByte() throws SQLException {
    return withNextValue(
        (value, jsonNode, fieldMetadata) ->
            mapSFExceptionToSQLException(() -> converters.getNumberConverter().getByte(value)));
  }

  @Override
  public short readShort() throws SQLException {
    return withNextValue(
        (value, jsonNode, fieldMetadata) -> {
          int columnType = ColumnTypeHelper.getColumnType(fieldMetadata.getType(), session);
          return mapSFExceptionToSQLException(
              () -> converters.getNumberConverter().getShort(value, columnType));
        });
  }

  @Override
  public int readInt() throws SQLException {
    return withNextValue(
        (value, jsonNode, fieldMetadata) -> {
          int columnType = ColumnTypeHelper.getColumnType(fieldMetadata.getType(), session);
          return mapSFExceptionToSQLException(
              () -> converters.getNumberConverter().getInt(value, columnType));
        });
  }

  @Override
  public long readLong() throws SQLException {
    return withNextValue(
        (value, jsonNode, fieldMetadata) -> {
          int columnType = ColumnTypeHelper.getColumnType(fieldMetadata.getType(), session);
          return mapSFExceptionToSQLException(
              () -> converters.getNumberConverter().getLong(value, columnType));
        });
  }

  @Override
  public float readFloat() throws SQLException {
    return withNextValue(
        (value, jsonNode, fieldMetadata) -> {
          int columnType = ColumnTypeHelper.getColumnType(fieldMetadata.getType(), session);
          return mapSFExceptionToSQLException(
              () -> converters.getNumberConverter().getFloat(value, columnType));
        });
  }

  @Override
  public double readDouble() throws SQLException {
    return withNextValue(
        (value, jsonNode, fieldMetadata) -> {
          int columnType = ColumnTypeHelper.getColumnType(fieldMetadata.getType(), session);
          return mapSFExceptionToSQLException(
              () -> converters.getNumberConverter().getDouble(value, columnType));
        });
  }

  @Override
  public BigDecimal readBigDecimal() throws SQLException {
    return withNextValue(
        (value, jsonNode, fieldMetadata) -> convertBigDecimal(value, fieldMetadata));
  }

  private BigDecimal convertBigDecimal(Object value, FieldMetadata fieldMetadata)
      throws SQLException {
    int columnType = ColumnTypeHelper.getColumnType(fieldMetadata.getType(), session);
    return mapSFExceptionToSQLException(
        () -> converters.getNumberConverter().getBigDecimal(value, columnType));
  }

  @Override
  public byte[] readBytes() throws SQLException {
    return withNextValue((value, jsonNode, fieldMetadata) -> convertToBytes(value, fieldMetadata));
  }

  private byte[] convertToBytes(Object value, FieldMetadata fieldMetadata) throws SQLException {
    int columnType = ColumnTypeHelper.getColumnType(fieldMetadata.getType(), session);
    int columnSubType = fieldMetadata.getType();
    int scale = fieldMetadata.getScale();
    return mapSFExceptionToSQLException(
        () -> converters.getBytesConverter().getBytes(value, columnType, columnSubType, scale));
  }

  @Override
  public Date readDate() throws SQLException {
    return withNextValue(
        (value, jsonNode, fieldMetadata) -> {
          if (value == null) {
            return null;
          }
          return formatDate((String) value);
        });
  }

  private Date formatDate(String value) {
    SnowflakeDateTimeFormat formatter = getFormat(session, "DATE_OUTPUT_FORMAT");
    SFTimestamp timestamp = formatter.parse(value);
    return Date.valueOf(
        Instant.ofEpochMilli(timestamp.getTime()).atZone(ZoneOffset.UTC).toLocalDate());
  }

  @Override
  public Time readTime() throws SQLException {
    return withNextValue(
        (value, jsonNode, fieldMetadata) -> {
          if (value == null) {
            return null;
          }
          return formatTime((String) value);
        });
  }

  private Time formatTime(String value) {
    SnowflakeDateTimeFormat formatter = getFormat(session, "TIME_OUTPUT_FORMAT");
    SFTimestamp timestamp = formatter.parse(value);
    return Time.valueOf(
        Instant.ofEpochMilli(timestamp.getTime()).atZone(ZoneOffset.UTC).toLocalTime());
  }

  @Override
  public Timestamp readTimestamp() throws SQLException {
    return readTimestamp(null);
  }

  @Override
  public Timestamp readTimestamp(TimeZone tz) throws SQLException {
    return withNextValue(
        (value, jsonNode, fieldMetadata) -> {
          if (value == null) {
            return null;
          }
          return formatTimestamp(tz, value, fieldMetadata);
        });
  }

  @Override
  public <T> T readObject(Class<T> type) throws SQLException {
    return withNextValue(
        (value, jsonNode, fieldMetadata) -> {
          if (SQLData.class.isAssignableFrom(type)) {
            if (jsonNode.isNull()) {
              return null;
            } else {
              SQLInput sqlInput =
                  new JsonSqlInput(
                      jsonNode, session, converters, fieldMetadata.getFields(), sessionTimeZone);
              SQLData instance = (SQLData) SQLDataCreationHelper.create(type);
              instance.readSQL(sqlInput, null);
              return (T) instance;
            }
          } else if (Map.class.isAssignableFrom(type)) {
            if (value == null) {
              return null;
            } else {
              return (T) convertSqlInputToMap((SQLInput) value);
            }
          } else if (value == null) {
            return null;
          } else if (String.class.isAssignableFrom(type)
              || Boolean.class.isAssignableFrom(type)
              || Byte.class.isAssignableFrom(type)
              || Short.class.isAssignableFrom(type)
              || Integer.class.isAssignableFrom(type)
              || Long.class.isAssignableFrom(type)
              || Float.class.isAssignableFrom(type)
              || Double.class.isAssignableFrom(type)) {
            return (T) value;
          } else if (Date.class.isAssignableFrom(type)) {
            return (T) formatDate((String) value);
          } else if (Time.class.isAssignableFrom(type)) {
            return (T) formatTime((String) value);
          } else if (Timestamp.class.isAssignableFrom(type)) {
            return (T) formatTimestamp(sessionTimeZone, value, fieldMetadata);
          } else if (Byte[].class.isAssignableFrom(type)) {
            return (T) convertToBytes(value, fieldMetadata);
          } else if (BigDecimal.class.isAssignableFrom(type)) {
            return (T) convertBigDecimal(value, fieldMetadata);
          } else {
            logger.debug(
                "Unsupported type passed to readObject(int columnIndex,Class<T> type): "
                    + type.getName());
            throw new SQLException(
                "Type passed to 'getObject(int columnIndex,Class<T> type)' is unsupported. Type: "
                    + type.getName());
          }
        });
  }

  private Timestamp formatTimestamp(TimeZone tz, Object value, FieldMetadata fieldMetadata)
      throws SQLException {
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
    return mapSFExceptionToSQLException(
        () ->
            converters
                .getDateTimeConverter()
                .getTimestamp(value, columnType, columnSubType, tz, scale));
  }

  @Override
  public Object readObject() throws SQLException {
    return withNextValue((value, jsonNode, fieldMetadata) -> value);
  }

  public boolean wasNull() {
    return wasNull;
  }

  @Override
  Map<String, Object> convertSqlInputToMap(SQLInput sqlInput) {
    return OBJECT_MAPPER.convertValue(
        ((JsonSqlInput) sqlInput).getInput(), new TypeReference<Map<String, Object>>() {});
  }

  private <T> T withNextValue(
      ThrowingTriFunction<Object, JsonNode, FieldMetadata, T, SQLException> action)
      throws SQLException {
    JsonNode jsonNode = elements.next();
    Object value = getValue(jsonNode);
    wasNull = value == null;
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
