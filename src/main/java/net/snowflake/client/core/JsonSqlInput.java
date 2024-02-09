package net.snowflake.client.core;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.TimeZone;
import net.snowflake.client.core.json.Converters;
import net.snowflake.client.core.structs.SQLDataCreationHelper;
import net.snowflake.client.jdbc.MetadataField;
import net.snowflake.client.jdbc.SnowflakeLoggedFeatureNotSupportedException;
import net.snowflake.client.jdbc.SnowflakeUtil;
import net.snowflake.client.util.ThrowingCallable;
import net.snowflake.client.util.ThrowingTriFunction;
import net.snowflake.common.core.SFTimestamp;
import net.snowflake.common.core.SnowflakeDateTimeFormat;

// TODO structuredType use json converters
public class JsonSqlInput implements SFSqlInput {
  private final JsonNode input;
  private final Iterator<JsonNode> elements;
  private final SFBaseSession session;
  private final Converters converters;
  private final List<MetadataField> fields;
  private int currentIndex = 0;

  public JsonSqlInput(
      JsonNode input, SFBaseSession session, Converters converters, List<MetadataField> fields) {
    this.input = input;
    this.elements = input.elements();
    this.session = session;
    this.converters = converters;
    this.fields = fields;
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
          int columnType = ColumnTypeHelper.getColumnType(fieldMetadata.getType(), session);
          int columnSubType = fieldMetadata.getType();
          TimeZone tz = TimeZone.getDefault(); // TODO structuredType how to get timezone?
          int scale = fieldMetadata.getScale();
          SnowflakeDateTimeFormat formatter =
              SnowflakeDateTimeFormat.fromSqlFormat(
                  (String) session.getCommonParameters().get("DATE_OUTPUT_FORMAT"));
          SFTimestamp timestamp = formatter.parse((String) value);
          return Date.valueOf(
              Instant.ofEpochMilli(timestamp.getTime()).atZone(ZoneOffset.UTC).toLocalDate());
        });
  }

  @Override
  public Time readTime() throws SQLException {
    return withNextValue(
        (value, jsonNode, fieldMetadata) -> {
          int columnType = ColumnTypeHelper.getColumnType(fieldMetadata.getType(), session);
          int columnSubType = fieldMetadata.getType();
          TimeZone tz = TimeZone.getDefault(); // TODO structuredType how to get timezone?
          int scale = fieldMetadata.getScale();
          SnowflakeDateTimeFormat formatter =
              SnowflakeDateTimeFormat.fromSqlFormat(
                  (String) session.getCommonParameters().get("TIME_OUTPUT_FORMAT"));
          SFTimestamp timestamp = formatter.parse((String) value);
          return Time.valueOf(
              Instant.ofEpochMilli(timestamp.getTime()).atZone(ZoneOffset.UTC).toLocalTime());
        });
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
          int columnType = ColumnTypeHelper.getColumnType(fieldMetadata.getType(), session);
          int columnSubType = fieldMetadata.getType();
          int scale = fieldMetadata.getScale();
          // TODO structuredType what if not a string value?
          Timestamp result = getTimestampFromType(columnSubType, (String) value);
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

  private Timestamp getTimestampFromType(int columnSubType, String value) {
    if (columnSubType == SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_LTZ) {
      return getTimestampFromFormat("TIMESTAMP_LTZ_OUTPUT_FORMAT", value);
    } else if (columnSubType == SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_NTZ
        || columnSubType == Types.TIMESTAMP) {
      return getTimestampFromFormat("TIMESTAMP_NTZ_OUTPUT_FORMAT", value);
    } else if (columnSubType == SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_TZ) {
      return getTimestampFromFormat("TIMESTAMP_TZ_OUTPUT_FORMAT", value);
    } else {
      return null;
    }
  }

  private Timestamp getTimestampFromFormat(String format, String value) {
    String rawFormat = (String) session.getCommonParameters().get(format);
    if (rawFormat == null || rawFormat.equals("")) {
      rawFormat = (String) session.getCommonParameters().get("TIMESTAMP_OUTPUT_FORMAT");
    }
    SnowflakeDateTimeFormat formatter = SnowflakeDateTimeFormat.fromSqlFormat(rawFormat);
    return formatter.parse(value).getTimestamp();
  }

  @Override
  public Reader readCharacterStream() throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(session, "readCharacterStream");
  }

  @Override
  public InputStream readAsciiStream() throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(session, "readAsciiStream");
  }

  @Override
  public InputStream readBinaryStream() throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(session, "readBinaryStream");
  }

  @Override
  public Object readObject() throws SQLException {
    // TODO structuredType return map
    throw new SnowflakeLoggedFeatureNotSupportedException(session, "readCharacterStream");
  }

  @Override
  public Ref readRef() throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(session, "readRef");
  }

  @Override
  public Blob readBlob() throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(session, "readBlob");
  }

  @Override
  public Clob readClob() throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(session, "readClob");
  }

  @Override
  public Array readArray() throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(session, "readArray");
  }

  @Override
  public boolean wasNull() throws SQLException {
    return false; // nulls are not allowed in structure types
  }

  @Override
  public URL readURL() throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(session, "readCharacterStream");
  }

  @Override
  public NClob readNClob() throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(session, "readNClob");
  }

  @Override
  public String readNString() throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(session, "readNString");
  }

  @Override
  public SQLXML readSQLXML() throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(session, "readSQLXML");
  }

  @Override
  public RowId readRowId() throws SQLException {
    throw new SnowflakeLoggedFeatureNotSupportedException(session, "readRowId");
  }

  @Override
  public <T> T readObject(Class<T> type) throws SQLException {
    return withNextValue(
        (__, jsonNode, fieldMetadata) -> {
          // TODO structuredType what if it is not an object but i.e. string?
          SQLData instance = (SQLData) SQLDataCreationHelper.create(type);
          instance.readSQL(
              new JsonSqlInput(
                  jsonNode, session, converters, Arrays.asList(fieldMetadata.getFields())),
              null);
          return (T) instance;
        });
  }

  private <T> T withNextValue(
      ThrowingTriFunction<Object, JsonNode, MetadataField, T, SQLException> action)
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

  private <T> T mapExceptions(ThrowingCallable<T, SFException> action) throws SQLException {
    try {
      return action.call();
    } catch (SFException e) {
      throw new SQLException(e);
    }
  }
}
