/*
 * Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import net.snowflake.client.core.json.Converters;
import net.snowflake.client.core.structs.SQLDataCreationHelper;
import net.snowflake.client.jdbc.FieldMetadata;
import net.snowflake.client.util.ThrowingBiFunction;
import org.apache.arrow.vector.util.JsonStringHashMap;

import static net.snowflake.client.jdbc.SnowflakeUtil.mapSFExceptionToSQLException;

@SnowflakeJdbcInternalApi
public class ArrowSqlInput extends BaseSqlInput {

    private final JsonStringHashMap<String, Object> input;
  private final Iterator<Object> structuredTypeFields;
  private int currentIndex = 0;

  public ArrowSqlInput(
      JsonStringHashMap<String, Object> input,
      SFBaseSession session,
      Converters converters,
      List<FieldMetadata> fields) {
    super(session, converters, fields);
    this.structuredTypeFields = input.values().iterator();
      this.input = input;
  }

  public Map<String, Object> getInput() {
      return input;
  }

  @Override
  public String readString() throws SQLException {
    return withNextValue(
        ((value, fieldMetadata) -> {
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
        (value, fieldMetadata) -> {
          int columnType = ColumnTypeHelper.getColumnType(fieldMetadata.getType(), session);
          return mapSFExceptionToSQLException(
              () -> converters.getBooleanConverter().getBoolean(value, columnType));
        });
  }

  @Override
  public byte readByte() throws SQLException {
    return withNextValue(
        (value, fieldMetadata) ->
            mapSFExceptionToSQLException(() -> converters.getNumberConverter().getByte(value)));
  }

  @Override
  public short readShort() throws SQLException {
    return withNextValue(
        (value, fieldMetadata) -> {
          int columnType = ColumnTypeHelper.getColumnType(fieldMetadata.getType(), session);
          return mapSFExceptionToSQLException(() -> converters.getNumberConverter().getShort(value, columnType));
        });
  }

  @Override
  public int readInt() throws SQLException {
    return withNextValue(
        (value, fieldMetadata) -> {
          int columnType = ColumnTypeHelper.getColumnType(fieldMetadata.getType(), session);
          return mapSFExceptionToSQLException(() -> converters.getNumberConverter().getInt(value, columnType));
        });
  }

  @Override
  public long readLong() throws SQLException {
    return withNextValue(
        (value, fieldMetadata) -> {
          int columnType = ColumnTypeHelper.getColumnType(fieldMetadata.getType(), session);
          return mapSFExceptionToSQLException(() -> converters.getNumberConverter().getLong(value, columnType));
        });
  }

  @Override
  public float readFloat() throws SQLException {
    return withNextValue(
        (value, fieldMetadata) -> {
          int columnType = ColumnTypeHelper.getColumnType(fieldMetadata.getType(), session);
          return mapSFExceptionToSQLException(() -> converters.getNumberConverter().getFloat(value, columnType));
        });
  }

  @Override
  public double readDouble() throws SQLException {
    return withNextValue(
        (value, fieldMetadata) -> {
          int columnType = ColumnTypeHelper.getColumnType(fieldMetadata.getType(), session);
          return mapSFExceptionToSQLException(() -> converters.getNumberConverter().getDouble(value, columnType));
        });
  }

  @Override
  public BigDecimal readBigDecimal() throws SQLException {
    return withNextValue(
        (value, fieldMetadata) -> {
          int columnType = ColumnTypeHelper.getColumnType(fieldMetadata.getType(), session);
          return mapSFExceptionToSQLException(
              () -> converters.getNumberConverter().getBigDecimal(value, columnType));
        });
  }

  @Override
  public byte[] readBytes() throws SQLException {
    return withNextValue(
        (value, fieldMetadata) -> {
          int columnType = ColumnTypeHelper.getColumnType(fieldMetadata.getType(), session);
          int columnSubType = fieldMetadata.getType();
          int scale = fieldMetadata.getScale();
          return mapSFExceptionToSQLException(
              () ->
                  converters.getBytesConverter().getBytes(value, columnType, columnSubType, scale));
        });
  }

  @Override
  public Date readDate() throws SQLException {
    return withNextValue(
        (value, fieldMetadata) ->
            mapSFExceptionToSQLException(
                () ->
                    converters
                        .getStructuredTypeDateTimeConverter()
                        .getDate((int) value, TimeZone.getDefault())));
  }

  @Override
  public Time readTime() throws SQLException {
    return withNextValue(
        (value, fieldMetadata) ->
            mapSFExceptionToSQLException(
                () -> {
                  int scale = fieldMetadata.getScale();
                  return converters
                      .getStructuredTypeDateTimeConverter()
                      .getTime((long) value, scale);
                }));
  }

  @Override
  public Timestamp readTimestamp(TimeZone tz) throws SQLException {
    return withNextValue(
        (value, fieldMetadata) -> {
          if (value == null) {
            return null;
          }
            int columnType = ColumnTypeHelper.getColumnType(fieldMetadata.getType(), session);
            int columnSubType = fieldMetadata.getType();
          int scale = fieldMetadata.getScale();
          return mapSFExceptionToSQLException(
              () ->
                  converters
                      .getStructuredTypeDateTimeConverter()
                      .getTimestamp(
                          (JsonStringHashMap<String, Object>) value,
                          columnType,
                              columnSubType,
                          tz,
                          scale));
        });
  }

  @Override
  public Object readObject() throws SQLException {
    return withNextValue(
        (value, fieldMetadata) -> {
          if (!(value instanceof JsonStringHashMap)) {
            throw new SQLException(
                "Invalid value passed to 'readObject()', expected Map; got: " + value.getClass());
          }
          return value;
        });
  }

  @Override
  public <T> T readObject(Class<T> type) throws SQLException {
    return withNextValue(
        (value, fieldMetadata) -> {
          SQLData instance = (SQLData) SQLDataCreationHelper.create(type);
          instance.readSQL(
              new ArrowSqlInput(
                  (JsonStringHashMap<String, Object>) value,
                  session,
                  converters,
                  fieldMetadata.getFields()),
              null);
          return (T) instance;
        });
  }

  private <T> T withNextValue(ThrowingBiFunction<Object, FieldMetadata, T, SQLException> action)
      throws SQLException {
    return action.apply(structuredTypeFields.next(), fields.get(currentIndex++));
  }
}
