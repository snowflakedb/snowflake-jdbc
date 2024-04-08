/*
 * Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import static net.snowflake.client.core.SFResultSet.logger;
import static net.snowflake.client.jdbc.SnowflakeUtil.mapSFExceptionToSQLException;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLData;
import java.sql.SQLException;
import java.sql.SQLInput;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import net.snowflake.client.core.json.Converters;
import net.snowflake.client.core.structs.SQLDataCreationHelper;
import net.snowflake.client.jdbc.FieldMetadata;
import net.snowflake.client.util.ThrowingBiFunction;
import org.apache.arrow.vector.util.JsonStringHashMap;

@SnowflakeJdbcInternalApi
public class ArrowSqlInput extends BaseSqlInput {

  private final Map<String, Object> input;
  private int currentIndex = 0;
  private boolean wasNull = false;

  public ArrowSqlInput(
      Map<String, Object> input,
      SFBaseSession session,
      Converters converters,
      List<FieldMetadata> fields) {
    super(session, converters, fields);
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
          return mapSFExceptionToSQLException(
              () -> converters.getNumberConverter().getShort(value, columnType));
        });
  }

  @Override
  public int readInt() throws SQLException {
    return withNextValue(
        (value, fieldMetadata) -> {
          int columnType = ColumnTypeHelper.getColumnType(fieldMetadata.getType(), session);
          return mapSFExceptionToSQLException(
              () -> converters.getNumberConverter().getInt(value, columnType));
        });
  }

  @Override
  public long readLong() throws SQLException {
    return withNextValue(
        (value, fieldMetadata) -> {
          int columnType = ColumnTypeHelper.getColumnType(fieldMetadata.getType(), session);
          return mapSFExceptionToSQLException(
              () -> converters.getNumberConverter().getLong(value, columnType));
        });
  }

  @Override
  public float readFloat() throws SQLException {
    return withNextValue(
        (value, fieldMetadata) -> {
          int columnType = ColumnTypeHelper.getColumnType(fieldMetadata.getType(), session);
          return mapSFExceptionToSQLException(
              () -> converters.getNumberConverter().getFloat(value, columnType));
        });
  }

  @Override
  public double readDouble() throws SQLException {
    return withNextValue(
        (value, fieldMetadata) -> {
          int columnType = ColumnTypeHelper.getColumnType(fieldMetadata.getType(), session);
          return mapSFExceptionToSQLException(
              () -> converters.getNumberConverter().getDouble(value, columnType));
        });
  }

  @Override
  public BigDecimal readBigDecimal() throws SQLException {
    return withNextValue((value, fieldMetadata) -> convertToBigDecimal(value, fieldMetadata));
  }

  private BigDecimal convertToBigDecimal(Object value, FieldMetadata fieldMetadata)
      throws SQLException {
    int columnType = ColumnTypeHelper.getColumnType(fieldMetadata.getType(), session);
    return mapSFExceptionToSQLException(
        () -> converters.getNumberConverter().getBigDecimal(value, columnType));
  }

  @Override
  public byte[] readBytes() throws SQLException {
    return withNextValue((value, fieldMetadata) -> converToBytes(value, fieldMetadata));
  }

  private byte[] converToBytes(Object value, FieldMetadata fieldMetadata) throws SQLException {
    int columnType = ColumnTypeHelper.getColumnType(fieldMetadata.getType(), session);
    int columnSubType = fieldMetadata.getType();
    int scale = fieldMetadata.getScale();
    return mapSFExceptionToSQLException(
        () -> converters.getBytesConverter().getBytes(value, columnType, columnSubType, scale));
  }

  @Override
  public Date readDate() throws SQLException {
    return withNextValue(
        (value, fieldMetadata) -> {
          if (value == null) {
            return null;
          }
          return formatDate((int) value);
        });
  }

  private Date formatDate(int value) throws SQLException {
    return mapSFExceptionToSQLException(
        () ->
            converters.getStructuredTypeDateTimeConverter().getDate(value, TimeZone.getDefault()));
  }

  @Override
  public Time readTime() throws SQLException {
    return withNextValue(
        (value, fieldMetadata) -> {
          if (value == null) {
            return null;
          }
          return formatTime((long) value, fieldMetadata);
        });
  }

  private Time formatTime(long value, FieldMetadata fieldMetadata) throws SQLException {
    return mapSFExceptionToSQLException(
        () -> {
          int scale = fieldMetadata.getScale();
          return converters.getStructuredTypeDateTimeConverter().getTime(value, scale);
        });
  }

  @Override
  public Timestamp readTimestamp(TimeZone tz) throws SQLException {
    return withNextValue((value, fieldMetadata) -> formatTimestamp(tz, value, fieldMetadata));
  }

  private Timestamp formatTimestamp(TimeZone tz, Object value, FieldMetadata fieldMetadata)
      throws SQLException {
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
                .getTimestamp((Map<String, Object>) value, columnType, columnSubType, tz, scale));
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
          if (SQLData.class.isAssignableFrom(type)) {
            if (value == null) {
              return null;
            } else {
              ArrowSqlInput sqlInput =
                  new ArrowSqlInput(
                      (Map<String, Object>) value, session, converters, fieldMetadata.getFields());
              SQLData instance = (SQLData) SQLDataCreationHelper.create(type);
              instance.readSQL(sqlInput, null);
              return (T) instance;
            }
          } else if (value == null) {
            return null;
          } else if (Map.class.isAssignableFrom(type)) {
            if (value == null) {
              return null;
            } else {
              return (T) convertSqlInputToMap((SQLInput) value);
            }
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
            return (T) formatDate((int) value);
          } else if (Time.class.isAssignableFrom(type)) {
            return (T) formatTime((long) value, fieldMetadata);
          } else if (Timestamp.class.isAssignableFrom(type)) {
            return (T) formatTimestamp(TimeZone.getDefault(), value, fieldMetadata);
          } else if (BigDecimal.class.isAssignableFrom(type)) {
            return (T) convertToBigDecimal(value, fieldMetadata);
          } else if (byte[].class.isAssignableFrom(type)) {
            return (T) converToBytes(value, fieldMetadata);
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

  @Override
  public boolean wasNull() {
    return wasNull;
  }

  @Override
  Map<String, Object> convertSqlInputToMap(SQLInput sqlInput) {
    return ((ArrowSqlInput) sqlInput).getInput();
  }

  private <T> T withNextValue(ThrowingBiFunction<Object, FieldMetadata, T, SQLException> action)
      throws SQLException {
    FieldMetadata field = fields.get(currentIndex++);
    Object value = input.get(field.getName());
    wasNull = value == null;
    return action.apply(value, field);
  }
}
