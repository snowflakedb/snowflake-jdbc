package net.snowflake.client.core;

import static net.snowflake.client.jdbc.SnowflakeUtil.mapSFExceptionToSQLException;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLData;
import java.sql.SQLException;
import java.sql.SQLInput;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import net.snowflake.client.core.json.Converters;
import net.snowflake.client.core.structs.SQLDataCreationHelper;
import net.snowflake.client.jdbc.FieldMetadata;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.client.util.ThrowingBiFunction;
import org.apache.arrow.vector.util.JsonStringArrayList;
import org.apache.arrow.vector.util.JsonStringHashMap;

@SnowflakeJdbcInternalApi
public class ArrowSqlInput extends BaseSqlInput {
  private static final SFLogger logger = SFLoggerFactory.getLogger(ArrowSqlInput.class);

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
    return withNextValue((this::convertString));
  }

  @Override
  public boolean readBoolean() throws SQLException {
    return withNextValue(this::convertBoolean);
  }

  @Override
  public byte readByte() throws SQLException {
    return withNextValue(
        (value, fieldMetadata) ->
            mapSFExceptionToSQLException(() -> converters.getNumberConverter().getByte(value)));
  }

  @Override
  public short readShort() throws SQLException {
    return withNextValue(this::convertShort);
  }

  @Override
  public int readInt() throws SQLException {
    return withNextValue(this::convertInt);
  }

  @Override
  public long readLong() throws SQLException {
    return withNextValue(this::convertLong);
  }

  @Override
  public float readFloat() throws SQLException {
    return withNextValue(this::convertFloat);
  }

  @Override
  public double readDouble() throws SQLException {
    return withNextValue(this::convertDouble);
  }

  @Override
  public BigDecimal readBigDecimal() throws SQLException {
    return withNextValue(this::convertBigDecimal);
  }

  @Override
  public byte[] readBytes() throws SQLException {
    return withNextValue(this::convertBytes);
  }

  @Override
  public Date readDate() throws SQLException {
    return withNextValue(
        (value, fieldMetadata) -> {
          if (value == null) {
            return null;
          }
          return convertDate((int) value);
        });
  }

  private Date convertDate(int value) throws SQLException {
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
          return convertTime((long) value, fieldMetadata);
        });
  }

  private Time convertTime(long value, FieldMetadata fieldMetadata) throws SQLException {
    return mapSFExceptionToSQLException(
        () -> {
          int scale = fieldMetadata.getScale();
          return converters.getStructuredTypeDateTimeConverter().getTime(value, scale);
        });
  }

  @Override
  public Timestamp readTimestamp(TimeZone tz) throws SQLException {
    return withNextValue((value, fieldMetadata) -> convertTimestamp(tz, value, fieldMetadata));
  }

  private Timestamp convertTimestamp(TimeZone tz, Object value, FieldMetadata fieldMetadata)
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
    return readObject(type, TimeZone.getDefault());
  }

  @Override
  public <T> T readObject(Class<T> type, TimeZone tz) throws SQLException {
    return withNextValue((value, fieldMetadata) -> convertObject(type, tz, value, fieldMetadata));
  }

  private <T> T convertObject(Class<T> type, TimeZone tz, Object value, FieldMetadata fieldMetadata)
      throws SQLException {
    if (value == null) {
      return null;
    } else if (SQLData.class.isAssignableFrom(type)) {
      ArrowSqlInput sqlInput =
          new ArrowSqlInput(
              (Map<String, Object>) value, session, converters, fieldMetadata.getFields());
      SQLData instance = (SQLData) SQLDataCreationHelper.create(type);
      instance.readSQL(sqlInput, null);
      return (T) instance;
    } else if (Map.class.isAssignableFrom(type)) {
      return (T) convertSqlInputToMap((SQLInput) value);
    } else if (String.class.isAssignableFrom(type)) {
      return (T) convertString(value, fieldMetadata);
    } else if (Boolean.class.isAssignableFrom(type)) {
      return (T) convertBoolean(value, fieldMetadata);
    } else if (Byte.class.isAssignableFrom(type)) {
      return (T) convertBytes(value, fieldMetadata);
    } else if (Short.class.isAssignableFrom(type)) {
      return (T) convertShort(value, fieldMetadata);
    } else if (Integer.class.isAssignableFrom(type)) {
      return (T) convertInt(value, fieldMetadata);
    } else if (Long.class.isAssignableFrom(type)) {
      return (T) convertLong(value, fieldMetadata);
    } else if (Float.class.isAssignableFrom(type)) {
      return (T) convertFloat(value, fieldMetadata);
    } else if (Double.class.isAssignableFrom(type)) {
      return (T) convertDouble(value, fieldMetadata);
    } else if (Date.class.isAssignableFrom(type)) {
      return (T) convertDate((int) value);
    } else if (Time.class.isAssignableFrom(type)) {
      return (T) convertTime((long) value, fieldMetadata);
    } else if (Timestamp.class.isAssignableFrom(type)) {
      return (T) convertTimestamp(tz, value, fieldMetadata);
    } else if (BigDecimal.class.isAssignableFrom(type)) {
      return (T) convertBigDecimal(value, fieldMetadata);
    } else if (byte[].class.isAssignableFrom(type)) {
      return (T) convertBytes(value, fieldMetadata);
    } else {
      logger.debug(
          "Unsupported type passed to readObject(int columnIndex,Class<T> type): "
              + type.getName());
      throw new SQLException(
          "Type passed to 'getObject(int columnIndex,Class<T> type)' is unsupported. Type: "
              + type.getName());
    }
  }

  @Override
  public <T> List<T> readList(Class<T> type) throws SQLException {
    return withNextValue(
        (value, fieldMetadata) -> {
          if (value == null) {
            return null;
          }
          List<T> result = new ArrayList();
          JsonStringArrayList<Map> maps = (JsonStringArrayList) value;
          for (Object ob : maps) {
            result.add(
                convertObject(type, TimeZone.getDefault(), ob, fieldMetadata.getFields().get(0)));
          }
          return result;
        });
  }

  @Override
  public <T> T[] readArray(Class<T> type) throws SQLException {
    return withNextValue(
        (value, fieldMetadata) -> {
          if (value == null) {
            return null;
          }
          JsonStringArrayList<Map> internalValues = (JsonStringArrayList) value;
          T[] array = (T[]) java.lang.reflect.Array.newInstance(type, internalValues.size());
          int counter = 0;
          for (Object ob : internalValues) {
            array[counter++] =
                convertObject(type, TimeZone.getDefault(), ob, fieldMetadata.getFields().get(0));
          }
          return array;
        });
  }

  @Override
  public <T> Map<String, T> readMap(Class<T> type) throws SQLException {
    return withNextValue(
        (value, fieldMetadata) -> {
          if (value == null) {
            return null;
          }
          Map<String, T> result = new HashMap();
          JsonStringArrayList<Map> maps = (JsonStringArrayList) value;
          for (Map map : maps) {
            result.put(
                map.get("key").toString(),
                convertObject(
                    type,
                    TimeZone.getDefault(),
                    map.get("value"),
                    fieldMetadata.getFields().get(1)));
          }
          return result;
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
