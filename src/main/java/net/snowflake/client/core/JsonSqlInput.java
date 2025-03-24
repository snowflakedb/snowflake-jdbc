package net.snowflake.client.core;

import static net.snowflake.client.core.SFBaseResultSet.OBJECT_MAPPER;
import static net.snowflake.client.jdbc.SnowflakeUtil.mapSFExceptionToSQLException;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLData;
import java.sql.SQLException;
import java.sql.SQLInput;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import net.snowflake.client.core.json.Converters;
import net.snowflake.client.core.structs.SQLDataCreationHelper;
import net.snowflake.client.jdbc.FieldMetadata;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.client.util.ThrowingBiFunction;
import net.snowflake.common.core.SFTimestamp;
import net.snowflake.common.core.SnowflakeDateTimeFormat;

@SnowflakeJdbcInternalApi
public class JsonSqlInput extends BaseSqlInput {
  private static final SFLogger logger = SFLoggerFactory.getLogger(JsonSqlInput.class);
  private final String text;
  private final JsonNode input;
  private final Iterator<JsonNode> elements;
  private final TimeZone sessionTimeZone;
  private int currentIndex = 0;
  private boolean wasNull = false;

  public JsonSqlInput(
      String text,
      JsonNode input,
      SFBaseSession session,
      Converters converters,
      List<FieldMetadata> fields,
      TimeZone sessionTimeZone) {
    super(session, converters, fields);
    this.text = text;
    this.input = input;
    this.elements = input.elements();
    this.sessionTimeZone = sessionTimeZone;
  }

  public JsonNode getInput() {
    return input;
  }

  public String getText() {
    return text;
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
          return convertDate((String) value);
        });
  }

  private Date convertDate(String value) {
    SnowflakeDateTimeFormat formatter = getFormat(session, "DATE_OUTPUT_FORMAT");
    SFTimestamp timestamp = formatter.parse(value);
    return Date.valueOf(
        Instant.ofEpochMilli(timestamp.getTime()).atZone(ZoneOffset.UTC).toLocalDate());
  }

  @Override
  public Time readTime() throws SQLException {
    return withNextValue(
        (value, fieldMetadata) -> {
          if (value == null) {
            return null;
          }
          return convertTime((String) value);
        });
  }

  private Time convertTime(String value) {
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
        (value, fieldMetadata) -> {
          if (value == null) {
            return null;
          }
          return convertTimestamp(tz, value, fieldMetadata);
        });
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
      if (!JsonNode.class.isAssignableFrom(value.getClass())) {
        logger.error("Object of class JsonNode is expected to convert to SqlData");
        return null;
      }
      JsonNode jsonNode = (JsonNode) value;
      SQLInput sqlInput =
          new JsonSqlInput(
              null, jsonNode, session, converters, fieldMetadata.getFields(), sessionTimeZone);
      SQLData instance = (SQLData) SQLDataCreationHelper.create(type);
      instance.readSQL(sqlInput, null);
      return (T) instance;
    } else if (Map.class.isAssignableFrom(type)) {
      if (value == null) {
        return null;
      } else {
        return (T) convertSqlInputToMap((SQLInput) value);
      }
    } else if (String.class.isAssignableFrom(type)) {
      return (T) convertString(value, fieldMetadata);
    } else if (Boolean.class.isAssignableFrom(type)) {
      return (T) convertBoolean(value, fieldMetadata);
    } else if (Byte.class.isAssignableFrom(type)) {
      return (T) convertString(value, fieldMetadata);
    } else if (Short.class.isAssignableFrom(type)) {
      return (T) convertShort(value, fieldMetadata);
    } else if (Integer.class.isAssignableFrom(type)) {
      return (T) convertInt(value, fieldMetadata);
    } else if (Long.class.isAssignableFrom(type)) {
      return (T) convertLong(value, fieldMetadata);
    } else if (Float.class.isAssignableFrom(type)) {
      return (T) convertFloat(value, fieldMetadata);
    } else if (Double.class.isAssignableFrom(type)) {
      return (T) convertFloat(value, fieldMetadata);
    } else if (Date.class.isAssignableFrom(type)) {
      return (T) convertDate((String) value);
    } else if (Time.class.isAssignableFrom(type)) {
      return (T) convertTime((String) value);
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
          if (ArrayNode.class.isAssignableFrom(value.getClass())) {
            for (JsonNode node : (ArrayNode) value) {
              result.add(
                  convertObject(
                      type,
                      TimeZone.getDefault(),
                      getValue(node),
                      fieldMetadata.getFields().get(0)));
            }
            return result;
          } else {
            logger.debug("Given object could not be converted to List of type: " + type.getName());
            throw new SQLException(
                "Given object could not be converted to List of type: " + type.getName());
          }
        });
  }

  @Override
  public <T> T[] readArray(Class<T> type) throws SQLException {
    return withNextValue(
        (value, fieldMetadata) -> {
          if (value == null) {
            return null;
          }
          if (ArrayNode.class.isAssignableFrom(value.getClass())) {
            ArrayNode valueNodes = (ArrayNode) value;
            T[] array = (T[]) java.lang.reflect.Array.newInstance(type, valueNodes.size());
            int counter = 0;
            for (JsonNode node : valueNodes) {
              array[counter++] =
                  convertObject(
                      type,
                      TimeZone.getDefault(),
                      getValue(node),
                      fieldMetadata.getFields().get(0));
            }
            return array;
          } else {
            logger.debug("Given object could not be converted to Array of type: " + type.getName());
            throw new SQLException(
                "Given object could not be converted to List of type: " + type.getName());
          }
        });
  }

  @Override
  public <T> Map<String, T> readMap(Class<T> type) throws SQLException {
    return withNextValue(
        (value, fieldMetadata) -> {
          if (value == null) {
            return null;
          }
          if (ObjectNode.class.isAssignableFrom(value.getClass())) {
            Map<String, T> result = new HashMap<>();
            ObjectNode arrayNode = (ObjectNode) value;
            for (Iterator<String> it = arrayNode.fieldNames(); it.hasNext(); ) {
              String key = it.next();
              result.put(
                  key,
                  convertObject(
                      type, TimeZone.getDefault(), getValue(arrayNode.get(key)), fieldMetadata));
            }
            return result;
          } else {
            logger.debug(
                "Given object could not be converted to Map of String and type: " + type.getName());
            throw new SQLException(
                "Given object could not be converted to Map of String and type: " + type.getName());
          }
        });
  }

  private Timestamp convertTimestamp(TimeZone tz, Object value, FieldMetadata fieldMetadata)
      throws SQLException {
    if (value == null) {
      return null;
    }
    int columnType = ColumnTypeHelper.getColumnType(fieldMetadata.getType(), session);
    int columnSubType = fieldMetadata.getType();
    int scale = fieldMetadata.getScale();
    Timestamp result =
        SfTimestampUtil.getTimestampFromType(
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
    return withNextValue((value, fieldMetadata) -> value);
  }

  @Override
  public <T> T readObject(Class<T> type) throws SQLException {
    return readObject(type, sessionTimeZone);
  }

  public boolean wasNull() {
    return wasNull;
  }

  @Override
  Map<String, Object> convertSqlInputToMap(SQLInput sqlInput) {
    return OBJECT_MAPPER.convertValue(
        ((JsonSqlInput) sqlInput).getInput(), new TypeReference<Map<String, Object>>() {});
  }

  private <T> T withNextValue(ThrowingBiFunction<Object, FieldMetadata, T, SQLException> action)
      throws SQLException {
    JsonNode jsonNode = elements.next();
    Object value = getValue(jsonNode);
    wasNull = value == null;
    return action.apply(value, fields.get(currentIndex++));
  }

  private Object getValue(JsonNode jsonNode) {
    if (jsonNode.isTextual()) {
      return jsonNode.textValue();
    } else if (jsonNode.isBoolean()) {
      return jsonNode.booleanValue();
    } else if (jsonNode.isNumber()) {
      return jsonNode.numberValue();
    } else if (jsonNode.isObject() || jsonNode.isArray()) {
      return jsonNode;
    }
    return null;
  }

  private static SnowflakeDateTimeFormat getFormat(SFBaseSession session, String format) {
    return SnowflakeDateTimeFormat.fromSqlFormat(
        (String) session.getCommonParameters().get(format));
  }
}
