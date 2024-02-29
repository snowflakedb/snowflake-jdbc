/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import static net.snowflake.client.jdbc.SnowflakeUtil.getTimestampFromType;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import net.snowflake.client.core.json.Converters;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.FieldMetadata;
import net.snowflake.client.jdbc.SnowflakeColumnMetadata;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.client.util.TypeConverter;
import net.snowflake.common.core.SFTimestamp;
import net.snowflake.common.core.SnowflakeDateTimeFormat;

/** Abstract class used to represent snowflake result set in json format */
public abstract class SFJsonResultSet extends SFBaseResultSet {
  private static final SFLogger logger = SFLoggerFactory.getLogger(SFJsonResultSet.class);
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getObjectMapper();

  protected final TimeZone sessionTimeZone;
  protected final Converters converters;

  protected SFJsonResultSet(TimeZone sessionTimeZone, Converters converters) {
    this.sessionTimeZone = sessionTimeZone;
    this.converters = converters;
  }

  /**
   * Given a column index, get current row's value as an object
   *
   * @param columnIndex index of columns
   * @return an object
   * @throws SFException raises if any error occurs
   */
  protected abstract Object getObjectInternal(int columnIndex) throws SFException;

  public Object getObject(int columnIndex) throws SFException {

    int type = resultSetMetaData.getColumnType(columnIndex);

    Object obj = getObjectInternal(columnIndex);
    if (obj == null) {
      return null;
    }

    switch (type) {
      case Types.VARCHAR:
      case Types.CHAR:
        return getString(columnIndex);

      case Types.BINARY:
        return getBytes(columnIndex);

      case Types.INTEGER:
        return getInt(columnIndex);

      case Types.DECIMAL:
        return getBigDecimal(columnIndex);

      case Types.BIGINT:
        return getBigInt(columnIndex, obj);

      case Types.DOUBLE:
        return getDouble(columnIndex);

      case Types.TIMESTAMP:
      case Types.TIMESTAMP_WITH_TIMEZONE:
        return getTimestamp(columnIndex);

      case Types.DATE:
        return getDate(columnIndex);

      case Types.TIME:
        return getTime(columnIndex);

      case Types.BOOLEAN:
        return getBoolean(columnIndex);

      case Types.STRUCT:
        if (Boolean.valueOf(System.getProperty(STRUCTURED_TYPE_ENABLED_PROPERTY_NAME))) {
          return getSqlInput((String) obj, columnIndex);
        } else {
          throw new SFException(ErrorCode.FEATURE_UNSUPPORTED, "data type: " + type);
        }
      case Types.ARRAY:
        if (Boolean.valueOf(System.getProperty(STRUCTURED_TYPE_ENABLED_PROPERTY_NAME))) {
          return getArray(columnIndex);
        } else {
          throw new SFException(ErrorCode.FEATURE_UNSUPPORTED, "data type: " + type);
        }

      default:
        throw new SFException(ErrorCode.FEATURE_UNSUPPORTED, "data type: " + type);
    }
  }

  @Override
  public Array getArray(int columnIndex) throws SFException {
    Object obj = getObjectInternal(columnIndex);
    return getArrayInternal((String) obj);
  }

  @Override
  public String getString(int columnIndex) throws SFException {
    logger.debug("public String getString(int columnIndex)", false);
    Object obj = getObjectInternal(columnIndex);
    int columnType = resultSetMetaData.getInternalColumnType(columnIndex);
    int columnSubType = resultSetMetaData.getInternalColumnType(columnIndex);
    int scale = resultSetMetaData.getScale(columnIndex);
    return converters.getStringConverter().getString(obj, columnType, columnSubType, scale);
  }

  @Override
  public boolean getBoolean(int columnIndex) throws SFException {
    logger.debug("public boolean getBoolean(int columnIndex)", false);
    int columnType = resultSetMetaData.getColumnType(columnIndex);
    return converters.getBooleanConverter().getBoolean(getObjectInternal(columnIndex), columnType);
  }

  @Override
  public byte getByte(int columnIndex) throws SFException {
    logger.debug("public short getByte(int columnIndex)", false);
    Object obj = getObjectInternal(columnIndex);
    return converters.getNumberConverter().getByte(obj);
  }

  @Override
  public short getShort(int columnIndex) throws SFException {
    logger.debug("public short getShort(int columnIndex)", false);
    Object obj = getObjectInternal(columnIndex);
    int columnType = resultSetMetaData.getColumnType(columnIndex);
    return converters.getNumberConverter().getShort(obj, columnType);
  }

  @Override
  public int getInt(int columnIndex) throws SFException {
    logger.debug("public int getInt(int columnIndex)", false);
    Object obj = getObjectInternal(columnIndex);
    int columnType = resultSetMetaData.getColumnType(columnIndex);
    return converters.getNumberConverter().getInt(obj, columnType);
  }

  @Override
  public long getLong(int columnIndex) throws SFException {
    logger.debug("public long getLong(int columnIndex)", false);
    Object obj = getObjectInternal(columnIndex);
    int columnType = resultSetMetaData.getColumnType(columnIndex);
    return converters.getNumberConverter().getLong(obj, columnType);
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex) throws SFException {
    logger.debug("public BigDecimal getBigDecimal(int columnIndex)", false);
    Object obj = getObjectInternal(columnIndex);
    int columnType = resultSetMetaData.getColumnType(columnIndex);
    return converters.getNumberConverter().getBigDecimal(obj, columnType);
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex, int scale) throws SFException {
    logger.debug("public BigDecimal getBigDecimal(int columnIndex)", false);
    Object obj = getObjectInternal(columnIndex);
    int columnType = resultSetMetaData.getColumnType(columnIndex);
    return converters.getNumberConverter().getBigDecimal(obj, columnType, scale);
  }

  @Override
  public Time getTime(int columnIndex) throws SFException {
    logger.debug("public Time getTime(int columnIndex)", false);
    Object obj = getObjectInternal(columnIndex);
    int columnType = resultSetMetaData.getColumnType(columnIndex);
    int columnSubType = resultSetMetaData.getInternalColumnType(columnIndex);
    int scale = resultSetMetaData.getScale(columnIndex);
    return converters
        .getDateTimeConverter()
        .getTime(obj, columnType, columnSubType, TimeZone.getDefault(), scale);
  }

  @Override
  public Timestamp getTimestamp(int columnIndex, TimeZone tz) throws SFException {
    logger.debug("public Timestamp getTimestamp(int columnIndex)", false);
    Object obj = getObjectInternal(columnIndex);
    int columnType = resultSetMetaData.getColumnType(columnIndex);
    int columnSubType = resultSetMetaData.getInternalColumnType(columnIndex);
    int scale = resultSetMetaData.getScale(columnIndex);
    return converters
        .getDateTimeConverter()
        .getTimestamp(obj, columnType, columnSubType, tz, scale);
  }

  @Override
  public float getFloat(int columnIndex) throws SFException {
    logger.debug("public float getFloat(int columnIndex)", false);
    Object obj = getObjectInternal(columnIndex);
    int columnType = resultSetMetaData.getColumnType(columnIndex);
    return converters.getNumberConverter().getFloat(obj, columnType);
  }

  @Override
  public double getDouble(int columnIndex) throws SFException {
    logger.debug("public double getDouble(int columnIndex)", false);
    Object obj = getObjectInternal(columnIndex);
    int columnType = resultSetMetaData.getColumnType(columnIndex);
    return converters.getNumberConverter().getDouble(obj, columnType);
  }

  @Override
  public byte[] getBytes(int columnIndex) throws SFException {
    logger.debug("public byte[] getBytes(int columnIndex)", false);
    Object obj = getObjectInternal(columnIndex);
    int columnType = resultSetMetaData.getColumnType(columnIndex);
    int columnSubType = resultSetMetaData.getInternalColumnType(columnIndex);
    int scale = resultSetMetaData.getScale(columnIndex);
    return converters.getBytesConverter().getBytes(obj, columnType, columnSubType, scale);
  }

  public Date getDate(int columnIndex) throws SFException {
    return getDate(columnIndex, TimeZone.getDefault());
  }

  @Override
  public Date getDate(int columnIndex, TimeZone tz) throws SFException {
    logger.debug("public Date getDate(int columnIndex)", false);
    Object obj = getObjectInternal(columnIndex);
    int columnType = resultSetMetaData.getColumnType(columnIndex);
    int columnSubType = resultSetMetaData.getInternalColumnType(columnIndex);
    int scale = resultSetMetaData.getScale(columnIndex);
    return converters.getDateTimeConverter().getDate(obj, columnType, columnSubType, tz, scale);
  }

  private Timestamp getTimestamp(int columnIndex) throws SFException {
    return getTimestamp(columnIndex, TimeZone.getDefault());
  }

  public Converters getConverters() {
    return converters;
  }

  private Object getSqlInput(String input, int columnIndex) throws SFException {
    try {
      JsonNode jsonNode = OBJECT_MAPPER.readTree(input);
      return new JsonSqlInput(
          jsonNode,
          session,
          converters,
          resultSetMetaData.getColumnMetadata().get(columnIndex - 1).getFields());
    } catch (JsonProcessingException e) {
      throw new SFException(e, ErrorCode.INVALID_STRUCT_DATA);
    }
  }

  private SfSqlArray getArrayInternal(String obj) throws SFException {
    try {
      SnowflakeColumnMetadata arrayMetadata = resultSetMetaData.getColumnMetadata().get(0);
      FieldMetadata fieldMetadata = arrayMetadata.getField(1);

      int columnSubType = fieldMetadata.getType();
      int columnType = ColumnTypeHelper.getColumnType(columnSubType, session);
      int scale = fieldMetadata.getScale();

      ArrayNode arrayNode = (ArrayNode) OBJECT_MAPPER.readTree(obj);

      Iterator nodeElements = arrayNode.elements();

      switch (columnSubType) {
        case Types.INTEGER:
        case Types.SMALLINT:
        case Types.TINYINT:
          TypeConverter integerConverter =
              value -> converters.getNumberConverter().getInt(value, Types.INTEGER);
          return new SfSqlArray(
              columnSubType, getStream(nodeElements, integerConverter).toArray(Integer[]::new));
        case Types.BIGINT:
        case Types.DECIMAL:
        case Types.NUMERIC:
          TypeConverter bigIntConverter =
              value -> converters.getNumberConverter().getBigInt(value, Types.BIGINT);
          return new SfSqlArray(
              columnSubType, convertToNumericArray(nodeElements, bigIntConverter));
        case Types.CHAR:
        case Types.VARCHAR:
        case Types.LONGNVARCHAR:
          TypeConverter varcharConverter = value -> value.toString();
          return new SfSqlArray(
              columnSubType, getStream(nodeElements, varcharConverter).toArray(String[]::new));
        case Types.BINARY:
          TypeConverter bytesConverter =
              value ->
                  converters.getBytesConverter().getBytes(value, columnType, Types.BINARY, scale);
          return new SfSqlArray(
              columnSubType, getStream(nodeElements, bytesConverter).toArray(Object[]::new));
        case Types.FLOAT:
        case Types.DOUBLE:
          TypeConverter doubleConverter =
              value -> converters.getNumberConverter().getDouble(value, Types.DOUBLE);
          return new SfSqlArray(
              columnSubType, getStream(nodeElements, doubleConverter).toArray(Double[]::new));
        case Types.DATE:
          TypeConverter dateConverter =
              value -> {
                SnowflakeDateTimeFormat formatter =
                    SnowflakeDateTimeFormat.fromSqlFormat(
                        (String) session.getCommonParameters().get("DATE_OUTPUT_FORMAT"));
                SFTimestamp timestamp = formatter.parse((String) value);
                return Date.valueOf(
                    Instant.ofEpochMilli(timestamp.getTime()).atZone(ZoneOffset.UTC).toLocalDate());
              };
          return new SfSqlArray(
              columnSubType, getStream(nodeElements, dateConverter).toArray(Date[]::new));
        case Types.TIME:
          TypeConverter timeConverter =
              value -> {
                SnowflakeDateTimeFormat formatter =
                    SnowflakeDateTimeFormat.fromSqlFormat(
                        (String) session.getCommonParameters().get("TIME_OUTPUT_FORMAT"));
                SFTimestamp timestamp = formatter.parse((String) value);
                return Time.valueOf(
                    Instant.ofEpochMilli(timestamp.getTime()).atZone(ZoneOffset.UTC).toLocalTime());
              };
          return new SfSqlArray(
              columnSubType, getStream(nodeElements, timeConverter).toArray(Time[]::new));
        case Types.TIMESTAMP:
          TypeConverter timestampConverter =
              value -> {
                Timestamp result = getTimestampFromType(columnSubType, (String) value, session);
                if (result != null) {
                  return result;
                }
                return converters
                    .getDateTimeConverter()
                    .getTimestamp(value, columnType, columnSubType, null, scale);
              };
          return new SfSqlArray(
              columnSubType, getStream(nodeElements, timestampConverter).toArray(Timestamp[]::new));
        case Types.BOOLEAN:
          TypeConverter booleanConverter =
              value -> converters.getBooleanConverter().getBoolean(value, columnType);
          return new SfSqlArray(
              columnSubType, getStream(nodeElements, booleanConverter).toArray(Boolean[]::new));
        case Types.STRUCT:
          TypeConverter structConverter =
              value -> {
                try {
                  return OBJECT_MAPPER.readValue(value, Map.class);
                } catch (JsonProcessingException e) {
                  throw new SFException(e, ErrorCode.INVALID_STRUCT_DATA);
                }
              };
          return new SfSqlArray(
              columnSubType, getStream(nodeElements, structConverter).toArray(Map[]::new));
        case Types.ARRAY:
          TypeConverter arrayConverter =
              value -> {
                try {
                  return OBJECT_MAPPER.readValue(value, HashMap[].class);
                } catch (JsonProcessingException e) {
                  throw new SFException(e, ErrorCode.INVALID_STRUCT_DATA);
                }
              };
          return new SfSqlArray(
              columnSubType, getStream(nodeElements, arrayConverter).toArray(Map[][]::new));
        default:
          return null;
      }
    } catch (JsonProcessingException e) {
      throw new SFException(e, ErrorCode.INVALID_STRUCT_DATA);
    }
  }

  private Object[] convertToNumericArray(Iterator nodeElements, TypeConverter bigIntConverter) {
    AtomicInteger bigDecimalCount = new AtomicInteger();
    Object[] elements =
        getStream(nodeElements, bigIntConverter)
            .map(
                elem -> {
                  if (elem instanceof BigDecimal) {
                    bigDecimalCount.incrementAndGet();
                  }
                  return elem;
                })
            .toArray(
                size -> {
                  boolean shouldbbeReturnAsBigDecimal = bigDecimalCount.get() > 0;
                  Class<?> returnedClass =
                      shouldbbeReturnAsBigDecimal ? BigDecimal.class : Long.class;
                  return java.lang.reflect.Array.newInstance(returnedClass, size);
                });
    return elements;
  }

  private Stream getStream(Iterator nodeElements, TypeConverter converter) {
    return StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(nodeElements, Spliterator.ORDERED), false)
        .map(
            elem -> {
              try {
                return convert(converter, (JsonNode) elem);
              } catch (SFException e) {
                throw new RuntimeException(e);
              }
            });
  }

  private static Object convert(TypeConverter converter, JsonNode elem) throws SFException {
    JsonNode node = elem;
    if (node.isValueNode()) {
      return converter.convert(node.asText());
    } else {
      return converter.convert(node.toString());
    }
  }

  /**
   * Sometimes large BIGINTS overflow the java Long type. In these cases, return a BigDecimal type
   * instead.
   *
   * @param columnIndex the column index
   * @return an object of type long or BigDecimal depending on number size
   * @throws SFException
   */
  private Object getBigInt(int columnIndex, Object obj) throws SFException {
    return converters.getNumberConverter().getBigInt(obj, columnIndex);
  }
}
