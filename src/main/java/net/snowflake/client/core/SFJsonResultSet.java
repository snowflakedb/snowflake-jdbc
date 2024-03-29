/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.math.BigDecimal;
import java.sql.*;
import java.util.Iterator;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import net.snowflake.client.core.structs.StructureTypeHelper;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.FieldMetadata;
import net.snowflake.client.jdbc.SnowflakeColumnMetadata;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.client.util.Converter;

/** Abstract class used to represent snowflake result set in json format */
public abstract class SFJsonResultSet extends SFBaseResultSet {
  private static final SFLogger logger = SFLoggerFactory.getLogger(SFJsonResultSet.class);
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getObjectMapper();

  protected final Converters converters;

  protected SFJsonResultSet(TimeZone sessionTimeZone, Converters converters) {
    this.sessionTimezone = sessionTimeZone;
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
        if (StructureTypeHelper.isStructureTypeEnabled()) {
          return getSqlInput((String) obj, columnIndex);
        } else {
          throw new SFException(ErrorCode.FEATURE_UNSUPPORTED, "data type: " + type);
        }
      case Types.ARRAY:
        if (StructureTypeHelper.isStructureTypeEnabled()) {
          return getArray(columnIndex);
        } else {
          throw new SFException(ErrorCode.FEATURE_UNSUPPORTED, "data type: " + type);
        }

      default:
        throw new SFException(ErrorCode.FEATURE_UNSUPPORTED, "data type: " + type);
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

  @Override
  public Array getArray(int columnIndex) throws SFException {
    Object obj = getObjectInternal(columnIndex);
    return getArrayInternal((String) obj, columnIndex);
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

  @Override
  @SnowflakeJdbcInternalApi
  public SQLInput createSqlInputForColumn(Object input, int columnIndex, SFBaseSession session) {
    return new JsonSqlInput(
            OBJECT_MAPPER.convertValue(input, JsonNode.class),
            session,
            converters,
            resultSetMetaData
                    .getColumnMetadata()
                    .get(columnIndex - 1)
                    .getFields(),
            sessionTimezone);
  }

  @Override
  @SnowflakeJdbcInternalApi
  public Date convertToDate(Object object, TimeZone tz) throws SFException {
    return (Date) converters.dateStringConverter(session).convert(object);
  }

  @Override
  @SnowflakeJdbcInternalApi
  public Time convertToTime(Object object, int scale) throws SFException {
    return (Time) converters.timeFromStringConverter(session).convert(object);
  }

  @Override
  @SnowflakeJdbcInternalApi
  public Timestamp convertToTimestamp(
          Object object, int columnType, int columnSubType, TimeZone tz, int scale) throws SFException {
    return (Timestamp)
            converters
                    .timestampFromStringConverter(columnSubType, columnType, scale, session, null, tz)
                    .convert(object);
  }

  private Timestamp getTimestamp(int columnIndex) throws SFException {
    return getTimestamp(columnIndex, TimeZone.getDefault());
  }

  @Override
  @SnowflakeJdbcInternalApi
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
          resultSetMetaData.getColumnMetadata().get(columnIndex - 1).getFields(),
          sessionTimezone);
    } catch (JsonProcessingException e) {
      throw new SFException(e, ErrorCode.INVALID_STRUCT_DATA);
    }
  }

  private SfSqlArray getArrayInternal(String obj, int columnIndex) throws SFException {
    try {
      SnowflakeColumnMetadata arrayMetadata =
          resultSetMetaData.getColumnMetadata().get(columnIndex - 1);
      FieldMetadata fieldMetadata = arrayMetadata.getFields().get(0);

      int columnSubType = fieldMetadata.getType();
      int columnType = ColumnTypeHelper.getColumnType(columnSubType, session);
      int scale = fieldMetadata.getScale();

      ArrayNode arrayNode = (ArrayNode) OBJECT_MAPPER.readTree(obj);
      Iterator<JsonNode> nodeElements = arrayNode.elements();

      switch (columnSubType) {
        case Types.INTEGER:
          return new SfSqlArray(
              columnSubType,
              getStream(nodeElements, converters.integerConverter(columnType))
                  .toArray(Integer[]::new));
        case Types.SMALLINT:
          return new SfSqlArray(
              columnSubType,
              getStream(nodeElements, converters.smallIntConverter(columnType))
                  .toArray(Short[]::new));
        case Types.TINYINT:
          return new SfSqlArray(
              columnSubType,
              getStream(nodeElements, converters.tinyIntConverter(columnType))
                  .toArray(Byte[]::new));
        case Types.BIGINT:
          return new SfSqlArray(
              columnSubType,
              getStream(nodeElements, converters.bigIntConverter(columnType)).toArray(Long[]::new));
        case Types.DECIMAL:
        case Types.NUMERIC:
          return new SfSqlArray(
              columnSubType,
              convertToFixedArray(getStream(nodeElements, converters.bigDecimalConverter(columnType))));
        case Types.CHAR:
        case Types.VARCHAR:
        case Types.LONGNVARCHAR:
          return new SfSqlArray(
              columnSubType,
              getStream(nodeElements, converters.varcharConverter(columnType, columnSubType, scale))
                  .toArray(String[]::new));
        case Types.BINARY:
          return new SfSqlArray(
              columnSubType,
              getStream(nodeElements, converters.bytesConverter(columnType, scale))
                  .toArray(Byte[][]::new));
        case Types.FLOAT:
        case Types.REAL:
          return new SfSqlArray(
              columnSubType,
              getStream(nodeElements, converters.floatConverter(columnType)).toArray(Float[]::new));
        case Types.DOUBLE:
          return new SfSqlArray(
              columnSubType,
              getStream(nodeElements, converters.doubleConverter(columnType))
                  .toArray(Double[]::new));
        case Types.DATE:
          return new SfSqlArray(
              columnSubType,
              getStream(nodeElements, converters.dateStringConverter(session)).toArray(Date[]::new));
        case Types.TIME:
          return new SfSqlArray(
              columnSubType,
              getStream(nodeElements, converters.timeFromStringConverter(session)).toArray(Time[]::new));
        case Types.TIMESTAMP:
          return new SfSqlArray(
              columnSubType,
              getStream(
                      nodeElements,
                      converters.timestampFromStringConverter(
                          columnSubType, columnType, scale, session, null, sessionTimezone))
                  .toArray(Timestamp[]::new));
        case Types.BOOLEAN:
          return new SfSqlArray(
              columnSubType,
              getStream(nodeElements, converters.booleanConverter(columnType))
                  .toArray(Boolean[]::new));
        case Types.STRUCT:
          return new SfSqlArray(
              columnSubType,
              getStream(nodeElements, converters.structConverter(OBJECT_MAPPER))
                  .toArray(Map[]::new));
        case Types.ARRAY:
          return new SfSqlArray(
              columnSubType,
              getStream(nodeElements, converters.arrayConverter(OBJECT_MAPPER))
                  .toArray(Map[][]::new));
        default:
          throw new SFException(
              ErrorCode.FEATURE_UNSUPPORTED,
              "Can't construct array for data type: " + columnSubType);
      }
    } catch (JsonProcessingException e) {
      throw new SFException(e, ErrorCode.INVALID_STRUCT_DATA);
    }
  }

  private Stream getStream(Iterator nodeElements, Converter converter) {
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

  private Object[] convertToFixedArray(Stream inputStream) {
    AtomicInteger bigDecimalCount = new AtomicInteger();
    Object[] elements =
            inputStream
                    .peek(
                            elem -> {
                              if (elem instanceof BigDecimal) {
                                bigDecimalCount.incrementAndGet();
                              }
                            })
                    .toArray(
                            size -> {
                              boolean shouldReturnAsBigDecimal = bigDecimalCount.get() > 0;
                              Class<?> returnedClass =
                                      shouldReturnAsBigDecimal ? BigDecimal.class : Long.class;
                              return java.lang.reflect.Array.newInstance(returnedClass, size);
                            });
    return elements;
  }

  private static Object convert(Converter converter, JsonNode node) throws SFException {
    if (node.isValueNode()) {
      return converter.convert(node.asText());
    } else {
      return converter.convert(node.toString());
    }
  }
}
