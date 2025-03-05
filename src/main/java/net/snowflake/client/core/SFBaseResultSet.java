package net.snowflake.client.core;

import static net.snowflake.client.jdbc.SnowflakeUtil.getJsonNodeStringValue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.SQLInput;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
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
import net.snowflake.client.jdbc.SnowflakeResultSetSerializable;
import net.snowflake.client.jdbc.SnowflakeResultSetSerializableV1;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.client.util.Converter;
import net.snowflake.common.core.SFBinaryFormat;
import net.snowflake.common.core.SnowflakeDateTimeFormat;

/** Base class for query result set and metadata result set */
public abstract class SFBaseResultSet {
  private static final SFLogger logger = SFLoggerFactory.getLogger(SFBaseResultSet.class);
  protected static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getObjectMapper();

  boolean wasNull = false;

  protected SFResultSetMetaData resultSetMetaData = null;

  protected int row = 0;

  protected Map<String, Object> parameters = new HashMap<>();

  // Formatters for different datatypes
  // TODO move all formatter to DataConversionContext.java
  SnowflakeDateTimeFormat timestampNTZFormatter;
  SnowflakeDateTimeFormat timestampLTZFormatter;
  SnowflakeDateTimeFormat timestampTZFormatter;
  SnowflakeDateTimeFormat dateFormatter;
  SnowflakeDateTimeFormat timeFormatter;
  boolean honorClientTZForTimestampNTZ = true;

  SFBinaryFormat binaryFormatter;

  protected long resultVersion = 0;

  protected int numberOfBinds = 0;

  protected List<MetaDataOfBinds> metaDataOfBinds = new ArrayList<>();

  // For creating incidents
  protected SFBaseSession session;

  // indicate whether the result set has been closed or not.
  protected boolean isClosed;

  // The serializable object which can serialize the metadata for this
  // result set
  protected SnowflakeResultSetSerializableV1 resultSetSerializable;

  protected TimeZone sessionTimeZone;

  public abstract boolean isLast();

  public abstract boolean isAfterLast();

  public abstract String getString(int columnIndex) throws SFException;

  public abstract boolean getBoolean(int columnIndex) throws SFException;

  public abstract byte getByte(int columnIndex) throws SFException;

  public abstract short getShort(int columnIndex) throws SFException;

  public abstract int getInt(int columnIndex) throws SFException;

  public abstract long getLong(int columnIndex) throws SFException;

  public abstract float getFloat(int columnIndex) throws SFException;

  public abstract double getDouble(int columnIndex) throws SFException;

  public abstract byte[] getBytes(int columnIndex) throws SFException;

  public abstract Time getTime(int columnIndex) throws SFException;

  public abstract Timestamp getTimestamp(int columnIndex, TimeZone tz) throws SFException;

  public abstract Date getDate(int columnIndex, TimeZone tz) throws SFException;

  public abstract Object getObject(int columnIndex) throws SFException;

  @SnowflakeJdbcInternalApi
  public abstract Object getObjectWithoutString(int columnIndex) throws SFException;

  public Array getArray(int columnIndex) throws SFException {
    throw new UnsupportedOperationException();
  }

  public abstract BigDecimal getBigDecimal(int columnIndex) throws SFException;

  public abstract BigDecimal getBigDecimal(int columnIndex, int scale) throws SFException;

  public abstract SFStatementType getStatementType();

  // this is useful to override the initial statement type if it is incorrect
  // (e.g. result scan yields a query type, but the results are from a DML statement)
  public abstract void setStatementType(SFStatementType statementType) throws SQLException;

  public abstract String getQueryId();

  public void setSession(SFBaseSession session) {
    this.session = session;
  }

  public SFBaseSession getSession() {
    return this.session;
  }

  // default implementation
  public boolean next() throws SFException, SnowflakeSQLException {
    logger.trace("boolean next()", false);
    return false;
  }

  public void close() throws SnowflakeSQLException {
    logger.trace("void close()", false);

    // no exception even if already closed.
    resultSetMetaData = null;
    isClosed = true;
  }

  public boolean wasNull() {
    logger.trace("boolean wasNull() returning {}", wasNull);

    return wasNull;
  }

  public SFResultSetMetaData getMetaData() {
    return resultSetMetaData;
  }

  public TimeZone getSessionTimezone() {
    return sessionTimeZone;
  }

  public int getRow() throws SQLException {
    return row;
  }

  public boolean absolute(int row) throws SFException {
    throw new SFException(ErrorCode.FEATURE_UNSUPPORTED, "seek to a specific row");
  }

  public boolean relative(int rows) throws SFException {
    throw new SFException(ErrorCode.FEATURE_UNSUPPORTED, "seek to a row relative to current row");
  }

  public boolean previous() throws SFException {
    throw new SFException(ErrorCode.FEATURE_UNSUPPORTED, "seek to a previous row");
  }

  public int getNumberOfBinds() {
    return numberOfBinds;
  }

  public List<MetaDataOfBinds> getMetaDataOfBinds() {
    return metaDataOfBinds;
  }

  public boolean isFirst() {
    return row == 1;
  }

  public boolean isBeforeFirst() {
    return row == 0;
  }

  public boolean isClosed() {
    return isClosed;
  }

  public boolean isArrayBindSupported() {
    return false;
  }

  /**
   * Split this whole SnowflakeResultSetSerializable into small pieces based on the user specified
   * data size.
   *
   * @param maxSizeInBytes The expected max data size wrapped in the ResultSetSerializables object.
   *     NOTE: this parameter is intended to make the data size in each serializable object to be
   *     less than it. But if user specifies a small value which may be smaller than the data size
   *     of one result chunk. So the definition can't be guaranteed completely. For this special
   *     case, one serializable object is used to wrap the data chunk.
   * @return a list of SnowflakeResultSetSerializable
   * @throws SQLException if fails to split objects.
   */
  public List<SnowflakeResultSetSerializable> getResultSetSerializables(long maxSizeInBytes)
      throws SQLException {
    return this.resultSetSerializable.splitBySize(maxSizeInBytes);
  }

  @SnowflakeJdbcInternalApi
  public Converters getConverters() {
    logger.debug("Json converters weren't created");
    return null;
  }

  @SnowflakeJdbcInternalApi
  public TimeZone getSessionTimeZone() {
    return resultSetSerializable.getTimeZone();
  }

  @SnowflakeJdbcInternalApi
  public SQLInput createSqlInputForColumn(
      Object input,
      Class<?> parentObjectClass,
      int columnIndex,
      SFBaseSession session,
      List<FieldMetadata> fields) {
    throw new UnsupportedOperationException();
  }

  @SnowflakeJdbcInternalApi
  public Date convertToDate(Object object, TimeZone tz) throws SFException {
    throw new UnsupportedOperationException();
  }

  @SnowflakeJdbcInternalApi
  public Time convertToTime(Object object, int scale) throws SFException {
    throw new UnsupportedOperationException();
  }

  @SnowflakeJdbcInternalApi
  public Timestamp convertToTimestamp(
      Object object, int columnType, int columnSubType, TimeZone tz, int scale) throws SFException {
    throw new UnsupportedOperationException();
  }

  @SnowflakeJdbcInternalApi
  protected SQLInput createJsonSqlInputForColumn(
      Object input, SFBaseSession session, List<FieldMetadata> fields) {
    JsonNode inputNode;
    if (input instanceof JsonNode) {
      inputNode = (JsonNode) input;
    } else {
      inputNode = OBJECT_MAPPER.convertValue(input, JsonNode.class);
    }
    return new JsonSqlInput(
        input.toString(), inputNode, session, getConverters(), fields, sessionTimeZone);
  }

  @SnowflakeJdbcInternalApi
  protected SfSqlArray getJsonArray(String arrayString, int columnIndex) throws SFException {
    try {
      List<FieldMetadata> fieldMetadataList = resultSetMetaData.getColumnFields(columnIndex);
      if (fieldMetadataList.size() != 1) {
        throw new SFException(
            ErrorCode.FEATURE_UNSUPPORTED,
            "Wrong size of fields for array type " + fieldMetadataList.size());
      }

      FieldMetadata fieldMetadata = fieldMetadataList.get(0);

      int columnSubType = fieldMetadata.getType();
      int columnType = ColumnTypeHelper.getColumnType(columnSubType, session);
      int scale = fieldMetadata.getScale();

      ArrayNode arrayNode = (ArrayNode) OBJECT_MAPPER.readTree(arrayString);
      Iterator<JsonNode> nodeElements = arrayNode.elements();

      switch (columnType) {
        case Types.INTEGER:
          return new SfSqlArray(
              arrayString,
              columnSubType,
              getStream(nodeElements, getConverters().integerConverter(columnType))
                  .toArray(Integer[]::new));
        case Types.SMALLINT:
          return new SfSqlArray(
              arrayString,
              columnSubType,
              getStream(nodeElements, getConverters().smallIntConverter(columnType))
                  .toArray(Short[]::new));
        case Types.TINYINT:
          return new SfSqlArray(
              arrayString,
              columnSubType,
              getStream(nodeElements, getConverters().tinyIntConverter(columnType))
                  .toArray(Byte[]::new));
        case Types.BIGINT:
          return new SfSqlArray(
              arrayString,
              columnSubType,
              getStream(nodeElements, getConverters().bigIntConverter(columnType))
                  .toArray(Long[]::new));
        case Types.DECIMAL:
        case Types.NUMERIC:
          return new SfSqlArray(
              arrayString,
              columnSubType,
              convertToFixedArray(
                  getStream(nodeElements, getConverters().bigDecimalConverter(columnType))));
        case Types.CHAR:
        case Types.VARCHAR:
        case Types.LONGNVARCHAR:
          return new SfSqlArray(
              arrayString,
              columnSubType,
              getStream(
                      nodeElements,
                      getConverters().varcharConverter(columnType, columnSubType, scale))
                  .toArray(String[]::new));
        case Types.BINARY:
          return new SfSqlArray(
              arrayString,
              columnSubType,
              getStream(nodeElements, getConverters().bytesConverter(columnType, scale))
                  .toArray(Byte[][]::new));
        case Types.FLOAT:
        case Types.REAL:
          return new SfSqlArray(
              arrayString,
              columnSubType,
              getStream(nodeElements, getConverters().floatConverter(columnType))
                  .toArray(Float[]::new));
        case Types.DOUBLE:
          return new SfSqlArray(
              arrayString,
              columnSubType,
              getStream(nodeElements, getConverters().doubleConverter(columnType))
                  .toArray(Double[]::new));
        case Types.DATE:
          return new SfSqlArray(
              arrayString,
              columnSubType,
              getStream(nodeElements, getConverters().dateStringConverter(session))
                  .toArray(Date[]::new));
        case Types.TIME:
          return new SfSqlArray(
              arrayString,
              columnSubType,
              getStream(nodeElements, getConverters().timeFromStringConverter(session))
                  .toArray(Time[]::new));
        case Types.TIMESTAMP:
          return new SfSqlArray(
              arrayString,
              columnSubType,
              getStream(
                      nodeElements,
                      getConverters()
                          .timestampFromStringConverter(
                              columnSubType, columnType, scale, session, null, sessionTimeZone))
                  .toArray(Timestamp[]::new));
        case Types.BOOLEAN:
          return new SfSqlArray(
              arrayString,
              columnSubType,
              getStream(nodeElements, getConverters().booleanConverter(columnType))
                  .toArray(Boolean[]::new));
        case Types.STRUCT:
          return new SfSqlArray(
              arrayString,
              columnSubType,
              getStream(nodeElements, getConverters().structConverter(OBJECT_MAPPER))
                  .toArray(Map[]::new));
        case Types.ARRAY:
          return new SfSqlArray(
              arrayString,
              columnSubType,
              getStream(nodeElements, getConverters().arrayConverter(OBJECT_MAPPER))
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

  @SnowflakeJdbcInternalApi
  protected Date convertStringToDate(String object, TimeZone tz) throws SFException {
    return (Date) getConverters().dateStringConverter(session).convert(object);
  }

  @SnowflakeJdbcInternalApi
  protected Time convertStringToTime(String object, int scale) throws SFException {
    return (Time) getConverters().timeFromStringConverter(session).convert(object);
  }

  @SnowflakeJdbcInternalApi
  protected Timestamp convertStringToTimestamp(
      String object, int columnType, int columnSubType, TimeZone tz, int scale) throws SFException {
    return (Timestamp)
        getConverters()
            .timestampFromStringConverter(columnSubType, columnType, scale, session, null, tz)
            .convert(object);
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

  private static Object convert(Converter converter, JsonNode node) throws SFException {
    String nodeValue = getJsonNodeStringValue(node);
    return converter.convert(nodeValue);
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
                  Class<?> returnedClass = shouldReturnAsBigDecimal ? BigDecimal.class : Long.class;
                  return java.lang.reflect.Array.newInstance(returnedClass, size);
                });
    return elements;
  }
}
