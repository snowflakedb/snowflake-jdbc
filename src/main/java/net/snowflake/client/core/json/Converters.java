package net.snowflake.client.core.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Map;
import java.util.TimeZone;
import net.snowflake.client.core.SFBaseSession;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.core.SqlInputTimestampUtil;
import net.snowflake.client.core.arrow.StructuredTypeDateTimeConverter;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeResultSetSerializableV1;
import net.snowflake.client.util.JsonStringToTypeConverter;
import net.snowflake.common.core.SFBinaryFormat;
import net.snowflake.common.core.SFTimestamp;
import net.snowflake.common.core.SnowflakeDateTimeFormat;

public class Converters {
  private final BooleanConverter booleanConverter;
  private final NumberConverter numberConverter;
  private final DateTimeConverter dateTimeConverter;
  private final BytesConverter bytesConverter;
  private final StringConverter stringConverter;
  private final StructuredTypeDateTimeConverter structuredTypeDateTimeConverter;

  public Converters(
      TimeZone sessionTimeZone,
      SFBaseSession session,
      long resultVersion,
      boolean honorClientTZForTimestampNTZ,
      boolean treatNTZAsUTC,
      boolean useSessionTimezone,
      boolean formatDateWithTimeZone,
      SFBinaryFormat binaryFormatter,
      SnowflakeDateTimeFormat dateFormatter,
      SnowflakeDateTimeFormat timeFormatter,
      SnowflakeDateTimeFormat timestampNTZFormatter,
      SnowflakeDateTimeFormat timestampLTZFormatter,
      SnowflakeDateTimeFormat timestampTZFormatter) {
    booleanConverter = new BooleanConverter();
    numberConverter = new NumberConverter();
    dateTimeConverter =
        new DateTimeConverter(
            sessionTimeZone,
            session,
            resultVersion,
            honorClientTZForTimestampNTZ,
            treatNTZAsUTC,
            useSessionTimezone,
            formatDateWithTimeZone);
    bytesConverter = new BytesConverter(this);
    stringConverter =
        new StringConverter(
            sessionTimeZone,
            binaryFormatter,
            dateFormatter,
            timeFormatter,
            timestampNTZFormatter,
            timestampLTZFormatter,
            timestampTZFormatter,
            resultVersion,
            session,
            this);
    structuredTypeDateTimeConverter =
        new StructuredTypeDateTimeConverter(
            sessionTimeZone,
            resultVersion,
            honorClientTZForTimestampNTZ,
            treatNTZAsUTC,
            useSessionTimezone,
            formatDateWithTimeZone);
  }

  @SnowflakeJdbcInternalApi
  public Converters(SFBaseSession session, SnowflakeResultSetSerializableV1 resultSetSerializable) {
    this(
        resultSetSerializable.getTimeZone(),
        session,
        resultSetSerializable.getResultVersion(),
        resultSetSerializable.isHonorClientTZForTimestampNTZ(),
        resultSetSerializable.getTreatNTZAsUTC(),
        resultSetSerializable.getUseSessionTimezone(),
        resultSetSerializable.getFormatDateWithTimeZone(),
        resultSetSerializable.getBinaryFormatter(),
        resultSetSerializable.getDateFormatter(),
        resultSetSerializable.getTimeFormatter(),
        resultSetSerializable.getTimestampNTZFormatter(),
        resultSetSerializable.getTimestampLTZFormatter(),
        resultSetSerializable.getTimestampTZFormatter());
  }

  public BooleanConverter getBooleanConverter() {
    return booleanConverter;
  }

  public NumberConverter getNumberConverter() {
    return numberConverter;
  }

  public DateTimeConverter getDateTimeConverter() {
    return dateTimeConverter;
  }

  public BytesConverter getBytesConverter() {
    return bytesConverter;
  }

  public StringConverter getStringConverter() {
    return stringConverter;
  }

  public StructuredTypeDateTimeConverter getStructuredTypeDateTimeConverter() {
    return structuredTypeDateTimeConverter;
  }

  @SnowflakeJdbcInternalApi
  public JsonStringToTypeConverter integerConverter(int columnType) {
    return value -> getNumberConverter().getInt(value, columnType);
  }

  @SnowflakeJdbcInternalApi
  public JsonStringToTypeConverter smallIntConverter(int columnType) {
    return value -> getNumberConverter().getShort(value, columnType);
  }

  @SnowflakeJdbcInternalApi
  public JsonStringToTypeConverter tinyIntConverter(int columnType) {
    return value -> getNumberConverter().getByte(value);
  }

  @SnowflakeJdbcInternalApi
  public JsonStringToTypeConverter bigIntConverter(int columnType) {
    return value -> getNumberConverter().getBigInt(value, columnType);
  }

  @SnowflakeJdbcInternalApi
  public JsonStringToTypeConverter longConverter(int columnType) {
    return value -> getNumberConverter().getLong(value, columnType);
  }

  @SnowflakeJdbcInternalApi
  public JsonStringToTypeConverter bigDecimalConverter(int columnType) {
    return value -> getNumberConverter().getBigDecimal(value, columnType);
  }

  @SnowflakeJdbcInternalApi
  public JsonStringToTypeConverter floatConverter(int columnType) {
    return value -> getNumberConverter().getBigDecimal(value, columnType);
  }

  @SnowflakeJdbcInternalApi
  public JsonStringToTypeConverter doubleConverter(int columnType) {
    return value -> getNumberConverter().getBigDecimal(value, columnType);
  }

  @SnowflakeJdbcInternalApi
  public JsonStringToTypeConverter bytesConverter(int columnType, int scale) {
    return value -> {
      byte[] primitiveArray = getBytesConverter().getBytes(value, columnType, Types.BINARY, scale);
      Byte[] newByteArray = new Byte[primitiveArray.length];
      Arrays.setAll(newByteArray, n -> primitiveArray[n]);
      return newByteArray;
    };
  }

  @SnowflakeJdbcInternalApi
  public JsonStringToTypeConverter varcharConverter(int columnType, int columnSubType, int scale) {
    return value -> getStringConverter().getString(value, columnType, columnSubType, scale);
  }

  @SnowflakeJdbcInternalApi
  public JsonStringToTypeConverter booleanConverter(int columnType) {
    return value -> getBooleanConverter().getBoolean(value, columnType);
  }

  @SnowflakeJdbcInternalApi
  public JsonStringToTypeConverter dateConverter(SFBaseSession session) {
    return value -> {
      SnowflakeDateTimeFormat formatter =
          SnowflakeDateTimeFormat.fromSqlFormat(
              (String) session.getCommonParameters().get("DATE_OUTPUT_FORMAT"));
      SFTimestamp timestamp = formatter.parse(value);
      return Date.valueOf(
          Instant.ofEpochMilli(timestamp.getTime()).atZone(ZoneOffset.UTC).toLocalDate());
    };
  }

  @SnowflakeJdbcInternalApi
  public JsonStringToTypeConverter timeConverter(SFBaseSession session) {
    return value -> {
      SnowflakeDateTimeFormat formatter =
          SnowflakeDateTimeFormat.fromSqlFormat(
              (String) session.getCommonParameters().get("TIME_OUTPUT_FORMAT"));
      SFTimestamp timestamp = formatter.parse(value);
      return Time.valueOf(
          Instant.ofEpochMilli(timestamp.getTime()).atZone(ZoneOffset.UTC).toLocalTime());
    };
  }

  @SnowflakeJdbcInternalApi
  public JsonStringToTypeConverter timestampConverter(
      int columnSubType,
      int columnType,
      int scale,
      SFBaseSession session,
      TimeZone tz,
      TimeZone sessionTimezone) {
    return value -> {
      Timestamp result =
          SqlInputTimestampUtil.getTimestampFromType(
              columnSubType, (String) value, session, sessionTimezone, tz);
      if (result != null) {
        return result;
      }
      return getDateTimeConverter()
          .getTimestamp(value, columnType, columnSubType, TimeZone.getDefault(), scale);
    };
  }

  @SnowflakeJdbcInternalApi
  public JsonStringToTypeConverter structConverter(ObjectMapper objectMapper) {
    return value -> {
      try {
        return objectMapper.readValue(value, Map.class);
      } catch (JsonProcessingException e) {
        throw new SFException(e, ErrorCode.INVALID_STRUCT_DATA);
      }
    };
  }

  @SnowflakeJdbcInternalApi
  public JsonStringToTypeConverter arrayConverter(ObjectMapper objectMapper) {
    return value -> {
      try {
        return objectMapper.readValue(value, Map[].class);
      } catch (JsonProcessingException e) {
        throw new SFException(e, ErrorCode.INVALID_STRUCT_DATA);
      }
    };
  }
}
