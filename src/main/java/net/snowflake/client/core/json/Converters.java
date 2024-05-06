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
import net.snowflake.client.core.SfTimestampUtil;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.core.arrow.StructuredTypeDateTimeConverter;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeResultSetSerializableV1;
import net.snowflake.client.util.Converter;
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
  public Converter integerConverter(int columnType) {
    return value -> getNumberConverter().getInt(value, columnType);
  }

  @SnowflakeJdbcInternalApi
  public Converter smallIntConverter(int columnType) {
    return value -> getNumberConverter().getShort(value, columnType);
  }

  @SnowflakeJdbcInternalApi
  public Converter tinyIntConverter(int columnType) {
    return value -> getNumberConverter().getByte(value);
  }

  @SnowflakeJdbcInternalApi
  public Converter bigIntConverter(int columnType) {
    return value -> getNumberConverter().getBigInt(value, columnType);
  }

  @SnowflakeJdbcInternalApi
  public Converter longConverter(int columnType) {
    return value -> getNumberConverter().getLong(value, columnType);
  }

  @SnowflakeJdbcInternalApi
  public Converter bigDecimalConverter(int columnType) {
    return value -> getNumberConverter().getBigDecimal(value, columnType);
  }

  @SnowflakeJdbcInternalApi
  public Converter floatConverter(int columnType) {
    return value -> getNumberConverter().getFloat(value, columnType);
  }

  @SnowflakeJdbcInternalApi
  public Converter doubleConverter(int columnType) {
    return value -> getNumberConverter().getDouble(value, columnType);
  }

  @SnowflakeJdbcInternalApi
  public Converter bytesConverter(int columnType, int scale) {
    return value -> {
      byte[] primitiveArray = getBytesConverter().getBytes(value, columnType, Types.BINARY, scale);
      Byte[] newByteArray = new Byte[primitiveArray.length];
      Arrays.setAll(newByteArray, n -> primitiveArray[n]);
      return newByteArray;
    };
  }

  @SnowflakeJdbcInternalApi
  public Converter varcharConverter(int columnType, int columnSubType, int scale) {
    return value -> getStringConverter().getString(value, columnType, columnSubType, scale);
  }

  @SnowflakeJdbcInternalApi
  public Converter booleanConverter(int columnType) {
    return value -> getBooleanConverter().getBoolean(value, columnType);
  }

  @SnowflakeJdbcInternalApi
  public Converter dateStringConverter(SFBaseSession session) {
    return value -> {
      SnowflakeDateTimeFormat formatter =
          SnowflakeDateTimeFormat.fromSqlFormat(
              (String) session.getCommonParameters().get("DATE_OUTPUT_FORMAT"));
      SFTimestamp timestamp = formatter.parse((String) value);
      return Date.valueOf(
          Instant.ofEpochMilli(timestamp.getTime()).atZone(ZoneOffset.UTC).toLocalDate());
    };
  }

  @SnowflakeJdbcInternalApi
  public Converter dateFromIntConverter(TimeZone tz) {
    return value -> structuredTypeDateTimeConverter.getDate((Integer) value, tz);
  }

  @SnowflakeJdbcInternalApi
  public Converter timeFromStringConverter(SFBaseSession session) {
    return value -> {
      SnowflakeDateTimeFormat formatter =
          SnowflakeDateTimeFormat.fromSqlFormat(
              (String) session.getCommonParameters().get("TIME_OUTPUT_FORMAT"));
      SFTimestamp timestamp = formatter.parse((String) value);
      return Time.valueOf(
          Instant.ofEpochMilli(timestamp.getTime()).atZone(ZoneOffset.UTC).toLocalTime());
    };
  }

  public Converter timeFromIntConverter(int scale) {
    return value -> structuredTypeDateTimeConverter.getTime((Long) value, scale);
  }

  @SnowflakeJdbcInternalApi
  public Converter timestampFromStringConverter(
      int columnSubType,
      int columnType,
      int scale,
      SFBaseSession session,
      TimeZone tz,
      TimeZone sessionTimezone) {
    return value -> {
      Timestamp result =
          SfTimestampUtil.getTimestampFromType(
              columnSubType, (String) value, session, sessionTimezone, tz);
      if (result != null) {
        return result;
      }
      return getDateTimeConverter()
          .getTimestamp(value, columnType, columnSubType, TimeZone.getDefault(), scale);
    };
  }

  public Converter timestampFromStructConverter(
      int columnType, int columnSubType, TimeZone tz, int scale) {
    return value ->
        structuredTypeDateTimeConverter.getTimestamp(
            (Map<String, Object>) value, columnType, columnSubType, tz, scale);
  }

  @SnowflakeJdbcInternalApi
  public Converter structConverter(ObjectMapper objectMapper) {
    return value -> {
      try {
        return objectMapper.readValue((String) value, Map.class);
      } catch (JsonProcessingException e) {
        throw new SFException(e, ErrorCode.INVALID_STRUCT_DATA);
      }
    };
  }

  @SnowflakeJdbcInternalApi
  public Converter arrayConverter(ObjectMapper objectMapper) {
    return value -> {
      try {
        return objectMapper.readValue((String) value, Map[].class);
      } catch (JsonProcessingException e) {
        throw new SFException(e, ErrorCode.INVALID_STRUCT_DATA);
      }
    };
  }
}
