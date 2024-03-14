package net.snowflake.client.core.arrow;

import static net.snowflake.client.jdbc.SnowflakeType.TIMESTAMP_LTZ;
import static net.snowflake.client.jdbc.SnowflakeType.TIMESTAMP_NTZ;
import static net.snowflake.client.jdbc.SnowflakeType.TIMESTAMP_TZ;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.TimeZone;
import net.snowflake.client.core.SFException;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeType;
import org.apache.arrow.vector.util.JsonStringHashMap;

public class StructuredTypeDateTimeConverter {

  private final TimeZone sessionTimeZone;
  private final long resultVersion;
  private final boolean honorClientTZForTimestampNTZ;
  private final boolean treatNTZAsUTC;
  private final boolean useSessionTimezone;
  private final boolean formatDateWithTimeZone;

  public StructuredTypeDateTimeConverter(
      TimeZone sessionTimeZone,
      long resultVersion,
      boolean honorClientTZForTimestampNTZ,
      boolean treatNTZAsUTC,
      boolean useSessionTimezone,
      boolean formatDateWithTimeZone) {

    this.sessionTimeZone = sessionTimeZone;
    this.resultVersion = resultVersion;
    this.honorClientTZForTimestampNTZ = honorClientTZForTimestampNTZ;
    this.treatNTZAsUTC = treatNTZAsUTC;
    this.useSessionTimezone = useSessionTimezone;
    this.formatDateWithTimeZone = formatDateWithTimeZone;
  }

  public Timestamp getTimestamp(
      JsonStringHashMap<String, Object> obj, SnowflakeType type, TimeZone tz, int scale)
      throws SFException {
    if (tz == null) {
      tz = TimeZone.getDefault();
    }
    switch (type) {
      case TIMESTAMP_LTZ:
        return convertTimestampLtz(obj, scale);
      case TIMESTAMP_NTZ:
        return convertTimestampNtz(obj, tz, scale);
      case TIMESTAMP_TZ:
        return convertTimestampTz(obj, scale);
    }
    throw new SFException(
        ErrorCode.INVALID_VALUE_CONVERT,
        "Unexpected Arrow Field for " + type.name() + " and object type " + obj.getClass());
  }

  public Date getDate(int value, TimeZone tz) throws SFException {
    return DateConverter.getDate(value, tz, sessionTimeZone, formatDateWithTimeZone);
  }

  public Time getTime(long value, int scale) throws SFException {
    return BigIntToTimeConverter.getTime(value, scale, useSessionTimezone);
  }

  private Timestamp convertTimestampLtz(Object obj, int scale) throws SFException {
    if (obj instanceof JsonStringHashMap) {
      JsonStringHashMap<String, Object> map = (JsonStringHashMap<String, Object>) obj;
      if (map.values().size() == 2) {
        return TwoFieldStructToTimestampLTZConverter.getTimestamp(
            (long) map.get("epoch"),
            (int) map.get("fraction"),
            false,
            sessionTimeZone,
            useSessionTimezone);
      }
    } else if (obj instanceof Long) {
      return BigIntToTimestampLTZConverter.getTimestamp((long) obj, scale);
    }
    throw new SFException(
        ErrorCode.INVALID_VALUE_CONVERT,
        "Unexpected Arrow Field for " + TIMESTAMP_LTZ + " and object type " + obj.getClass());
  }

  private Timestamp convertTimestampNtz(Object obj, TimeZone tz, int scale) throws SFException {
    if (obj instanceof JsonStringHashMap) {
      JsonStringHashMap<String, Object> map = (JsonStringHashMap<String, Object>) obj;
      if (map.values().size() == 2) {
        return TwoFieldStructToTimestampNTZConverter.getTimestamp(
            (long) map.get("epoch"),
            (int) map.get("fraction"),
            false,
            tz,
            sessionTimeZone,
            treatNTZAsUTC,
            useSessionTimezone,
            honorClientTZForTimestampNTZ);
      }
    } else if (obj instanceof Long) {
      return BigIntToTimestampNTZConverter.getTimestamp(
          (long) obj, tz, scale, honorClientTZForTimestampNTZ, false);
    }
    throw new SFException(
        ErrorCode.INVALID_VALUE_CONVERT,
        "Unexpected Arrow Field for " + TIMESTAMP_NTZ + " and object type " + obj.getClass());
  }

  private Timestamp convertTimestampTz(Object obj, int scale) throws SFException {
    if (obj instanceof JsonStringHashMap) {
      JsonStringHashMap<String, Object> map = (JsonStringHashMap<String, Object>) obj;
      if (map.values().size() == 2) {
        return TwoFieldStructToTimestampTZConverter.getTimestamp(
            (long) map.get("epoch"), (int) map.get("timezone"), scale);
      } else if (map.values().size() == 3) {
        return ThreeFieldStructToTimestampTZConverter.getTimestamp(
            (long) map.get("epoch"),
            (int) map.get("fraction"),
            (int) map.get("timezone"),
            false,
            resultVersion,
            useSessionTimezone);
      }
    }
    throw new SFException(
        ErrorCode.INVALID_VALUE_CONVERT,
        "Unexpected Arrow Field for " + TIMESTAMP_TZ + " and object type " + obj.getClass());
  }
}
