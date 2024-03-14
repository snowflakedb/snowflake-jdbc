package net.snowflake.client.core.json;

import java.util.TimeZone;
import net.snowflake.client.core.SFBaseSession;
import net.snowflake.common.core.SFBinaryFormat;
import net.snowflake.common.core.SnowflakeDateTimeFormat;

public class Converters {
  private final BooleanConverter booleanConverter;
  private final NumberConverter numberConverter;
  private final DateTimeConverter dateTimeConverter;
  private final BytesConverter bytesConverter;
  private final StringConverter stringConverter;

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
}
