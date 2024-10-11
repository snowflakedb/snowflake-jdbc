package net.snowflake.client.core.json;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.TimeZone;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SFSession;
import net.snowflake.client.jdbc.SnowflakeUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DateTimeConverterTest {
  private final TimeZone honoluluTimeZone =
      TimeZone.getTimeZone(ZoneId.of("Pacific/Honolulu")); // session time zone
  private final TimeZone nuukTimeZone = TimeZone.getTimeZone(ZoneId.of("America/Nuuk"));
  private final DateTimeConverter dateTimeConverter =
      new DateTimeConverter(honoluluTimeZone, new SFSession(), 1, true, false, false, false);
  private final DateTimeConverter dateTimeConverterWithUseSessionTimeZone =
      new DateTimeConverter(honoluluTimeZone, new SFSession(), 1, true, false, true, false);
  private final DateTimeConverter dateTimeConverterWithTreatNTZAsUTC =
      new DateTimeConverter(honoluluTimeZone, new SFSession(), 1, true, true, false, false);

  @Test
  public void testGetVariousTypesWhenNullObjectGiven() throws SFException {
    Assertions.assertNull(
        dateTimeConverter.getTimestamp(null, Types.TIMESTAMP, Types.TIMESTAMP, null, 0));
    Assertions.assertNull(
        dateTimeConverter.getTime(null, Types.TIMESTAMP, Types.TIMESTAMP, null, 0));
    Assertions.assertNull(
        dateTimeConverter.getDate(null, Types.TIMESTAMP, Types.TIMESTAMP, null, 0));
  }

  @Test
  public void testGetTimestampWithDefaultTimeZone() throws SFException {
    Assertions.assertEquals(
        Timestamp.valueOf(LocalDateTime.of(2023, 8, 9, 8, 2, 3)),
        dateTimeConverter.getTimestamp("1691568123", Types.TIMESTAMP, Types.TIMESTAMP, null, 0));
    Assertions.assertEquals(
        Timestamp.valueOf(LocalDateTime.of(2023, 8, 9, 8, 2, 3, 456789000)),
        dateTimeConverter.getTimestamp(
            "1691568123.456789", Types.TIMESTAMP, Types.TIMESTAMP, null, 0));
    Assertions.assertEquals(
        Timestamp.valueOf(LocalDateTime.of(2023, 8, 9, 8, 2, 3, 456789123)),
        dateTimeConverter.getTimestamp(
            "1691568123.456789123", Types.TIMESTAMP, Types.TIMESTAMP, null, 0));
  }

  @Test
  public void testGetTimestampWithSpecificTimeZone() throws SFException {
    Assertions.assertEquals(
        Timestamp.valueOf(LocalDateTime.of(2023, 8, 9, 8, 2, 3)).toString(),
        dateTimeConverterWithTreatNTZAsUTC
            .getTimestamp("1691568123", Types.TIMESTAMP, Types.TIMESTAMP, nuukTimeZone, 0)
            .toString());
    Assertions.assertEquals(
        Timestamp.valueOf(LocalDateTime.of(2023, 8, 9, 8, 2, 3, 456789000)).toString(),
        dateTimeConverterWithTreatNTZAsUTC
            .getTimestamp("1691568123.456789", Types.TIMESTAMP, Types.TIMESTAMP, nuukTimeZone, 0)
            .toString());
    Assertions.assertEquals(
        Timestamp.valueOf(LocalDateTime.of(2023, 8, 9, 8, 2, 3, 456789123)).toString(),
        dateTimeConverterWithTreatNTZAsUTC
            .getTimestamp("1691568123.456789123", Types.TIMESTAMP, Types.TIMESTAMP, nuukTimeZone, 0)
            .toString());
  }

  // TODO replace equality when SNOW-991418 is fixed
  @Test
  public void testGetTimeWithDefaultTimeZone() throws SFException {
    Assertions.assertEquals(
        Time.valueOf(LocalTime.of(8, 2, 3)).toString(),
        dateTimeConverter
            .getTime("1691568123", Types.TIMESTAMP, Types.TIMESTAMP, null, 0)
            .toString());
    Assertions.assertEquals(
        Time.valueOf(LocalTime.of(8, 2, 3)).toString(),
        dateTimeConverter
            .getTime("1691568123.456789", Types.TIMESTAMP, Types.TIMESTAMP, null, 0)
            .toString());
    Assertions.assertEquals(
        Time.valueOf(LocalTime.of(8, 2, 3)).toString(),
        dateTimeConverter
            .getTime("1691568123.456789123", Types.TIMESTAMP, Types.TIMESTAMP, null, 0)
            .toString());
  }

  @Test
  public void testGetTimeWithSessionTimeZone() throws SFException {
    Time expected = Time.valueOf(LocalTime.of(22, 2, 3));
    Time actual =
        dateTimeConverterWithUseSessionTimeZone.getTime(
            "1691568123", Types.TIMESTAMP, SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_LTZ, null, 0);
    Assertions.assertEquals(expected.toString(), actual.toString());
  }

  @Test
  public void testGetDateWithDefaultTimeZone() throws SFException {
    Assertions.assertEquals(
        Date.valueOf(LocalDate.of(2023, 8, 9)).toString(),
        dateTimeConverter
            .getDate("1691568123", Types.TIMESTAMP, Types.TIMESTAMP, null, 0)
            .toString());
    Assertions.assertEquals(
        Date.valueOf(LocalDate.of(2023, 8, 9)).toString(),
        dateTimeConverter
            .getDate("1691568123.456789", Types.TIMESTAMP, Types.TIMESTAMP, null, 0)
            .toString());
    Assertions.assertEquals(
        Date.valueOf(LocalDate.of(2023, 8, 9)).toString(),
        dateTimeConverter
            .getDate("1691568123.456789123", Types.TIMESTAMP, Types.TIMESTAMP, null, 0)
            .toString());
  }

  @Test
  public void testGetDateWithSpecificTimeZone() throws SFException {
    Assertions.assertEquals(
        Date.valueOf(LocalDate.of(2023, 8, 9)).toString(),
        dateTimeConverter
            .getDate("1691568123", Types.TIMESTAMP, Types.TIMESTAMP, nuukTimeZone, 0)
            .toString());
    Assertions.assertEquals(
        Date.valueOf(LocalDate.of(2023, 8, 9)).toString(),
        dateTimeConverter
            .getDate("1691568123.456789", Types.TIMESTAMP, Types.TIMESTAMP, nuukTimeZone, 0)
            .toString());
    Assertions.assertEquals(
        Date.valueOf(LocalDate.of(2023, 8, 9)).toString(),
        dateTimeConverter
            .getDate("1691568123.456789123", Types.TIMESTAMP, Types.TIMESTAMP, nuukTimeZone, 0)
            .toString());
  }

  @Test
  public void testGetDateWithSessionTimeZone() throws SFException {
    Date expected = Date.valueOf(LocalDate.of(2023, 8, 8));
    Date actual =
        dateTimeConverterWithUseSessionTimeZone.getDate(
            "1691568123", Types.TIMESTAMP, SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_LTZ, null, 0);
    Assertions.assertEquals(expected.toString(), actual.toString());
  }
}
