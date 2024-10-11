package net.snowflake.client.core.json;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

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
    assertNull(dateTimeConverter.getTimestamp(null, Types.TIMESTAMP, Types.TIMESTAMP, null, 0));
    assertNull(dateTimeConverter.getTime(null, Types.TIMESTAMP, Types.TIMESTAMP, null, 0));
    assertNull(dateTimeConverter.getDate(null, Types.TIMESTAMP, Types.TIMESTAMP, null, 0));
  }

  @Test
  public void testGetTimestampWithDefaultTimeZone() throws SFException {
    assertEquals(
        Timestamp.valueOf(LocalDateTime.of(2023, 8, 9, 8, 2, 3)),
        dateTimeConverter.getTimestamp("1691568123", Types.TIMESTAMP, Types.TIMESTAMP, null, 0));
    assertEquals(
        Timestamp.valueOf(LocalDateTime.of(2023, 8, 9, 8, 2, 3, 456789000)),
        dateTimeConverter.getTimestamp(
            "1691568123.456789", Types.TIMESTAMP, Types.TIMESTAMP, null, 0));
    assertEquals(
        Timestamp.valueOf(LocalDateTime.of(2023, 8, 9, 8, 2, 3, 456789123)),
        dateTimeConverter.getTimestamp(
            "1691568123.456789123", Types.TIMESTAMP, Types.TIMESTAMP, null, 0));
  }

  @Test
  public void testGetTimestampWithSpecificTimeZone() throws SFException {
    assertEquals(
        Timestamp.valueOf(LocalDateTime.of(2023, 8, 9, 8, 2, 3)).toString(),
        dateTimeConverterWithTreatNTZAsUTC
            .getTimestamp("1691568123", Types.TIMESTAMP, Types.TIMESTAMP, nuukTimeZone, 0)
            .toString());
    assertEquals(
        Timestamp.valueOf(LocalDateTime.of(2023, 8, 9, 8, 2, 3, 456789000)).toString(),
        dateTimeConverterWithTreatNTZAsUTC
            .getTimestamp("1691568123.456789", Types.TIMESTAMP, Types.TIMESTAMP, nuukTimeZone, 0)
            .toString());
    assertEquals(
        Timestamp.valueOf(LocalDateTime.of(2023, 8, 9, 8, 2, 3, 456789123)).toString(),
        dateTimeConverterWithTreatNTZAsUTC
            .getTimestamp("1691568123.456789123", Types.TIMESTAMP, Types.TIMESTAMP, nuukTimeZone, 0)
            .toString());
  }

  // TODO replace equality when SNOW-991418 is fixed
  @Test
  public void testGetTimeWithDefaultTimeZone() throws SFException {
    assertEquals(
        Time.valueOf(LocalTime.of(8, 2, 3)).toString(),
        dateTimeConverter
            .getTime("1691568123", Types.TIMESTAMP, Types.TIMESTAMP, null, 0)
            .toString());
    assertEquals(
        Time.valueOf(LocalTime.of(8, 2, 3)).toString(),
        dateTimeConverter
            .getTime("1691568123.456789", Types.TIMESTAMP, Types.TIMESTAMP, null, 0)
            .toString());
    assertEquals(
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
    assertEquals(expected.toString(), actual.toString());
  }

  @Test
  public void testGetDateWithDefaultTimeZone() throws SFException {
    assertEquals(
        Date.valueOf(LocalDate.of(2023, 8, 9)).toString(),
        dateTimeConverter
            .getDate("1691568123", Types.TIMESTAMP, Types.TIMESTAMP, null, 0)
            .toString());
    assertEquals(
        Date.valueOf(LocalDate.of(2023, 8, 9)).toString(),
        dateTimeConverter
            .getDate("1691568123.456789", Types.TIMESTAMP, Types.TIMESTAMP, null, 0)
            .toString());
    assertEquals(
        Date.valueOf(LocalDate.of(2023, 8, 9)).toString(),
        dateTimeConverter
            .getDate("1691568123.456789123", Types.TIMESTAMP, Types.TIMESTAMP, null, 0)
            .toString());
  }

  @Test
  public void testGetDateWithSpecificTimeZone() throws SFException {
    assertEquals(
        Date.valueOf(LocalDate.of(2023, 8, 9)).toString(),
        dateTimeConverter
            .getDate("1691568123", Types.TIMESTAMP, Types.TIMESTAMP, nuukTimeZone, 0)
            .toString());
    assertEquals(
        Date.valueOf(LocalDate.of(2023, 8, 9)).toString(),
        dateTimeConverter
            .getDate("1691568123.456789", Types.TIMESTAMP, Types.TIMESTAMP, nuukTimeZone, 0)
            .toString());
    assertEquals(
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
    assertEquals(expected.toString(), actual.toString());
  }
}
