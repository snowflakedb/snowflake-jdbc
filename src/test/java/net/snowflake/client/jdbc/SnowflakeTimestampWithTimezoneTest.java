package net.snowflake.client.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

/**
 * Tests SnowflakeTimestampWithTimezone to ensure the output is not impacted by Day Light Saving
 * Time. Not this test case is not thread safe, because TimeZone.setDefault is called.
 */
public class SnowflakeTimestampWithTimezoneTest extends BaseJDBCTest {
  private static TimeZone orgTimeZone;

  static class Params implements ArgumentsProvider {
    public Stream<Arguments> provideArguments(ExtensionContext context) {
      String[] timeZoneList = {"PST", "America/New_York", "UTC", "Asia/Singapore"};

      String[] dateTimeList = {
        "2018-03-11 01:10:34.0123456",
        "2018-03-11 02:10:34.0123456",
        "2018-03-11 03:10:34.0123456",
        "2018-11-04 01:10:34.123",
        "2018-11-04 02:10:34.123",
        "2018-11-04 03:10:34.123",
        "2020-03-11 01:10:34.456",
        "2020-03-11 02:10:34.456",
        "2020-03-11 03:10:34.456",
        "2020-11-01 01:10:34.123",
        "2020-11-01 02:10:34.123",
        "2020-11-01 03:10:34.123"
      };

      List<Arguments> testCases = new ArrayList<>();
      for (String timeZone : timeZoneList) {
        for (String dateTime : dateTimeList) {
          testCases.add(Arguments.of(timeZone, dateTime, dateTime));
        }
      }
      return testCases.stream();
    }
  }

  /** Records the original TimeZone */
  @BeforeAll
  public static void keepOriginalTimeZone() {
    orgTimeZone = TimeZone.getDefault();
  }

  @AfterAll
  public static void restoreTimeZone() {
    TimeZone.setDefault(orgTimeZone);
  }

  @ParameterizedTest(name = "{index}: {1} {0}")
  @ArgumentsSource(Params.class)
  public void testTimestampNTZ(String timeZone, String inputTimestamp, String outputTimestamp) {
    TimeZone.setDefault(TimeZone.getTimeZone(timeZone));
    LocalDateTime dt = parseTimestampNTZ(inputTimestamp);
    SnowflakeTimestampWithTimezone stn =
        new SnowflakeTimestampWithTimezone(
            dt.toEpochSecond(ZoneOffset.UTC) * 1000, dt.getNano(), TimeZone.getTimeZone("UTC"));
    assertEquals(outputTimestamp, stn.toString());
  }

  @Test
  public void testGetTimezone() {
    String timezone = "Australia/Sydney";
    // Create a timestamp from a point in time
    Long datetime = System.currentTimeMillis();
    Timestamp currentTimestamp = new Timestamp(datetime);
    SnowflakeTimestampWithTimezone ts =
        new SnowflakeTimestampWithTimezone(currentTimestamp, TimeZone.getTimeZone(timezone));
    // verify timezone was set
    assertEquals(ts.getTimezone().getID(), timezone);
  }

  @Test
  public void testToZonedDateTime() {
    String timezone = "Australia/Sydney";
    String zonedDateTime = "2022-03-17T10:10:08+11:00[Australia/Sydney]";
    // Create a timestamp from a point in time
    Long datetime = 1647472208000L;
    Timestamp timestamp = new Timestamp(datetime);
    SnowflakeTimestampWithTimezone ts =
        new SnowflakeTimestampWithTimezone(timestamp, TimeZone.getTimeZone(timezone));
    ZonedDateTime zd = ts.toZonedDateTime();
    // verify timestamp was converted to zoned datetime
    assertEquals(zd.toString(), zonedDateTime);
  }
}
