/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static org.junit.Assert.assertEquals;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Tests SnowflakeTimestampNTZAsUTC to ensure the output is not impacted by Day Light Saving Time.
 * Not this test case is not thread safe, because TimeZone.setDefault is called.
 */
@RunWith(Parameterized.class)
public class SnowflakeTimestampNTZAsUTCTest extends BaseJDBCTest {
  private static TimeZone orgTimeZone;

  private final String timeZone;
  private final String inputTimestamp;
  private final String outputTimestamp;

  public SnowflakeTimestampNTZAsUTCTest(
      String timeZone, String inputTimestamp, String outputTimestamp) {
    this.timeZone = timeZone;
    this.inputTimestamp = inputTimestamp;
    this.outputTimestamp = outputTimestamp;
  }

  @Parameterized.Parameters(name = "tz={0}, input={1}, output={2}")
  public static Collection convert() {
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

    List<Object> testCases = new ArrayList<>();
    for (String timeZone : timeZoneList) {
      for (String dateTime : dateTimeList) {
        testCases.add(new Object[] {timeZone, dateTime, dateTime});
      }
    }
    return testCases;
  }

  /** Records the original TimeZone */
  @BeforeClass
  public static void keepOriginalTimeZone() {
    orgTimeZone = TimeZone.getDefault();
  }

  @AfterClass
  public static void restoreTimeZone() {
    TimeZone.setDefault(orgTimeZone);
  }

  @Test
  public void testTimestampNTZ() throws Throwable {
    TimeZone.setDefault(TimeZone.getTimeZone(timeZone));
    LocalDateTime dt = parseTimestampNTZ(this.inputTimestamp);
    SnowflakeTimestampNTZAsUTC stn =
        new SnowflakeTimestampNTZAsUTC(dt.toEpochSecond(ZoneOffset.UTC) * 1000, dt.getNano());
    assertEquals(this.outputTimestamp, stn.toString());
  }
}
