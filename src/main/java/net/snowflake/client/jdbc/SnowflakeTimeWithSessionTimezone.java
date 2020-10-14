/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.TimeZone;

/**
 * Time with toString() overridden to display time values in session timezone. Only relevant for
 * timestamp objects fetched as times. Normal time objects do not have a timezone associated with
 * them.
 */
public class SnowflakeTimeWithSessionTimezone extends Time {

  int nanos = 0;
  boolean useWallclockTime = false;
  ZoneOffset offset = ZoneOffset.UTC;

  public SnowflakeTimeWithSessionTimezone(long time, int nanos, boolean useWallclockTime) {
    super(time);
    this.nanos = nanos;
    this.useWallclockTime = useWallclockTime;
  }

  public SnowflakeTimeWithSessionTimezone(
      Timestamp ts, TimeZone sessionTimeZone, boolean useWallclockTime) {
    super(ts.getTime());
    this.nanos = ts.getNanos();
    this.useWallclockTime = useWallclockTime;
    if (sessionTimeZone != null) {
      this.offset = ZoneId.of(sessionTimeZone.getID()).getRules().getOffset(ts.toInstant());
    }
  }

  /**
   * Returns a string representation in UTC so as to display "wallclock time"
   *
   * @return a string representation of the object
   */
  public synchronized String toString() {
    if (!useWallclockTime) {
      return super.toString();
    }
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss");
    LocalDateTime ldt =
        LocalDateTime.ofEpochSecond(
            SnowflakeUtil.getSecondsFromMillis(this.getTime()), this.nanos, this.offset);
    return ldt.format(formatter);
  }
}
