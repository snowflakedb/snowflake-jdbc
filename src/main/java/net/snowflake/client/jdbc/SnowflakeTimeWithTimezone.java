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
public class SnowflakeTimeWithTimezone extends Time {

  int nanos = 0;
  boolean useSessionTimeZone = false;
  ZoneOffset offset = ZoneOffset.UTC;

  public SnowflakeTimeWithTimezone(long time, int nanos, boolean useSessionTimeZone) {
    super(time);
    this.nanos = nanos;
    this.useSessionTimeZone = useSessionTimeZone;
  }

  public SnowflakeTimeWithTimezone(
      Timestamp ts, TimeZone sessionTimeZone, boolean useSessionTimeZone) {
    super(ts.getTime());
    this.nanos = ts.getNanos();
    this.useSessionTimeZone = useSessionTimeZone;
    if (sessionTimeZone != null) {
      this.offset = ZoneId.of(sessionTimeZone.getID()).getRules().getOffset(ts.toInstant());
    }
  }

  public int getNano() {
    return nanos;
  }

  public ZoneOffset getOffset() {
    return offset;
  }

  /**
   * Returns a string representation in session's timezone so as to display "wallclock time"
   *
   * @return a string representation of the object
   */
  public synchronized String toString() {
    if (!useSessionTimeZone) {
      return super.toString();
    }
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss");
    LocalDateTime ldt =
        LocalDateTime.ofEpochSecond(
            SnowflakeUtil.getSecondsFromMillis(this.getTime()), this.nanos, this.offset);
    return ldt.format(formatter);
  }
}
