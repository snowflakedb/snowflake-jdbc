package net.snowflake.client.jdbc;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.TimeZone;

/**
 * Timestamp with toString() overridden to display timestamp in session timezone. The default
 * timezone is UTC if no timezone is specified.
 */
public class SnowflakeTimestampWithTimezone extends Timestamp {
  private static final long serialVersionUID = 1L;

  private TimeZone timezone = TimeZone.getTimeZone("UTC");

  public SnowflakeTimestampWithTimezone(long seconds, int nanoseconds, TimeZone timezone) {
    super(seconds);
    this.setNanos(nanoseconds);
    this.timezone = timezone;
  }

  public SnowflakeTimestampWithTimezone(Timestamp ts, TimeZone timezone) {
    this(ts.getTime(), ts.getNanos(), timezone);
  }

  public SnowflakeTimestampWithTimezone(Timestamp ts) {
    this(ts.getTime(), ts.getNanos(), TimeZone.getTimeZone("UTC"));
  }

  /**
   * Gets the timezone.
   *
   * @return the timezone.
   */
  public TimeZone getTimezone() {
    return this.timezone;
  }

  /**
   * Converts this timestamp to a zoned date time.
   *
   * @return the zoned date time corresponding to this timestamp.
   */
  public ZonedDateTime toZonedDateTime() {
    return ZonedDateTime.ofInstant(toInstant(), this.timezone.toZoneId());
  }

  /**
   * Returns a string representation in UTC
   *
   * @return a string representation of the object
   */
  public synchronized String toString() {
    int trailingZeros = 0;
    int tmpNanos = this.getNanos();
    if (tmpNanos == 0) {
      trailingZeros = 8;
    } else {
      while (tmpNanos % 10 == 0) {
        tmpNanos /= 10;
        trailingZeros++;
      }
    }
    final String baseFormat = "uuuu-MM-dd HH:mm:ss.";
    StringBuilder buf = new StringBuilder(baseFormat.length() + 9 - trailingZeros);
    buf.append(baseFormat);
    for (int i = 0; i < 9 - trailingZeros; ++i) {
      buf.append("S");
    }
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(buf.toString());

    ZoneOffset offset = ZoneId.of(timezone.getID()).getRules().getOffset(this.toInstant());
    LocalDateTime ldt =
        LocalDateTime.ofEpochSecond(
            SnowflakeUtil.getSecondsFromMillis(this.getTime()), this.getNanos(), offset);
    return ldt.format(formatter);
  }
}
