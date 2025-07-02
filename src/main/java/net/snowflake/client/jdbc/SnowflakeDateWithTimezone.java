package net.snowflake.client.jdbc;

import java.sql.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

/**
 * Date with toString() overridden to display date values in session timezone. Only relevant for
 * timestamp objects fetched as dates. Normal date objects do not have a timezone associated with
 * them.
 */
public class SnowflakeDateWithTimezone extends Date {

  TimeZone timezone = TimeZone.getDefault();
  boolean useSessionTimezone = false;

  public SnowflakeDateWithTimezone(long date, TimeZone timezone, boolean useSessionTimezone) {
    super(date);
    this.timezone = timezone;
    this.useSessionTimezone = useSessionTimezone;
  }

  /**
   * Returns a string representation in UTC so as to display "wallclock time"
   *
   * @return a string representation of the object
   */
  public synchronized String toString() {
    if (!useSessionTimezone) {
      return super.toString();
    }
    String baseFormat = "yyyy-MM-dd";
    DateFormat formatter = new SimpleDateFormat(baseFormat);
    formatter.setTimeZone(this.timezone);
    return formatter.format(this);
  }
}
