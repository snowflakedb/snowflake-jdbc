package net.snowflake.client.common.core;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;

/**
 * Represents dates without time.
 *
 * <p>Used to store dates, as in 'midnights'.
 *
 * <p>Stores UTC midnights always for a given date.
 *
 * <p>Instances of this class are immutable.
 *
 * @author mzukowski
 */
@SnowflakeJdbcInternalApi
public class SFDate extends SFInstant implements Comparable<SFDate> {
  private static final long MILLIS_IN_DAY = 24 * 3600 * 1000;
  private final Date date;

  // min/max values
  public static final SFDate MIN_VALID_VALUE, MAX_VALID_VALUE;

  static {
    final GregorianCalendar cal = new GregorianCalendar(SFInstant.GMT);
    cal.setGregorianChange(new Date(Long.MIN_VALUE));

    // clears HOUR, MINUTE, SECOND, etc.
    cal.setTimeInMillis(0);

    // min supported date is -999999-01-01
    cal.set(Calendar.ERA, GregorianCalendar.BC);
    cal.set(1_000_000, Calendar.JANUARY, 1);
    MIN_VALID_VALUE = new SFDate(cal.getTimeInMillis());

    // max supported date is 999999-12-31
    cal.set(Calendar.ERA, GregorianCalendar.AD);
    cal.set(999_999, Calendar.DECEMBER, 31);
    MAX_VALID_VALUE = new SFDate(cal.getTimeInMillis());
  }

  /**
   * Return the embedded Java date object
   *
   * @return Date instance
   */
  public Date getDate() {
    return date;
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return date.hashCode();
  }

  /**
   * Return the number of milliseconds since UTC epoch
   *
   * @return milliseconds
   */
  public long getTime() {
    return date.getTime();
  }

  /**
   * Construct a string that can be safely passed to XP.
   *
   * @return UTC string
   */
  public String toUTCString() {
    Calendar calendar = CalendarCache.get("UTC");
    DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
    df.setCalendar(calendar);
    return df.format(date.getTime());
  }

  /**
   * Compare if we're smaller than, equal to, or larger than the other SFDate
   *
   * @param other target SFDate
   * @return value lower than 0 if we're smaller, 0 if we're equal, value larger than 0 if we're
   *     larger.
   */
  public int compareTo(SFDate other) {
    return date.compareTo(other.date);
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object o) {
    if (o == null || !(o instanceof SFDate)) {
      return false;
    }
    return equals((SFDate) o);
  }

  /**
   * Returns if we're equal to the other SFDate
   *
   * @param other target SFDate
   * @return true if identical otherwise false
   */
  public boolean equals(SFDate other) {
    return date.equals(other.date);
  }

  /**
   * Constructor using UTC milliseconds
   *
   * @param millis epoch time in UTC in milliseconds
   */
  public SFDate(long millis) {
    date = normalize(millis, null);
  }

  /**
   * Constructor using Java Date
   *
   * @param date date instance
   */
  public SFDate(Date date) {
    if (date == null) {
      throw new IllegalArgumentException("Illegal null date parameter.");
    }
    this.date = normalize(date.getTime(), null);
  }

  /**
   * Copy constructor. Unless an explicit copy of {@code original} is needed, use of this
   * constructor is unnecessary since SFDates are immutable.
   *
   * @param sfd SFDate instance
   */
  public SFDate(SFDate sfd) {
    // object is immutable, date is always set
    this.date = (Date) sfd.date.clone();
  }

  /**
   * Utility function generating a new SFDate from a given SFTimestamp.
   *
   * @param timestamp source timestamp
   * @return SFDate instance
   */
  public static SFDate fromTimestamp(SFTimestamp timestamp) {
    long millis = timestamp.getTime();
    TimeZone tz = timestamp.getTimeZone();
    return new SFDate(normalize(millis, tz));
  }

  /**
   * Normalize the timestamp to midnight on that date
   *
   * @param millis epoch time
   * @param tz timezone
   * @return java.util.Date
   */
  private static Date normalize(long millis, TimeZone tz) {
    if (tz != null) { // Get the same time as local time but in UTC
      millis += tz.getOffset(millis);
    }
    // It's critical we round down (negative infinity) to get midnight.
    millis = millis - Math.floorMod(millis, MILLIS_IN_DAY);
    return new Date(millis);
  }

  /**
   * Extract a particular component of a date.
   *
   * @param field field id as specified in the Calendar class
   * @param optWeekStart Optional WEEK_START value
   * @param optWoyPolicy Optional WEEK_OF_YEAR_POLICY value
   * @return value
   */
  @Override
  public int extract(int field, Integer optWeekStart, Integer optWoyPolicy) {
    return extract(field, SFInstant.GMT, date.getTime(), optWeekStart, optWoyPolicy);
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return "SFDate(date='" + date + "')";
  }

  /**
   * Increment current date by specified number of days and return a new SFDate object.
   *
   * @param ndays
   * @return
   */
  public SFDate addDays(int ndays) {
    return new SFDate(date.getTime() + (MILLIS_IN_DAY * ndays));
  }
}
