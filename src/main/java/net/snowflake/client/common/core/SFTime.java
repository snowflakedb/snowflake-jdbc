package net.snowflake.client.common.core;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;

/**
 * Represents times without date.
 *
 * <p>Stores fractional seconds since midnight, with nanosecond precision.
 *
 * <p>Instances of this class are immutable.
 *
 * @author mkember
 */
@SnowflakeJdbcInternalApi
public class SFTime extends SFInstant implements Comparable<SFTime> {
  // The java.sql.Time class is too cumbersome to use (it only supports
  // millisecond precision), so we store nanoseconds since midnight directly.
  private final long nanos;

  // Used for unit conversions.
  private static final long NS_IN_SECOND = 1000000000;
  private static final long NS_IN_MILLIS = 1000000;
  private static final long NS_IN_DAY = 86400 * NS_IN_SECOND;

  // min/max values
  public static final SFTime MIN_VALID_VALUE = SFTime.fromNanoseconds(0);
  public static final SFTime MAX_VALID_VALUE = SFTime.fromNanoseconds(NS_IN_DAY - 1);

  /**
   * Constructs an SFTime given nanoseconds since midnight.
   *
   * @param nanos nanoseconds
   * @return SFTime instance
   */
  public static SFTime fromNanoseconds(long nanos) {
    return new SFTime(nanos);
  }

  /**
   * Constructs an SFTime from fractional seconds at a particular scale.
   *
   * @param value Fractional seconds since midnight.
   * @param scale The scale for interpreting {@code value}. A scale of 0 means seconds, and a scale
   *     of 9 means nanoseconds.
   * @return A new SFTime object.
   */
  public static SFTime fromFractionalSeconds(long value, int scale) {
    assert scale >= 0 && scale <= 9;
    int powerOfTen = SFInstant.POWERS_OF_TEN[9 - scale];
    return new SFTime(value * powerOfTen);
  }

  /**
   * Constructs an SFTime from an SFTimestamp.
   *
   * <p>The resulting SFTime contains the time of day
   *
   * @param ts SFTimestamp
   * @return SFTime
   */
  public static SFTime fromTimestamp(SFTimestamp ts) {
    long secondsSinceMidnight =
        ts.extract(Calendar.HOUR_OF_DAY) * 3600L
            + ts.extract(Calendar.MINUTE) * 60
            + ts.extract(Calendar.SECOND);
    long nsSinceMidnight = secondsSinceMidnight * NS_IN_SECOND;
    long additionalNs = ts.getNanos();
    return new SFTime(nsSinceMidnight + additionalNs);
  }

  private SFTime(long nanos) {
    assert nanos >= 0 && nanos < 86400 * NS_IN_SECOND;
    this.nanos = nanos;
  }

  /**
   * Returns nanoseconds since midnight.
   *
   * @return nanoseconds
   */
  public long getNanoseconds() {
    return nanos;
  }

  /**
   * Returns nanoseconds since the last whole-number second.
   *
   * @return nanoseconds
   */
  public int getNanosecondsWithinSecond() {
    return (int) (nanos % SFInstant.POWERS_OF_TEN[9]);
  }

  /**
   * Returns fractional seconds since midnight at the given scale, truncated from the internal
   * nanosecond representation.
   *
   * @param scale scale
   * @return fractional seconds
   * @see #fromFractionalSeconds(long, int)
   */
  public long getFractionalSeconds(int scale) {
    assert scale >= 0 && scale <= 9;
    return nanos / SFInstant.POWERS_OF_TEN[9 - scale];
  }

  /**
   * Create an SFTime by adding hours, minutes, seconds, or milliseconds.
   *
   * <p>Performs the addition modulo 24 hours so that the result stays in range.
   *
   * @param component The time component from the Calendar class.
   * @param increment Number of hours/minutes/etc. to add.
   * @return A new SFTime with the result of the addition.
   */
  public SFTime addComponent(int component, long increment) {
    // Reduce increment mod 24h and convert to nanoseconds.
    switch (component) {
      case Calendar.HOUR_OF_DAY:
        increment = 3600 * NS_IN_SECOND * (increment % 24);
        break;
      case Calendar.MINUTE:
        increment = 60 * NS_IN_SECOND * (increment % (24 * 60));
        break;
      case Calendar.SECOND:
        increment = NS_IN_SECOND * (increment % (24 * 60 * 60));
        break;
      case Calendar.MILLISECOND:
        increment = NS_IN_MILLIS * (increment % (24 * 60 * 60 * 1000));
        break;
      default:
        throw new IllegalArgumentException("invalid component " + component);
    }

    // Get the positive remainder.
    // TODO(mkember): Use Math.floorMod when GSCommon is on Java 1.8.
    long newNanos = ((nanos + increment) % NS_IN_DAY + NS_IN_DAY) % NS_IN_DAY;
    return new SFTime(newNanos);
  }

  /**
   * Create an SFTime with an adjusted scale.
   *
   * @param scale The desired scale.
   * @return Returns a new SFTime if changing the scale would result in a different (less precise)
   *     time. Returns this object otherwise.
   */
  public SFTime adjustScale(int scale) {
    long powerOfTen = SFInstant.POWERS_OF_TEN[9 - scale];
    long extraDigits = nanos % powerOfTen;
    return extraDigits == 0 ? this : new SFTime(nanos - extraDigits);
  }

  /**
   * Constructs a string that can be safely passed to XP.
   *
   * @return a UTC string
   */
  public String toUTCString() {
    long nanoPart = nanos % NS_IN_SECOND;
    String nanoStr = String.format(".%1$09d", nanoPart);
    DateFormat df = new SimpleDateFormat("HH:mm:ss" + nanoStr);
    df.setCalendar(CalendarCache.get(SFInstant.GMT));
    return df.format(nanos / NS_IN_MILLIS);
  }

  /**
   * Constructs a string containing the number of seconds since midnight with the given number of
   * decimal places.
   *
   * @param decimalPlaces decimal places
   * @return seconds in a string
   */
  public String toSecondsString(int decimalPlaces) {
    StringBuilder result = new StringBuilder();
    result.append(nanos / NS_IN_SECOND);
    if (decimalPlaces > 0) {
      long nanoPart = nanos % NS_IN_SECOND;
      long powerOfTen = SFInstant.POWERS_OF_TEN[9 - decimalPlaces];
      long afterDecimal = nanoPart / powerOfTen;
      result.append('.');
      String fmt = "%0" + decimalPlaces + "d";
      result.append(String.format(fmt, afterDecimal));
    }
    return result.toString();
  }

  /**
   * Compares with other SFTime
   *
   * @param other target SFTime
   * @return 1 if larger, 0 if equal otherwise -1
   */
  public int compareTo(SFTime other) {
    return Long.compare(nanos, other.nanos);
  }

  /** {@inheritDoc} */
  @Override
  public int extract(int field, Integer optWeekStart, Integer optWoyPolicy) {
    return extract(field, SFInstant.GMT, nanos / NS_IN_MILLIS, optWeekStart, optWoyPolicy);
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return (int) (nanos % Integer.MAX_VALUE);
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object other) {
    return other instanceof SFTime && nanos == ((SFTime) other).nanos;
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return "SFTime(nanos=" + nanos + ")";
  }
}
