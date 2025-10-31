package net.snowflake.client.common.core;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import net.snowflake.client.common.util.TimeUtil;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;

/**
 * Represents timestamps, including their originating time zone.
 *
 * <p>Stores data in UTC, with nanosecond precision.
 *
 * <p>Instances of this class are immutable.
 *
 * @author mzukowski
 */
@SnowflakeJdbcInternalApi
public class SFTimestamp extends SFInstant implements Comparable<SFTimestamp> {
  private static final BigInteger LONG_MIN_VALUE_BIGINT = BigInteger.valueOf(Long.MIN_VALUE);
  private static final BigInteger LONG_MAX_VALUE_BIGINT = BigInteger.valueOf(Long.MAX_VALUE);

  private static final BigDecimal NANOS_IN_SECOND = BigDecimal.valueOf(1).scaleByPowerOfTen(9);

  public static final SFTimestamp MIN_VALID_VALUE, MAX_VALID_VALUE;

  public static final long EPOCH_MS_2100 = 4102444800L * 1000;
  public static final long EPOCH_MS_2500 = 16725225600L * 1000;

  public static final long MS_Y400 = EPOCH_MS_2500 - EPOCH_MS_2100;

  static {
    final GregorianCalendar cal = new GregorianCalendar(SFInstant.GMT);
    cal.setGregorianChange(new Date(Long.MIN_VALUE));

    // clears HOUR, MINUTE, SECOND, etc.
    cal.setTimeInMillis(0);

    // min supported timestamp is -1000000-12-31 00:00:00 UTC
    cal.set(Calendar.ERA, GregorianCalendar.BC);
    cal.set(1_000_001, Calendar.DECEMBER, 31);
    MIN_VALID_VALUE = SFTimestamp.fromMilliseconds(cal.getTimeInMillis(), SFInstant.GMT);

    // max supported timestamp is 1000000-01-02 00:00:00 UTC
    cal.set(Calendar.ERA, GregorianCalendar.AD);
    cal.set(1_000_000, Calendar.JANUARY, 2);
    MAX_VALID_VALUE = SFTimestamp.fromMilliseconds(cal.getTimeInMillis(), SFInstant.GMT);
  }

  /**
   * Thrown when a Snowflake timestamp cannot be manipulated in Java due to size limitations.
   * Snowflake can use up to a full SB16 to represent a timestamp. Java, on the other hand, requires
   * that the number of millis since epoch fit into a long. For timestamps whose millis since epoch
   * don't fit into a long, certain operations, such as conversion to java .sql.Timestamp, are not
   * available.
   */
  public static class TimestampOperationNotAvailableException extends RuntimeException {
    TimestampOperationNotAvailableException(SFTimestamp timestamp) {
      // Don't call SFTimestamp.toString() here, because SFTimestamp.toString
      // () uses getTimestamp(), which may also throw this exception and thus
      // create an infinite loop.
      super("nanos=" + timestamp.nanosSinceEpoch);
    }
  }

  // this could be a BigInteger but it's more convenient to represent it as a
  // BigDecimal because we need to scale it by powers of 10 a lot.
  private final BigDecimal nanosSinceEpoch;

  private final TimeZone timeZone;

  /****** WARNING: need to match values in Timestamp.hpp in XP *****/
  // See https://snowflakecomputing.atlassian.net/wiki/display/EN/Timestamp+Data+Type
  private static final int BITS_FOR_TIMEZONE = 14;

  private static final int MASK_OF_TIMEZONE = (1 << BITS_FOR_TIMEZONE) - 1;

  /**
   * Constructs an SF timestamp from a given number of UTC milliseconds and an originating timezone.
   *
   * @param ms milliseconds
   * @param tz timezone
   * @return SFTimestamp instance
   */
  public static SFTimestamp fromMilliseconds(long ms, TimeZone tz) {
    return fromNanoseconds(new BigDecimal(ms).scaleByPowerOfTen(6), tz);
  }

  /**
   * Constructs an SF timestamp from a given number of UTC nanoseconds and an originating timezone.
   *
   * @param ns nanoseconds
   * @param tz timezone
   * @return SFTimestamp instance
   */
  public static SFTimestamp fromNanoseconds(long ns, TimeZone tz) {
    return fromNanoseconds(new BigDecimal(ns), tz);
  }

  /**
   * Constructs an SF timestamp from a given number of UTC nanoseconds, using the specified timezone
   *
   * @param ns nanoseconds in BigDecimal
   * @param tz timezone
   * @return SFTimestamp instance
   */
  public static SFTimestamp fromNanoseconds(BigDecimal ns, TimeZone tz) {
    return new SFTimestamp(ns, tz);
  }

  /**
   * Constructs an SF timestamp from a given number of UTC nanoseconds, using the GMT timezone.
   *
   * @param ns nanoseconds in BigDecimal
   * @return SFTimestamp instance
   */
  public static SFTimestamp fromNanoseconds(BigDecimal ns) {
    return new SFTimestamp(ns, SFInstant.GMT);
  }

  /**
   * Convert a timezone index to timezone.
   *
   * @param timezoneIndex timezone index where 1440 is UTC
   * @return Timezone instance
   */
  public static TimeZone convertTimezoneIndexToTimeZone(int timezoneIndex) {
    assert timezoneIndex >= 0 && timezoneIndex <= 2880; // API
    timezoneIndex -= 1440;
    boolean negate = (timezoneIndex < 0);
    timezoneIndex = Math.abs(timezoneIndex);
    int hour = timezoneIndex / 60;
    assert hour >= 0 && hour <= 24;
    int min = timezoneIndex % 60;
    assert min >= 0 && min <= 59;
    String tzName = String.format("GMT%s%02d:%02d", negate ? "-" : "+", hour, min);
    return TimeZone.getTimeZone(tzName);
  }

  /**
   * Constructs an SFTimestamp from a binary representation
   *
   * @param binary Physical representation, consists of UTC fractions (scaled decimals) and optional
   *     timezone index
   * @param scale Scale of fractional seconds
   * @param tz If specified, TimeZone to use. If null, we'll assume the timezone is embedded in the
   *     number
   * @return SFTimestamp instance
   */
  public static SFTimestamp fromBinary(BigDecimal binary, int scale, TimeZone tz) {
    BigDecimal nanoseconds;
    if (tz != null) {
      // Just scale the decimal number, e.g. for actual value "123.456" with
      // scale=3, which is represented as 123456 (fractions),
      // we'll get 123456000000 nanoseconds
      nanoseconds = binary.scaleByPowerOfTen(9 - scale);
    } else {
      // We will extract the time zone, and adapt the input on the way.
      // The input here is the integer respresentation in XP, formatted
      // as a decimal using the scale.
      // For example, (assuming everything is decimal) a time moment
      // 123.456 (scale = 3) in timezone 78 is represented in XP as 12345678.
      // (On binary level: 123456 << 14 + 78)
      // We want to convert it to 123456, and extract 78 on the way.

      // Extract timezone offset
      // Grab integer, e.g. 12345678
      BigInteger secsWithTzOff = binary.toBigIntegerExact();
      // Extract 78
      BigInteger tzOff = secsWithTzOff.and(BigInteger.valueOf(MASK_OF_TIMEZONE));
      // Get rid of the timezone, e.g. 123456
      BigInteger secsWoTzOff = secsWithTzOff.shiftRight(BITS_FOR_TIMEZONE);

      // Scale to nanoseconds, e.g. 123456000000
      nanoseconds = new BigDecimal(secsWoTzOff).scaleByPowerOfTen(9 - scale);

      // Finally, construct a timezone from timezone offset
      // @todo This is very ugly - maybe we should consider not using TimeZone
      // to represent this.
      tz = convertTimezoneIndexToTimeZone(tzOff.intValue());
    }

    return fromNanoseconds(nanoseconds, tz);
  }

  /**
   * Constructs an SF timestamp from a Date and a number of nanoseconds.
   *
   * @param date date instance
   * @param nanos nanoseconds
   * @param tz timezone
   * @return SFTImestamp instance
   */
  public static SFTimestamp fromDate(Date date, int nanos, TimeZone tz) {
    Timestamp t = new Timestamp(date.getTime());
    t.setNanos(nanos);
    SFTimestamp res = new SFTimestamp(t, tz);
    return res;
  }

  /**
   * Constructs an SF timestamp from an SF date,
   *
   * @param sfd SFDate representing a date. Note - it's always UTC milliseconds.
   * @param tz timezone
   * @return SFTimestamp representing a midnight in a specified timezone.
   */
  public static SFTimestamp fromSFDate(SFDate sfd, TimeZone tz) {
    // Create a Calendar representing midnight in UTC
    Calendar calUTC = CalendarCache.get(SFInstant.GMT, "GMT-source");
    calUTC.setTime(sfd.getDate());
    // Create a Calendar with midnight in the defined timezone
    Calendar calLocal = CalendarCache.get(tz);
    calLocal.set(
        calUTC.get(Calendar.YEAR), calUTC.get(Calendar.MONTH), calUTC.get(Calendar.DAY_OF_MONTH));
    calLocal.set(Calendar.ERA, calUTC.get(Calendar.ERA));

    // Build the result timestamp
    return fromMilliseconds(calLocal.getTimeInMillis(), tz);
  }

  private SFTimestamp(BigDecimal nanosSinceEpoch, TimeZone tz) {
    this.nanosSinceEpoch = nanosSinceEpoch;

    if (tz != null) {
      this.timeZone = tz;
    } else {
      this.timeZone = SFInstant.GMT;
    }
  }

  /**
   * Constructs an SFTimestamp from a java Timestamp and java TimeZone.
   *
   * @param ts timestamp
   * @param tz timezone
   */
  public SFTimestamp(Timestamp ts, TimeZone tz) {
    if (ts == null) {
      ts = new Timestamp(new Date().getTime());
    }

    this.nanosSinceEpoch =
        new BigDecimal(ts.getTime())
            // Zero out the milliseconds part.
            // java.sql.Timestamp is supposed to contain only integral
            // seconds in its millis timestamp, and then the fractional
            // seconds in the nanos field.
            // But, depending on how it is constructed, the millis after
            // second part could be present in both the millis and the nanos
            // variables... go figure.
            .scaleByPowerOfTen(-3)
            .setScale(0, RoundingMode.FLOOR)
            // seconds to nanos
            .scaleByPowerOfTen(9)
            // add the fractional seconds from the nanos part
            .add(BigDecimal.valueOf(ts.getNanos()));

    if (tz != null) {
      timeZone = tz;
    } else {
      timeZone = SFInstant.GMT;
    }
  }

  /**
   * Constructs an SFTimestamp from a java Timestamp. The result timestamp has no timezone.
   *
   * @param ts Java timestamp
   */
  public SFTimestamp(Timestamp ts) {
    this(ts, SFInstant.GMT);
  }

  /**
   * Constructs an SFTimestamp from SFDate. The result timestamp has no timezone
   *
   * @param date SFDate instance
   */
  public SFTimestamp(SFDate date) {
    this(new Timestamp(date.getTime()), SFInstant.GMT);
  }

  /** Empty constructor, will create a timestamp at this instant */
  public SFTimestamp() {
    this((Timestamp) null, null);
  }

  /**
   * Copy constructor. Unless an explicit copy of {@code original} is needed, use of this
   * constructor is unnecessary since SFTimestamps are immutable.
   *
   * @param sft source SFTimestamp instance
   */
  public SFTimestamp(SFTimestamp sft) {
    // It's safe to copy BigInteger by reference; it's immutable.
    this.nanosSinceEpoch = sft.nanosSinceEpoch;

    this.timeZone = (TimeZone) sft.timeZone.clone();
  }

  public BigDecimal getNanosSinceEpoch() {
    return nanosSinceEpoch;
  }

  /**
   * Convert this to a Java timestamp.
   *
   * @return Java timestamp or null
   * @throws SFTimestamp.TimestampOperationNotAvailableException if this timestamp doesn't fit in a
   *     Java timestamp
   */
  public Timestamp getTimestamp() throws SFTimestamp.TimestampOperationNotAvailableException {
    Timestamp ts = TimeUtil.timestampFromNs(nanosSinceEpoch);

    if (ts == null) {
      throw new SFTimestamp.TimestampOperationNotAvailableException(this);
    }

    return ts;
  }

  /**
   * Returns Timezone
   *
   * @return Timezone
   */
  public TimeZone getTimeZone() {
    return timeZone;
  }

  /**
   * Returns a new SFTimestamp object, with specified timeZone set in it.
   *
   * @param timeZone the timezone for this timestamp
   * @return the new SFTimestamp in the new timezone
   */
  public SFTimestamp changeTimeZone(TimeZone timeZone) {
    return new SFTimestamp(this.nanosSinceEpoch, timeZone);
  }

  /**
   * Moves the timestamp to another time zone without changing its displayed value. For example,
   * 18:53:21 PDT will become 18:53:21 EDT. This is NOT the same as changeTimestamp(); that will
   * change 18:53:21 PDT into 21:53:21 EDT.
   *
   * @param newTimeZone a target Timezone
   * @return SFTimestamp instance
   * @throws SFTimestamp.TimestampOperationNotAvailableException if this timestamp doesn't fit into
   *     a Java timestamp
   */
  public SFTimestamp moveToTimeZone(TimeZone newTimeZone)
      throws SFTimestamp.TimestampOperationNotAvailableException {
    return moveToTimeZone(newTimeZone, false /*cCompatibility*/);
  }

  /**
   * Moves the timestamp to another time zone without changing its displayed value. For example,
   * 18:53:21 PDT will become 18:53:21 EDT. This is NOT the same as changeTimestamp(); that will
   * change 18:53:21 PDT into 21:53:21 EDT.
   *
   * @param newTimeZone a target Timezone
   * @param cCompatibility corrects incompatibilities between Java and C libraries for timestamps
   *     that are ambiguous (e.g. '2016-11-06 01:30') or illegal (e.g. '2016-03-13 02:30') due to
   *     DST rules.
   * @return SFTimestamp instance
   * @throws SFTimestamp.TimestampOperationNotAvailableException if this timestamp doesn't fit into
   *     a Java timestamp
   */
  public SFTimestamp moveToTimeZone(TimeZone newTimeZone, boolean cCompatibility)
      throws SFTimestamp.TimestampOperationNotAvailableException {
    int offsetMillisInOldTZ = getTimeZone().getOffset(getTime());

    Calendar calendar = CalendarCache.get(getTimeZone());
    calendar.setTimeInMillis(getTime());

    int millisecondWithinDay =
        ((calendar.get(Calendar.HOUR_OF_DAY) * 60 + calendar.get(Calendar.MINUTE)) * 60
                    + calendar.get(Calendar.SECOND))
                * 1000
            + calendar.get(Calendar.MILLISECOND);

    int era = calendar.get(Calendar.ERA);
    int year = calendar.get(Calendar.YEAR);
    int month = calendar.get(Calendar.MONTH);
    int dayOfMonth = calendar.get(Calendar.DAY_OF_MONTH);
    int dayOfWeek = calendar.get(Calendar.DAY_OF_WEEK);

    int offsetMillisInNewTZ =
        newTimeZone.getOffset(era, year, month, dayOfMonth, dayOfWeek, millisecondWithinDay);

    if (cCompatibility && !(newTimeZone instanceof TimeUtil.CCompatibleTimeZone)) {
      if (TimeUtil.isDSTAmbiguous(
          getTime() + offsetMillisInOldTZ - offsetMillisInNewTZ, newTimeZone)) {
        // For an ambiguous timestamp, such as '2016-11-06 01:30' in
        // America/Los_Angeles:
        // - The C library chooses the earlier instant, i.e.
        //   2016-11-06 01:30 -0700 or 2016-11-06 08:30 Z
        // - Java chooses the later instant, i.e.
        //   2016-11-06 01:30 -0800 or 2016-11-06 09:30 Z
        // For compatibility, we need to bring back the Java timestamp by an
        // hour.

        offsetMillisInNewTZ += newTimeZone.getDSTSavings();
      } else if (TimeUtil.isDSTIllegal(
          era, year, month, dayOfMonth, dayOfWeek, millisecondWithinDay, newTimeZone)) {
        // For an illegal timestamp, such as '2016-03-13 02:30' in
        // America/Los_Angeles:
        // - The C library corrects this to
        //   2016-03-13 03:30 -0700  or 2016-03-13 10:30 Z
        // - Java corrects this to
        //   2016-03-13 01:30 -0800 or 2016-03-13 09:30 Z
        //   (which doesn't really make sense).
        // For compatibility, we need to spring forward the Java timestamp by
        // an hour.

        offsetMillisInNewTZ -= newTimeZone.getDSTSavings();
      }
    }

    int offsetMillis = offsetMillisInOldTZ - offsetMillisInNewTZ;

    long newMillis = getTime() + offsetMillis;

    Timestamp newSqlTs = new Timestamp(newMillis);
    newSqlTs.setNanos(getNanos());

    return new SFTimestamp(newSqlTs, newTimeZone);
  }

  /**
   * Adjusts fractional second accuracy if necessary.
   *
   * @param scale scale
   * @return SFTimestamp instance
   */
  public SFTimestamp adjustScale(int scale) {
    if (scale < 0 || scale > 9) {
      throw new IllegalArgumentException("Invalid timestamp scale " + scale);
    }

    int zeroesAtEnd = 9 - scale;
    BigDecimal powerOfTen = BigDecimal.valueOf(SFInstant.POWERS_OF_TEN[zeroesAtEnd]);

    BigDecimal extraDigits = nanosSinceEpoch.remainder(powerOfTen);

    if (extraDigits.equals(BigDecimal.ZERO)) {
      return this;
    }

    // Wrap negative remainders around, otherwise timestamps before epoch
    // will round up instead of down.
    if (extraDigits.compareTo(BigDecimal.ZERO) < 0) {
      extraDigits = powerOfTen.add(extraDigits);
    }

    BigDecimal newNanosSinceEpoch = nanosSinceEpoch.subtract(extraDigits);

    return new SFTimestamp(newNanosSinceEpoch, getTimeZone());
  }

  /**
   * Returns UTC string
   *
   * @return UTC string
   */
  public String toUTCString() {
    Timestamp timestamp = TimeUtil.timestampFromNs(nanosSinceEpoch);
    if (timestamp == null) {
      // This can't be represented as a Java timestamp.
      // Just print the seconds since epoch.
      return "(seconds_since_epoch=" + nanosSinceEpoch.scaleByPowerOfTen(-9).toPlainString() + ")";
    }

    String nanoStr = String.format(".%1$09d", timestamp.getNanos());
    Calendar calendar = CalendarCache.get("UTC");
    DateFormat tsf = new SimpleDateFormat("yyyy-MM-dd' 'HH:mm:ss" + nanoStr + "XXX");
    tsf.setCalendar(calendar);
    String res = tsf.format(timestamp.getTime());
    return res;
  }

  /**
   * Compares with other SFTimestamp
   *
   * @param other target SFTimestamp
   * @return 1 if larger, 0 if equals otherwise -1
   */
  public int compareTo(SFTimestamp other) {
    return nanosSinceEpoch.compareTo(other.nanosSinceEpoch);
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object o) {
    if (o == null || !(o instanceof SFTimestamp)) {
      return false;
    }
    return equals((SFTimestamp) o);
  }

  /**
   * Checks if equals with other SFTimestamp
   *
   * @param other target SFTimestamp
   * @return true if equal otherwise false
   */
  public boolean equals(SFTimestamp other) {
    return nanosSinceEpoch.equals(other.nanosSinceEpoch);
  }

  /**
   * Returns epoch time in milliseconds as a long. CAUTION: If this timestamp's millis since epoch
   * can't fit into a long, an exception will be thrown!
   *
   * @return milliseconds since epoch as a long
   * @throws SFTimestamp.TimestampOperationNotAvailableException if this timestamp doesn't fit into
   *     a Java timestamp
   */
  public long getTime() throws SFTimestamp.TimestampOperationNotAvailableException {
    // IntelliJ doesn't like BigInteger.longValueExact() for some reason
    BigInteger timeBigInt = getTimeInMsBigInt();

    if (timeBigInt.compareTo(LONG_MIN_VALUE_BIGINT) < 0
        || timeBigInt.compareTo(LONG_MAX_VALUE_BIGINT) > 0) {
      throw new SFTimestamp.TimestampOperationNotAvailableException(this);
    }

    return timeBigInt.longValue();
  }

  /**
   * Returns the epoch time in milliseconds.
   *
   * @return milliseconds since epoch
   */
  public BigInteger getTimeInMsBigInt() {
    return nanosSinceEpoch
        .scaleByPowerOfTen(-6)
        // always round down; this is important for timestamps before the epoch.
        // by default, Java will round towards 0.
        .setScale(0, RoundingMode.FLOOR)
        .toBigInteger();
  }

  /**
   * Returns the integral seconds component of this timestamp.
   *
   * @return
   */
  public BigInteger getSeconds() {
    return nanosSinceEpoch
        .scaleByPowerOfTen(-9)
        // always round down; this is important for timestamps before the epoch.
        // by default, Java will round towards 0.
        .setScale(0, RoundingMode.FLOOR)
        .toBigInteger();
  }

  /**
   * Returns the fractional seconds component of this timestamp, i.e. the number of nanos from the
   * nearest integral second (rounded down).
   *
   * @return nanoseconds
   */
  public int getNanos() {
    BigDecimal nsFractional = nanosSinceEpoch.remainder(NANOS_IN_SECOND);

    // Remainder can return negative numbers; wrap it around.
    if (nsFractional.compareTo(BigDecimal.ZERO) < 0) {
      nsFractional = nsFractional.add(NANOS_IN_SECOND);
    }

    return nsFractional.intValue();
  }

  /** {@inheritDoc} */
  public int hashCode() {
    return this.toBinary(9, false).hashCode();
  }

  /**
   * Calculates the offset of this timestamp's time zone at this timestamp's time instant. This is
   * equivalent to getTimeZone().getOffset(getTime()) except it won't throw
   * TimestampOperationNotAvailableException for timestamps that don't fit in Java timestamp.
   *
   * @return the time zone offset in millis
   */
  public int getTimeZoneOffsetMillis() {
    BigInteger timeMs = getTimeInMsBigInt();

    // We only support timestamps within a two-million-years range. For timestamps
    // that are smaller than the range, we assume there is no timezone offset change.
    timeMs = timeMs.max(MIN_VALID_VALUE.getTimeInMsBigInt());

    // For future timestamps after year 2100, we assumed the current timezone offset rule applies.
    return normalizeTimeZoneOffset(timeMs);
  }

  /** Compute future TimeZone offset based on current dst rules. */
  private int normalizeTimeZoneOffset(BigInteger timeMs) {
    // 400 years is a magic cycle. It's the cycle for Gregorian Calendar and it contains a number of
    // days that is a multiple of 7. Assuming no time zone rule changes after 2100, we can get the
    // timezone offset for any timestamp after 2500 by mapping it to somewhere between [2100, 2500).
    if (timeMs.compareTo(BigInteger.valueOf(EPOCH_MS_2500)) < 0) {
      return timeZone.getOffset(timeMs.longValue());
    }

    final BigInteger year2100 = BigInteger.valueOf(EPOCH_MS_2100);
    final BigInteger msInY400 = BigInteger.valueOf(MS_Y400);
    final BigInteger remainderMs = timeMs.subtract(year2100).mod(msInY400);
    BigInteger adjustedTimeMs = year2100.add(remainderMs);
    return timeZone.getOffset(adjustedTimeMs.longValue());
  }

  /**
   * Constructs a binary representation of this timestamp. This is the opposite of fromBinary().
   *
   * @param scale scale of fractional seconds
   * @param includeTimeZone encode the time zone in the lower-order bits
   * @return the binary representation of this timestamp
   */
  public BigInteger toBinary(int scale, boolean includeTimeZone) {
    if (scale < 0 || scale > 9) {
      throw new IllegalArgumentException("Scale must be between 0 and 9");
    }

    BigDecimal timeInNs = this.nanosSinceEpoch;

    // If the scale is 9 (maximum), then we return the number of nanos.
    // If it is 8, we return the number of 10's of nanos. etc.

    // slide the decimal point
    BigDecimal scaledTime = timeInNs.scaleByPowerOfTen(scale - 9);
    // round to an integer
    // We have to use RoundingMode.DOWN instead of RoundingMode.FLOOR because we
    // want to truncate the extra digits; for negative timestamps, FLOOR will do
    // the wrong thing.
    scaledTime = scaledTime.setScale(0, RoundingMode.DOWN);

    BigInteger fcpInt = scaledTime.unscaledValue();

    if (includeTimeZone) {
      // now add the time zone
      int offsetMillis = getTimeZoneOffsetMillis();

      // our offset is in minutes
      int offsetMin = offsetMillis / 60000;

      // this code mimics fromBinary; see above
      assert offsetMin >= -1440 && offsetMin <= 1440;
      offsetMin += 1440;

      fcpInt = fcpInt.shiftLeft(14);
      fcpInt = fcpInt.add(BigInteger.valueOf(offsetMin & MASK_OF_TIMEZONE));
    }

    return fcpInt;
  }

  /**
   * Extract a particular component of a date.
   *
   * @param field field id as specified in the Calendar class. TODO: we don't support e.g.
   *     nanosecond nor timezones for now
   * @return The value of the extracted field.
   * @throws SFTimestamp.TimestampOperationNotAvailableException if this timestamp doesn't fit into
   *     a Java timestamp
   */
  @Override
  public int extract(int field, Integer optWeekStart, Integer optWoyPolicy)
      throws SFTimestamp.TimestampOperationNotAvailableException {
    TimeZone tz = timeZone == null ? SFInstant.GMT : timeZone;
    return extract(field, tz, getTime(), optWeekStart, optWoyPolicy);
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    String tsStr;

    try {
      tsStr = "timestamp='" + getTimestamp().toString() + "'";
    } catch (SFTimestamp.TimestampOperationNotAvailableException e) {
      tsStr = "--nanos=" + nanosSinceEpoch;
    }

    return "SFTimestamp(" + tsStr + " timeZone='" + timeZone + "')";
  }
}
