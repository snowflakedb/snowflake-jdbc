package net.snowflake.client.common.util;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import net.snowflake.client.common.core.SFTime;
import net.snowflake.client.common.core.SFTimestamp;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import sun.util.calendar.ZoneInfo;

/**
 * Time-related utilities
 *
 * @author mzukowski
 */
@SnowflakeJdbcInternalApi
public class TimeUtil {
  private static final BigDecimal LONG_MIN_VALUE_BIGD = BigDecimal.valueOf(Long.MIN_VALUE);
  private static final BigDecimal LONG_MAX_VALUE_BIGD = BigDecimal.valueOf(Long.MAX_VALUE);

  public enum TimestampType {
    TIMESTAMP_NTZ,
    TIMESTAMP_TZ,
    TIMESTAMP_LTZ
  }

  public enum TimestampUnit {
    IN_NANOSECONDS,
    IN_SECONDS
  }

  /**
   * Custom ZoneInfo implementations can be compatible with C libraries regarding ambiguous and
   * illegal timestamps. Such implementations should extend this class.
   */
  public abstract static class CCompatibleTimeZone extends ZoneInfo {
    /**
     * Whether the offset of this timezone ever changes.
     *
     * @return true if the offset never changes.
     */
    public abstract boolean hasSingleOffset();
  }

  private static TimeZone utcTZ = TimeZone.getTimeZone("UTC");

  /**
   * Converts the number of nanoseconds into a java.sql.Timestamp, if that is possible.
   * (java.sql.Timestamp can only accommodate timestamps where the number of millis since epoch can
   * be represented by a long).
   *
   * <p>Note: this method will adjust timestamp before 1582-10-05 to be Gregorian Calendar (by
   * default java use Julian Calendar)
   *
   * @param ns a BigDecimal value
   * @return timestamp, or null if ns is outside the range of Java timestamps
   */
  public static Timestamp timestampFromNs(BigDecimal ns) {
    // Get the integral-seconds part in nanoseconds
    // We round down, for negative times it's critical
    BigDecimal nsIntegral =
        ns.scaleByPowerOfTen(-9).setScale(0, RoundingMode.FLOOR).scaleByPowerOfTen(9);
    // Get the full-seconds part in ms
    BigDecimal msIntegral = nsIntegral.scaleByPowerOfTen(-6);
    // Get the fractional-seconds-part (in nanoseconds)
    // Note - if ns was negative, this will be positive
    BigDecimal nsFractional = ns.subtract(nsIntegral);

    // Create the timestamp
    if (msIntegral.compareTo(LONG_MIN_VALUE_BIGD) < 0
        || msIntegral.compareTo(LONG_MAX_VALUE_BIGD) > 0) {
      // not representable by java.sql.Timestamp
      return null;
    }

    Timestamp timestamp = new Timestamp(msIntegral.longValueExact());
    timestamp.setNanos(nsFractional.intValueExact());

    return timestamp;
  }

  /**
   * Convert a timestamp internal value (scaled number of seconds + fractional seconds) into a
   * SFTimestamp.
   *
   * @param timestampStr timestamp object
   * @param scale timestamp scale
   * @param internalColumnType snowflake timestamp type
   * @param resultVersion For new result version, timestamp with timezone is formatted as the
   *     seconds since epoch with fractional part in the decimal followed by time zone index. E.g.:
   *     "123.456 1440". Here 123.456 is the * number of seconds since epoch and 1440 is the
   *     timezone index.
   * @param sessionTZ session timezone
   * @return converted snowflake timestamp object
   * @throws IllegalArgumentException if timestampStr is an invalid timestamp
   */
  public static SFTimestamp getSFTimestamp(
      String timestampStr,
      int scale,
      TimeUtil.TimestampType internalColumnType,
      long resultVersion,
      TimeZone sessionTZ)
      throws IllegalArgumentException {
    return getSFTimestamp(
        timestampStr,
        TimeUtil.TimestampUnit.IN_SECONDS,
        scale,
        internalColumnType,
        resultVersion,
        sessionTZ);
  }

  /**
   * Convert a timestamp internal value (scaled number of seconds + fractional seconds or
   * nanoseconds) into a SFTimestamp.
   *
   * @param timestampStr timestamp object
   * @param unit IN_SECONDS or IN_NANOSECONDS, a unit of epoch time in timestampStr
   * @param scale timestamp scale for IN_SECONDS. Must be 0 for IN_NANOSECONDS.
   * @param internalColumnType snowflake timestamp type
   * @param resultVersion For new result version, timestamp with timezone is formatted as the
   *     seconds since epoch with fractional part in the decimal followed by time zone index. E.g.:
   *     "123.456 1440". Here 123.456 is the * number of seconds since epoch and 1440 is the
   *     timezone index.
   * @param sessionTZ session timezone
   * @return converted snowflake timestamp object
   * @throws IllegalArgumentException if timestampStr is an invalid timestamp
   */
  public static SFTimestamp getSFTimestamp(
      String timestampStr,
      TimeUtil.TimestampUnit unit,
      int scale,
      TimeUtil.TimestampType internalColumnType,
      long resultVersion,
      TimeZone sessionTZ)
      throws IllegalArgumentException {
    assert unit == TimeUtil.TimestampUnit.IN_NANOSECONDS && scale == 0
        || unit == TimeUtil.TimestampUnit.IN_SECONDS && scale >= 0;
    try {
      BigDecimal fractionsSinceEpoch;

      // Derive the used timezone - if NULL, will be extracted from the number.
      TimeZone tz;
      switch (internalColumnType) {
        case TIMESTAMP_NTZ:
          fractionsSinceEpoch = parseSecondsSinceEpoch(timestampStr, scale);
          // Always in UTC
          tz = utcTZ;
          break;
        case TIMESTAMP_TZ:

          /*
           * For new result version, timestamp with timezone is formatted as
           * the seconds since epoch with fractional part in the decimal followed
           * by time zone index. E.g.: "123.456 1440". Here 123.456 is the
           * number of seconds since epoch and 1440 is the timezone index.
           */
          if (resultVersion > 0) {
            int indexForSeparator = timestampStr.indexOf(' ');
            String secondsSinceEpochStr = timestampStr.substring(0, indexForSeparator);
            String timezoneIndexStr = timestampStr.substring(indexForSeparator + 1);

            fractionsSinceEpoch = parseSecondsSinceEpoch(secondsSinceEpochStr, scale);

            tz = SFTimestamp.convertTimezoneIndexToTimeZone(Integer.parseInt(timezoneIndexStr));
          } else {
            fractionsSinceEpoch = parseSecondsSinceEpoch(timestampStr, scale);

            // Timezone needs to be derived from the binary value for old
            // result version
            tz = null;
          }
          break;
        default:
          // Timezone from the environment
          assert internalColumnType == TimeUtil.TimestampType.TIMESTAMP_LTZ;
          fractionsSinceEpoch = parseSecondsSinceEpoch(timestampStr, scale);
          tz = sessionTZ;
          break;
      }

      // Construct a timestamp in the proper timezone
      if (unit == TimeUtil.TimestampUnit.IN_SECONDS) {
        return SFTimestamp.fromBinary(fractionsSinceEpoch, scale, tz);
      } else {
        return SFTimestamp.fromNanoseconds(fractionsSinceEpoch, tz);
      }
    } catch (NumberFormatException ex) {
      throw new IllegalArgumentException(ex.getMessage());
    }
  }

  /**
   * Convert a time internal value (scaled number of seconds + fractional seconds) into an SFTime.
   *
   * <p>Example: getSFTime("123.456", 5) returns an SFTime for 00:02:03.45600.
   *
   * @param obj time object
   * @param scale time scale
   * @return snowflake time object
   * @throws IllegalArgumentException if time is invalid
   */
  public static SFTime getSFTime(String obj, int scale) throws IllegalArgumentException {
    try {
      long fractionsSinceMidnight = parseSecondsSinceEpoch(obj, scale).longValue();
      return SFTime.fromFractionalSeconds(fractionsSinceMidnight, scale);
    } catch (NumberFormatException ex) {
      throw new IllegalArgumentException(ex.getMessage());
    }
  }

  /**
   * Parse seconds since epoch with both seconds and fractional seconds after decimal point (e.g
   * 123.456 with a scale of 3) to a representation with fractions normalized to an integer (e.g.
   * 123456)
   *
   * @param secondsSinceEpochStr
   * @param scale
   * @return a BigDecimal containing the number of fractional seconds since epoch.
   */
  private static BigDecimal parseSecondsSinceEpoch(String secondsSinceEpochStr, int scale) {
    // seconds since epoch has both seconds and fractional seconds after decimal
    // point. Ex: 134567890.12345678
    // Note: can actually contain timezone in the lowest part
    // Example: obj is e.g. "123.456" (scale=3)
    //          Then, secondsSinceEpoch is 123.456
    BigDecimal secondsSinceEpoch = new BigDecimal(secondsSinceEpochStr);

    // Representation with fractions normalized to an integer
    // Note: can actually contain timezone in the lowest part
    // Example: fractionsSinceEpoch is 123456
    return secondsSinceEpoch.scaleByPowerOfTen(scale);
  }

  /**
   * Returns true if the specified instant corresponds to a wallclock time that is "ambiguous" in
   * the given timezone because of daylight saving time rules. For example, '2016-11-06 01:30' is
   * ambiguous in time zone 'America/Los_Angeles', because the clock goes from 2:00 back to 1:00; so
   * '2016-11-06 01:30 PT' can mean either '2016-11-06 08:30 GMT' or '2016-01-30 09:30 GMT'.
   *
   * <p>Always returns false for instances of CCompatibleTimeZone.
   *
   * @param dateInMs
   * @param tz
   * @return true if the given timestamp is ambiguous due to DST rules.
   */
  public static boolean isDSTAmbiguous(long dateInMs, TimeZone tz) {
    int dstSavings = tz.getDSTSavings();

    if (dstSavings == 0) {
      // No DST in this time zone
      return false;
    }

    long datePrevHour = dateInMs - dstSavings;

    int tzOffset = tz.getOffset(dateInMs);
    int tzOffsetPrevHour = tz.getOffset(datePrevHour);

    return (tzOffsetPrevHour - tzOffset == dstSavings);
  }

  /**
   * Returns true if the specified instant is illegal in the current timezone because of daylight
   * saving time rules. For example, '2016-03-13 02:30' is illegal in time zone
   * 'America/Los_Angeles', because the clock goes from 2:00 straight to 3:00.
   *
   * <p>(Note that illegal timestamps can't be detected from millis since epoch, since an illegal
   * timestamp will be mapped to a legal timestamp as it is converted to millis since epoch. We need
   * the timestamp to be broken into its components. Hence the asymmetry in the arguments compared
   * to isDSTAmbiguous()).
   *
   * <p>Always returns false for instances of CCompatibleTimeZone.
   *
   * @param era
   * @param year
   * @param month
   * @param dayOfMonth
   * @param dayOfWeek
   * @param millisecondWithinDay
   * @param tz
   * @return true if the given timestamp is illegal due to DST rules.
   */
  public static boolean isDSTIllegal(
      int era,
      int year,
      int month,
      int dayOfMonth,
      int dayOfWeek,
      int millisecondWithinDay,
      TimeZone tz) {
    int dstSavings = tz.getDSTSavings();

    if (dstSavings == 0) {
      // No DST in this time zone
      return false;
    }

    // dstSavings is usually 1 hour, hence the naming
    int millisecondWithinDayPrevHour = millisecondWithinDay - dstSavings;

    if (millisecondWithinDayPrevHour < 0) {
      // Handling this would be complicated.
      // Luckily, no DST I've ever heard of changes on a day boundary.
      return false;
    }

    int tzOffset = tz.getOffset(era, year, month, dayOfMonth, dayOfWeek, millisecondWithinDay);

    int tzOffsetPrevHour =
        tz.getOffset(era, year, month, dayOfMonth, dayOfWeek, millisecondWithinDayPrevHour);

    return (tzOffset - tzOffsetPrevHour == dstSavings);
  }

  /**
   * Extracts YEAR as a signed integer, taking care of BC.
   *
   * @param cal calendar to extract YEAR from
   * @return ISO year extracted
   */
  public static int getYearAsInt(Calendar cal) {
    final int era = cal.get(Calendar.ERA);
    int year = cal.get(Calendar.YEAR);
    if (era == GregorianCalendar.BC) {
      // ISO Year 0 is represented as year 1 of era BC. So need to negate, and add 1.
      year = -year + 1;
    }
    return year;
  }
}
