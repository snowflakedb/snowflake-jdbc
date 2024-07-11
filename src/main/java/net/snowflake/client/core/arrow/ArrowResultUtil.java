/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core.arrow;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.Calendar;
import java.util.TimeZone;
import net.snowflake.client.core.ResultUtil;
import net.snowflake.client.core.SFException;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeTimestampWithTimezone;
import net.snowflake.client.log.ArgSupplier;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.common.core.CalendarCache;

/** Result utility methods specifically for Arrow format */
public class ArrowResultUtil {
  private static final SFLogger logger = SFLoggerFactory.getLogger(ArrowResultUtil.class);

  private static final int[] POWERS_OF_10 = {
    1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000
  };

  public static final int MAX_SCALE_POWERS_OF_10 = 9;

  public static long powerOfTen(int pow) {
    long val = 1;
    while (pow > MAX_SCALE_POWERS_OF_10) {
      val *= POWERS_OF_10[MAX_SCALE_POWERS_OF_10];
      pow -= MAX_SCALE_POWERS_OF_10;
    }
    return val * POWERS_OF_10[pow];
  }

  public static String getStringFormat(int scale) {
    StringBuilder sb = new StringBuilder();
    return sb.append("%.").append(scale).append('f').toString();
  }

  /**
   * new method to get Date from integer
   *
   * @param day
   * @return Date
   */
  public static Date getDate(int day) {
    LocalDate localDate = LocalDate.ofEpochDay(day);
    return Date.valueOf(localDate);
  }

  /**
   * Method to get Date from integer using timezone offsets
   *
   * @param day
   * @param oldTz
   * @param newTz
   * @return
   * @throws SFException
   */
  public static Date getDate(int day, TimeZone oldTz, TimeZone newTz) throws SFException {
    try {
      // return the date adjusted to the JVM default time zone
      long milliSecsSinceEpoch = (long) day * ResultUtil.MILLIS_IN_ONE_DAY;

      long milliSecsSinceEpochNew =
          milliSecsSinceEpoch + moveToTimeZoneOffset(milliSecsSinceEpoch, oldTz, newTz);

      Date preDate = new Date(milliSecsSinceEpochNew);

      // if date is on or before 1582-10-04, apply the difference
      // by (H-H/4-2) where H is the hundreds digit of the year according to:
      // http://en.wikipedia.org/wiki/Gregorian_calendar
      Date newDate = ResultUtil.adjustDate(preDate);
      logger.debug(
          "Adjust date from {} to {}",
          (ArgSupplier) preDate::toString,
          (ArgSupplier) newDate::toString);
      return newDate;
    } catch (NumberFormatException ex) {
      throw new SFException(ErrorCode.INTERNAL_ERROR, "Invalid date value: " + day);
    }
  }

  /**
   * simplified moveToTimeZone method
   *
   * @param milliSecsSinceEpoch
   * @param oldTZ
   * @param newTZ
   * @return offset
   */
  private static long moveToTimeZoneOffset(
      long milliSecsSinceEpoch, TimeZone oldTZ, TimeZone newTZ) {
    if (oldTZ.hasSameRules(newTZ)) {
      // same time zone
      return 0;
    }
    int offsetMillisInOldTZ = oldTZ.getOffset(milliSecsSinceEpoch);

    Calendar calendar = CalendarCache.get(oldTZ);
    calendar.setTimeInMillis(milliSecsSinceEpoch);

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
        newTZ.getOffset(era, year, month, dayOfMonth, dayOfWeek, millisecondWithinDay);

    int offsetMillis = offsetMillisInOldTZ - offsetMillisInNewTZ;
    return offsetMillis;
  }

  /**
   * move the input timestamp form oldTZ to newTZ
   *
   * @param ts
   * @param oldTZ
   * @param newTZ
   * @return timestamp in newTZ
   */
  public static Timestamp moveToTimeZone(Timestamp ts, TimeZone oldTZ, TimeZone newTZ) {
    long offset = moveToTimeZoneOffset(ts.getTime(), oldTZ, newTZ);
    if (offset == 0) {
      return ts;
    }
    int nanos = ts.getNanos();
    ts = new Timestamp(ts.getTime() + offset);
    ts.setNanos(nanos);
    return ts;
  }

  /**
   * generate Java Timestamp object
   *
   * @param epoch the value since epoch time
   * @param scale the scale of the value
   * @return
   */
  public static Timestamp toJavaTimestamp(long epoch, int scale) {
    long seconds = epoch / powerOfTen(scale);
    int fraction = (int) ((epoch % powerOfTen(scale)) * powerOfTen(9 - scale));
    if (fraction < 0) {
      // handle negative case here
      seconds--;
      fraction += 1000000000;
    }
    return createTimestamp(seconds, fraction, TimeZone.getDefault(), false);
  }

  /**
   * check whether the input seconds out of the scope of Java timestamp
   *
   * @param seconds
   * @return
   */
  public static boolean isTimestampOverflow(long seconds) {
    return seconds < Long.MIN_VALUE / powerOfTen(3) || seconds > Long.MAX_VALUE / powerOfTen(3);
  }

  /**
   * create Java timestamp using seconds since epoch and fraction in nanoseconds For example,
   * 1232.234 represents as epoch = 1232 and fraction = 234,000,000 For example, -1232.234
   * represents as epoch = -1233 and fraction = 766,000,000 For example, -0.13 represents as epoch =
   * -1 and fraction = 870,000,000
   *
   * @param seconds
   * @param fraction
   * @param timezone - The timezone being used for the toString() formatting
   * @param timezone -
   * @return java timestamp object
   */
  public static Timestamp createTimestamp(
      long seconds, int fraction, TimeZone timezone, boolean useSessionTz) {
    // If JDBC_TREAT_TIMESTAMP_NTZ_AS_UTC=true, set timezone to UTC to get
    // timestamp object. This will avoid moving the timezone and creating
    // daylight savings offset errors.
    if (useSessionTz) {
      return new SnowflakeTimestampWithTimezone(
          seconds * ArrowResultUtil.powerOfTen(3), fraction, timezone);
    }
    Timestamp ts = new Timestamp(seconds * ArrowResultUtil.powerOfTen(3));
    ts.setNanos(fraction);
    return ts;
  }
}
