/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core.arrow;

import net.snowflake.client.core.IncidentUtil;
import net.snowflake.client.core.ResultUtil;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SFSession;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.log.ArgSupplier;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.common.core.CalendarCache;

import java.sql.Date;
import java.util.Calendar;
import java.util.TimeZone;

/**
 * Result utility methods specifically for Arrow format
 */
public class ArrowResultUtil
{
  static final SFLogger logger = SFLoggerFactory.getLogger(ArrowResultUtil.class);

  /**
   * new method to get Date from integer
   *
   * @param day
   * @param tz
   * @param session
   * @return
   * @throws SFException
   */
  public static Date getDate(int day, TimeZone tz, SFSession session) throws SFException
  {
    try
    {
      // return the date adjusted to the JVM default time zone
      long milliSecsSinceEpoch = (long) day * ResultUtil.MILLIS_IN_ONE_DAY;

      long milliSecsSinceEpochNew = moveToTimeZone(milliSecsSinceEpoch, TimeZone.getTimeZone("UTC"), tz);

      Date preDate = new Date(milliSecsSinceEpochNew);

      // if date is on or before 1582-10-04, apply the difference
      // by (H-H/4-2) where H is the hundreds digit of the year according to:
      // http://en.wikipedia.org/wiki/Gregorian_calendar
      Date newDate = ResultUtil.adjustDate(preDate);
      logger.debug("Adjust date from {} to {}",
                   (ArgSupplier) preDate::toString,
                   (ArgSupplier) newDate::toString);
      return newDate;
    }
    catch (NumberFormatException ex)
    {
      throw (SFException) IncidentUtil.generateIncidentV2WithException(
          session,
          new SFException(ErrorCode.INTERNAL_ERROR,
                          "Invalid date value: " + day),
          null,
          null);
    }
  }

  /**
   * simplified moveToTimeZone method
   *
   * @param epoch
   * @param oldTZ
   * @param newTZ
   * @return moved epoch
   */
  public static long moveToTimeZone(long epoch, TimeZone oldTZ, TimeZone newTZ)
  {
    if (oldTZ.getRawOffset() == newTZ.getRawOffset())
    {
      // same time zone
      return epoch;
    }
    int offsetMillisInOldTZ = oldTZ.getOffset(epoch);

    Calendar calendar = CalendarCache.get(oldTZ);
    calendar.setTimeInMillis(epoch);

    int millisecondWithinDay = ((calendar.get(Calendar.HOUR_OF_DAY) * 60 +
                                 calendar.get(Calendar.MINUTE)) * 60 +
                                calendar.get(Calendar.SECOND)) * 1000 +
                               calendar.get(Calendar.MILLISECOND);

    int era = calendar.get(Calendar.ERA);
    int year = calendar.get(Calendar.YEAR);
    int month = calendar.get(Calendar.MONTH);
    int dayOfMonth = calendar.get(Calendar.DAY_OF_MONTH);
    int dayOfWeek = calendar.get(Calendar.DAY_OF_WEEK);

    int offsetMillisInNewTZ = newTZ.getOffset(
        era,
        year,
        month,
        dayOfMonth,
        dayOfWeek,
        millisecondWithinDay);

    int offsetMillis = offsetMillisInOldTZ - offsetMillisInNewTZ;
    return epoch + offsetMillis;
  }
}
