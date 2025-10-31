package net.snowflake.client.common.core;

import java.util.Calendar;
import java.util.TimeZone;
import net.snowflake.client.common.util.TimeUtil;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;

/**
 * Simple interface for uniform accessing of moments in time.
 *
 * @author mzukowski
 */
@SnowflakeJdbcInternalApi
public abstract class SFInstant {
  // Additional component type not covered by Calendar
  public static final int DAY_OF_WEEK_ISO = 10001;
  public static final int WEEK_ISO = 10002;
  public static final int YEAR_OF_WEEK = 10003;
  public static final int YEAR_OF_WEEK_ISO = 10004;

  // Used in subclasses
  protected static final TimeZone GMT = TimeZone.getTimeZone("GMT");
  protected static final int[] POWERS_OF_TEN = {
    1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000,
  };

  public abstract int extract(int field, Integer optWeekStart, Integer optWoyPolicy);

  /**
   * Extract a particular component of a date.
   *
   * @param field field id as specified in the Calendar class.
   * @return The value of the extracted field.
   */
  public int extract(int field) {
    return extract(field, null, null);
  }

  /**
   * Generic function children use to implement public extract.
   *
   * @param field Field id as specified in the Calendar class. We also support additional components
   *     defined in SFInstant
   * @param tz TimeZone to use
   * @param time Moment in time to extract from
   * @param optWeekStart Optional WEEK_START value
   * @param optWoyPolicy Optional WEEK_OF_YEAR_POLICY value
   * @return The value of the extracted field.
   */
  protected int extract(
      int field, TimeZone tz, long time, Integer optWeekStart, Integer optWoyPolicy) {
    Calendar calendar = CalendarCache.get(tz);

    // Mapping of WEEK_START 1..7 values (-1) to Java constants
    int firstDays[] = {
      Calendar.MONDAY,
      Calendar.TUESDAY,
      Calendar.WEDNESDAY,
      Calendar.THURSDAY,
      Calendar.FRIDAY,
      Calendar.SATURDAY,
      Calendar.SUNDAY
    };

    // Set the time
    calendar.setTimeInMillis(time);

    // Remember if we're processing an ISO component
    boolean isIso = false;
    // Remember if we're in the Jan1st semantics
    boolean isJan1st = false;

    int weekStart = optWeekStart != null ? optWeekStart : 0; // 0,1-Monday..7-Sunday
    weekStart = (weekStart == 0) ? 1 : weekStart; // 1-Monday..7-Sunday
    weekStart = weekStart % 7; // 0-Sunday..6-Saturday

    if (field == DAY_OF_WEEK_ISO || field == WEEK_ISO || field == YEAR_OF_WEEK_ISO) {
      // An ISO component. Ignore optional params
      calendar.setFirstDayOfWeek(Calendar.MONDAY);
      calendar.setMinimalDaysInFirstWeek(4);
      // And continue using standard components
      if (field == DAY_OF_WEEK_ISO) {
        field = Calendar.DAY_OF_WEEK;
      } else if (field == WEEK_ISO) {
        field = Calendar.WEEK_OF_YEAR;
      } else if (field == YEAR_OF_WEEK_ISO) {
        field = YEAR_OF_WEEK;
      }
      // Remember we're in ISO mode
      isIso = true;
    } else {
      // Process optional parameters
      int firstDay = Calendar.MONDAY;
      if (optWeekStart != null && optWeekStart != 0) {
        assert (optWeekStart >= 1 && optWeekStart <= 7);
        firstDay = firstDays[optWeekStart - 1];
      }

      int minimalDaysInFirstWeek = 4;
      if (optWoyPolicy != null && optWoyPolicy != 0) {
        assert (optWoyPolicy == 1);
        minimalDaysInFirstWeek = 1;
        // Remember we're in Jan1st mode
        isJan1st = true;
      }

      calendar.setFirstDayOfWeek(firstDay);
      calendar.setMinimalDaysInFirstWeek(minimalDaysInFirstWeek);
    }

    // Extract the field. Special code for some fields
    int val;

    if (isJan1st && field == YEAR_OF_WEEK) {
      // Simple, same as year
      val = TimeUtil.getYearAsInt(calendar);
    } else if (isJan1st && field == Calendar.WEEK_OF_YEAR) {
      // Manually compute it, it's easier than adapting to Java's weird semantics
      long outputYear = TimeUtil.getYearAsInt(calendar);
      int yday = calendar.get(Calendar.DAY_OF_YEAR); // 1..365
      yday--; // now it's 0..365, like in C
      int wday = calendar.get(Calendar.DAY_OF_WEEK); // Sunday..Saturday
      wday -= Calendar.SUNDAY; // now it's 0(Sunday)..6(Saturday), like in C

      // Now, check day of week on Jan 1st, same format as tm_wday, 0..6
      // We use 700, as it's divisible by 7 and larger by 365,
      // to guarantee non-negative result.
      int dowJan1 = (wday - yday + 700) % 7;

      int week = (yday + (7 - weekStart + dowJan1) % 7) / 7 + 1;

      val = week;
    } else if (field == YEAR_OF_WEEK) {
      int year = TimeUtil.getYearAsInt(calendar);
      int week = calendar.get(Calendar.WEEK_OF_YEAR);
      int month = calendar.get(Calendar.MONTH);

      if (month == 0 && week > 10) {
        // January, but large week - it belongs to the previous year
        year--;
      } else if (month == 11 && week < 10) {
        // December, but small week - it belongs to the next year
        year++;
      }
      val = year;
    } else if (field == Calendar.YEAR) {
      val = TimeUtil.getYearAsInt(calendar);
    } else {
      // For most components, just use Calendar's output
      val = calendar.get(field);

      // Post-process some fields
      if (field == Calendar.MONTH) { // Calendar.MONTH is 0-based, fix it
        val += 1;
      } else if (field == Calendar.DAY_OF_WEEK) {
        if (isIso) {
          // Convert Java SUNDAY..SATURDAY to 1-Monday..7-Sunday
          val = (val + 6 - Calendar.SUNDAY) % 7 + 1;
        } else if (optWeekStart == null || optWeekStart == 0) {
          // Old behavior, we want 0-Sunday .. 6-Saturday
          // Calendar.DAY_OF_WEEK has 1-Sunday, 7-Saturday
          val -= 1;
        } else {
          // Fixed beginning of the week
          // For weekStart = 2, we want Tue=1,Wed=2,...,Mon=7
          assert (optWeekStart >= 1 && optWeekStart <= 7);
          val = (val + 7 + 6 - optWeekStart) % 7 + 1;
        }
      }
    }
    return val;
  }
}
