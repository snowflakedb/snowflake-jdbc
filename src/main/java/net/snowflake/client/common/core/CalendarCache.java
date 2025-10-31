package net.snowflake.client.common.core;

import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.TimeZone;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;

/**
 * Created by mzukowski on 3/13/16.
 *
 * <p>Implements a simple thread-local cache for GregorianCalendar objects. Note, by default it is
 * not enabled, and will re-create the objects.
 */
@SnowflakeJdbcInternalApi
public class CalendarCache {
  /** TimeZone factory from which we get timezones. This enables custom TimeZone implementations. */
  public interface TimeZoneFactory {
    TimeZone getTimeZone(String id);
  }

  /** Default TimeZoneFactory implementation producing Java timezones. */
  private static class DefaultTimeZoneFactory implements CalendarCache.TimeZoneFactory {
    @Override
    public TimeZone getTimeZone(String id) {
      return TimeZone.getTimeZone(id);
    }
  }

  private static class CalendarCacheState {
    HashMap<String, GregorianCalendar> map;

    boolean enabled;

    /**
     * If set, we will always produce clones of cached objects. This still makes it faster than
     * completely new object, but makes objects safe for external operations that modify Calendar
     * (e.g. SimpleDateFormat can change the timezone).
     */
    boolean produceClones;

    /** If set, we always use Gregorian calendar, even before 1582 */
    boolean alwaysGregorian;

    /** TimeZoneFactory from which timezones are fetched. */
    CalendarCache.TimeZoneFactory timeZoneFactory;

    CalendarCacheState() {
      map = new HashMap<>();
      enabled = false;
      produceClones = true;
      alwaysGregorian = false;

      // set timeZoneFactory to a default one producing Java timezones
      timeZoneFactory = new CalendarCache.DefaultTimeZoneFactory();
    }
  }

  /** Thread local state */
  private static final ThreadLocal<CalendarCache.CalendarCacheState> localState =
      new ThreadLocal<CalendarCache.CalendarCacheState>() {
        @Override
        protected CalendarCache.CalendarCacheState initialValue() {
          return new CalendarCache.CalendarCacheState();
        }
      };

  /**
   * Enable or disable the cache
   *
   * @param enabled true to enable cache or false
   */
  public static void setEnabled(boolean enabled) {
    localState.get().enabled = enabled;
  }

  /**
   * Configure the CalendarCache to produce clones of cached objects (default behavior) or not. It
   * can be beneficial performance wise to not clone them, but it's possibly dangerous as _some_
   * operations using calendars (e.g. SimpleDateFormat) can modify the Calendar object.
   *
   * @param produceClones should it produce clones of cached objects.
   */
  public static void setProduceClones(boolean produceClones) {
    localState.get().produceClones = produceClones;
  }

  /**
   * Set if the the calendars produced should not observe Julian calendar before 1582.
   *
   * <p>Calling this function also clears the cache.
   */
  public static void setAlwaysGregorian(boolean alwaysGregorian) {
    localState.get().alwaysGregorian = alwaysGregorian;

    // Clear the cache, in case old values had a different setting
    localState.get().map.clear();
  }

  /**
   * Sets the timezone factory.
   *
   * @param tzFactory The TimeZoneFactory object to use, or null if the default timezone factory is
   *     to be used.
   */
  public static void setTimeZoneFactory(CalendarCache.TimeZoneFactory tzFactory) {
    localState.get().timeZoneFactory =
        tzFactory == null ? new CalendarCache.DefaultTimeZoneFactory() : tzFactory;

    // Clear the cache, in case old values had a different setting
    localState.get().map.clear();
  }

  /**
   * Helper function. Returns a GregorianCalendar object.
   *
   * @param timezone Timezone to create the calendar in.
   * @param tzId The id of the timezone (e.g. "America/Los_Angeles")
   * @param calId Explicit ID of the calendar. Can be used e.g. if more than one copy of the
   *     calendar for the same timezone is needed. Note, if specified, the caller is responsible for
   *     making sure it's always the same timezone.
   * @return The GregorianCalendar object. Note, it can be reused by other consumers, so be careful
   *     changing any settings in it
   */
  public static GregorianCalendar get(TimeZone timezone, String tzId, String calId) {
    CalendarCache.CalendarCacheState state = localState.get();
    GregorianCalendar res;
    if (state.enabled) {
      res = state.map.get(calId);
      if (res == null) { // Not found, create it
        res = createCalendar(timezone, tzId);
        // Save a fully-prepped entry in the cache
        state.map.put(calId, res);
      } else { // Found in cache

        // Verify it's the same timezone, just in case
        if (timezone != null && !res.getTimeZone().hasSameRules(timezone)) {
          throw new IllegalArgumentException(
              "Cached timezone is not equivalent to the requested one:"
                  + timezone
                  + "  VS  "
                  + res.getTimeZone());
        }
        // Verify it's the same gregorian offset, just in case
        if (state.alwaysGregorian && res.getGregorianChange().getTime() != Long.MIN_VALUE) {
          throw new IllegalArgumentException(
              "Cached calendar gregorian offset is not set as expected:"
                  + res.getGregorianChange().getTime()
                  + "  VS  "
                  + Long.MIN_VALUE);
        }
      }
      if (state.produceClones) {
        // Produce clones - for both newly created and previously cached calendars
        res = (GregorianCalendar) res.clone();
      } else {
        // Returning a possibly previously used calendar, clear it
        res.clear();
      }
      return res;
    } else {
      // Cache disabled, just create it
      return createCalendar(timezone, tzId);
    }
  }

  /** Create a properly initialized GregorianCalendar */
  private static GregorianCalendar createCalendar(TimeZone timezone, String tzId) {
    final CalendarCache.CalendarCacheState state = localState.get();

    if (timezone == null) {
      // If not specified, create a timezone from an id
      timezone = state.timeZoneFactory.getTimeZone(tzId);
    }
    // Create a new calendar
    GregorianCalendar cal = new GregorianCalendar(timezone);
    // Clear it, seems sometimes needed
    cal.clear();
    // Set always gregorian if needed
    if (state.alwaysGregorian) {
      cal.setGregorianChange(new Date(Long.MIN_VALUE));
    }
    return cal;
  }

  /**
   * Return a GregorianCalendar object identified by id. If caching is enabled and an object with
   * the same id existed, it will be returned, otherwise a new object will be created.
   *
   * @param timezone Timezone to create the calendar in
   * @param calId Explicit ID of the calendar. Can be used e.g. if more than one copy of the
   *     calendar for the same timezone is needed. Note, if specified, the caller is responsible for
   *     making sure it's always the same timezone.
   * @return The GregorianCalendar object. Note, it can be reused by other consumers, so be careful
   *     changing any settings in it
   */
  public static GregorianCalendar get(TimeZone timezone, String calId) {
    return get(timezone, null, calId);
  }

  /**
   * Return a cached GregorianCalendar object. If doesn't exist, will be created.
   *
   * @param timezone Timezone to create the calendar in
   * @return The GregorianCalendar object. Note, it can be reused by other consumers, so do not
   *     change any non-basic settings in it (like date).
   */
  public static GregorianCalendar get(TimeZone timezone) {
    // Use TimeZone id to lookup the Calendar
    return get(timezone, null, timezone.getID());
  }

  /**
   * Return a cached GregorianCalendar object. If doesn't exist, will be created.
   *
   * @param tzId The id of the timezone (e.g. "America/Los_Angeles")
   * @return The GregorianCalendar object. Note, it can be reused by other consumers, so do not
   *     change any non-basic settings in it (like date).
   */
  public static GregorianCalendar get(String tzId) {
    // Pass null as timezone - we'll create it when needed.
    // We use tzId also as a calendar ID.
    return get(null, tzId, tzId);
  }
}
