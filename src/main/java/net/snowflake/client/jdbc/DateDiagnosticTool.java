package net.snowflake.client.jdbc;

import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

import java.sql.Date;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.TimeZone;

public class DateDiagnosticTool {
  private static final SFLogger logger = SFLoggerFactory.getLogger(DateDiagnosticTool.class);
  public static void diagnose() {
    logger.debug("============ Starting diagnostics tool ============");
    testUtilDate();
    testJavaTime();
    printSystemInfo();
    printTzdbInfo("UTC");
    printTzdbInfo(TimeZone.getDefault().getID());
    logger.debug("============ Ending diagnostics ============");
  }

  private static void testUtilDate() {
    logger.debug("============ START java.sql.Date test ============");
    TimeZone timeZone = TimeZone.getDefault();

    // start customer code
    Calendar cal = Calendar.getInstance(timeZone, Locale.US);
    cal.clear();
    cal.set(Calendar.ERA, GregorianCalendar.AD);
    cal.set(Calendar.YEAR, 2016);
    cal.set(Calendar.MONTH, 2 - 1);
    cal.set(Calendar.DATE, 19);
    long millis = cal.getTimeInMillis();
    Date customerDate = new Date(millis);
    // end customer code

    // start jdbc driver SnowflakePreparedStatementV1.setDate
    int offset = TimeZone.getDefault().getOffset(customerDate.getTime());
    String value = String.valueOf(customerDate.getTime() + offset);
    // end jdbc driver SnowflakePreparedStatementV1.setDate

    // start jdbc driver BindUploader.constructor
    Calendar calendar = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    dateFormat.setCalendar(calendar);
    // end jdbc driver BindUploader.constructor

    // start jdbc driver BindUploader.synchronizedDateFormat
    Long parsed = Long.parseLong(value);
    java.sql.Date sqlDate = new java.sql.Date(parsed);
    String formattedWithDefaultTimeZone = dateFormat.format(sqlDate);
    // end jdbc driver BindUploader.synchronizedDateFormat

    printDateFormatDetails(dateFormat);
    printCalendarDetails(dateFormat.getCalendar());
    logger.debug("Customer date Class: {}", customerDate.getClass().getName()); // Should be java.sql.Date
    logger.debug("date Class: {}", sqlDate.getClass().getName()); // Should be java.sql.Date
    logger.debug("date Millis: {}", sqlDate.getTime()); // Should be identical on both machines
    logger.debug("SQL Date: {}", sqlDate);

    logger.debug("Calendar milliseconds: {}", millis);
    logger.debug("Customer date: {}", customerDate);
    logger.debug("Customer date time: {}", customerDate.getTime());
    logger.debug("Offset: {}", offset);
    logger.debug("Parsed value: {}", parsed);
    logger.debug("Formatted Date: {}", formattedWithDefaultTimeZone);
    logger.debug("============ END java.sql.Date test ============");
  }

  private static void testJavaTime() {
    logger.debug("============ START java.time test ============");
    Calendar cal = Calendar.getInstance(TimeZone.getDefault(), Locale.US);
    cal.clear();
    cal.set(Calendar.ERA, GregorianCalendar.AD);
    cal.set(Calendar.YEAR, 2016);
    cal.set(Calendar.MONTH, 2 - 1);
    cal.set(Calendar.DATE, 19);
    long millis = cal.getTimeInMillis();

    ZoneId systemTimezone = ZoneId.systemDefault();
    ZoneId utcZone = ZoneId.of("UTC");
    Instant initialInstant = Instant.ofEpochMilli(millis);
    Duration offsetDuration = Duration.ofSeconds(systemTimezone.getRules().getOffset(initialInstant).getTotalSeconds());
    Instant adjustedInstant = initialInstant.plus(offsetDuration);
    DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    LocalDate datePartInUtc = adjustedInstant.atZone(utcZone).toLocalDate();
    String formattedResult = datePartInUtc.format(dateFormatter);

    logger.debug("System time zone: {}", systemTimezone);
    logger.debug("Millis: {}", millis);
    logger.debug("Initial Instant (UTC): {}", initialInstant);
    logger.debug("Offset: {}", offsetDuration);
    logger.debug("Adjusted Instant (UTC): {}", adjustedInstant);
    logger.debug("Formatted Date: {}", formattedResult);
    logger.debug("============ END java.time test ============");
  }

  private static void printDateFormatDetails(SimpleDateFormat dateFormat) {
    logger.debug("--- SimpleDateFormat State ---");
    logger.debug("dateFormat Class: {}", dateFormat.getClass().getName());
    logger.debug("dateFormat Pattern: {}", dateFormat.toPattern());
    logger.debug("dateFormat 2DigitYearStart: {}", dateFormat.get2DigitYearStart());
    try {
      // NumberFormat can be complex, just print its toString representation
      NumberFormat nf = dateFormat.getNumberFormat();
      logger.debug("dateFormat NumberFormat: {}", (nf != null ? nf.toString() : "null"));
    } catch (Exception e) {
      logger.debug("dateFormat NumberFormat: Error getting - " + e.getMessage());
    }
  }

  private static void printCalendarDetails(Calendar internalCalendar) {
    logger.debug("--- Calendar Used By SimpleDateFormat (via getCalendar()) ---");
    logger.debug("Internal Calendar Class: {}", internalCalendar.getClass().getName());
    TimeZone internalTimeZone = internalCalendar.getTimeZone();
    logger.debug("Internal Calendar TimeZone ID: {}", internalTimeZone.getID());
    logger.debug("Internal Calendar TimeZone Raw Offset (ms): {}", internalTimeZone.getRawOffset());
    logger.debug("Internal Calendar TimeZone Uses DST: {}", internalTimeZone.useDaylightTime());
    logger.debug("Internal Calendar Is Lenient: {}", internalCalendar.isLenient());
    logger.debug("Internal Calendar First Day Of Week: {}", internalCalendar.getFirstDayOfWeek() + " (Sunday=" + Calendar.SUNDAY + ", Monday=" + Calendar.MONDAY + ")");
    logger.debug("Internal Calendar Minimal Days In First Week: {}", internalCalendar.getMinimalDaysInFirstWeek());
    try {
      // GregorianCalendar specific detail
      if (internalCalendar instanceof GregorianCalendar) {
        logger.debug("Internal Calendar Gregorian Change Date: {}", ((GregorianCalendar) internalCalendar).getGregorianChange());
      }
    } catch (Exception e) {
      logger.debug("Internal Calendar Gregorian Change Date: Error getting - " + e.getMessage());
    }
  }

  private static void printSystemInfo() {
    // --- Dump Environment Information ---
    logger.debug("--- Java Environment Info ---");
    // Core Java Version/Vendor (Most Important for Bug Identification)
    logger.debug("java.version: {}", System.getProperty("java.version"));
    logger.debug("java.runtime.version: {}", System.getProperty("java.runtime.version"));
    logger.debug("java.vendor: {}", System.getProperty("java.vendor"));
    logger.debug("java.vm.name: {}", System.getProperty("java.vm.name"));
    logger.debug("java.vm.version: {}", System.getProperty("java.vm.version"));
    logger.debug("java.vm.vendor: {}", System.getProperty("java.vm.vendor"));

    // Operating System
    logger.debug("os.name: {}", System.getProperty("os.name"));
    logger.debug("os.version: {}", System.getProperty("os.version"));
    logger.debug("os.arch: {}", System.getProperty("os.arch"));

    // Locale Settings (Affects default formatting patterns)
    logger.debug("Default Locale: {}", Locale.getDefault().toString());
    logger.debug("user.language: {}", System.getProperty("user.language"));
    // Use user.region if available (newer property), fallback to user.country
    String region = System.getProperty("user.region");
    if (region == null || region.isEmpty()) {
      region = System.getProperty("user.country");
    }
    logger.debug("user.region/country: {}", region);
    logger.debug("user.script: {}", System.getProperty("user.script")); // Less common, but relevant for locale
    logger.debug("user.variant: {}", System.getProperty("user.variant")); // Less common

    // Timezone Settings (Capture the *initial* default)
    TimeZone initialDefaultTimeZone = TimeZone.getDefault();
    logger.debug("Initial Default TimeZone ID: {}", initialDefaultTimeZone.getID());
    logger.debug("Initial Default TimeZone Offset (ms): {}", initialDefaultTimeZone.getRawOffset());
    logger.debug("Initial Default TimeZone Uses DST: {}", initialDefaultTimeZone.useDaylightTime());
    logger.debug("user.timezone property: {}", System.getProperty("user.timezone")); // May reflect launch setting

    // Other potentially relevant properties
    logger.debug("file.encoding: {}", System.getProperty("file.encoding"));
    logger.debug("-----------------------------");
    logger.debug("Starting specific test logic now...");
  }

  private static void printTzdbInfo(String zone) {
    logger.debug("--- Timezone Database (TZDB) Info for {}---", zone);
    try {
      java.time.zone.ZoneRulesProvider.getVersions(zone);
      java.util.NavigableMap<String, java.time.zone.ZoneRules> versions = java.time.zone.ZoneRulesProvider.getVersions(zone);

      if (versions != null && !versions.isEmpty()) {
        String latestVersionId = versions.lastKey();
        logger.debug("TZDB Version (from ZoneRulesProvider): {}", latestVersionId);
      } else {
        logger.debug("TZDB Version (from ZoneRulesProvider): Not available or empty map.");
      }
    } catch (Throwable t) {
      logger.debug("TZDB Version (from ZoneRulesProvider): Failed to retrieve (" + t.getClass().getName() + ")");
    }
  }
}
