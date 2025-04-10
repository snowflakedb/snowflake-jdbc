package net.snowflake.client.jdbc;

import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

import java.sql.Date;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.TimeZone;

public class DateDiagnosticTool {
  private static final SFLogger logger = SFLoggerFactory.getLogger(SnowflakeDriver.class);
  public static void diagnose() {
    logger.info("============ Starting diagnostics tool ============");
    testUtilDate();
    testJavaTime();
    printSystemInfo();
    printTzdbInfo("UTC");
    printTzdbInfo(TimeZone.getDefault().getID());
    logger.info("============ Ending diagnostics ============");
  }

  private static void testUtilDate() {
    logger.info("============ START java.sql.Date test ============");
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
    logger.info("Customer date Class: {}", customerDate.getClass().getName()); // Should be java.sql.Date
    logger.info("date Class: {}", sqlDate.getClass().getName()); // Should be java.sql.Date
    logger.info("date Millis: {}", sqlDate.getTime()); // Should be identical on both machines
    logger.info("SQL Date: {}", sqlDate);

    logger.info("Calendar milliseconds: {}", millis);
    logger.info("Customer date: {}", customerDate);
    logger.info("Customer date time: {}", customerDate.getTime());
    logger.info("Offset: {}", offset);
    logger.info("Parsed value: {}", parsed);
    logger.info("Formatted Date: {}", formattedWithDefaultTimeZone);
    logger.info("============ END java.sql.Date test ============");
  }

  private static void testJavaTime() {
    logger.info("============ START java.time test ============");
    ZoneId systemTimezone = ZoneId.systemDefault();
    ZoneId utcZone = ZoneId.of("UTC");
    ZonedDateTime systemDateTime = ZonedDateTime.of(LocalDate.of(2016, 2, 19), LocalTime.MIDNIGHT, systemTimezone);
    Instant initialInstant = systemDateTime.toInstant();
    Duration offsetDuration = Duration.ofSeconds(systemTimezone.getRules().getOffset(initialInstant).getTotalSeconds());
    Instant adjustedInstant = initialInstant.plus(offsetDuration);
    DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    LocalDate datePartInUtc = adjustedInstant.atZone(utcZone).toLocalDate();
    String formattedResult = datePartInUtc.format(dateFormatter);

    logger.info("System time zone DateTime: {}", systemDateTime);
    logger.info("Initial Instant (UTC): {}", initialInstant);
    logger.info("Offset: {}", offsetDuration);
    logger.info("Adjusted Instant (UTC): {}", adjustedInstant);
    logger.info("Formatted Date: {}", formattedResult);
    logger.info("============ END java.time test ============");
  }

  private static void printDateFormatDetails(SimpleDateFormat dateFormat) {
    logger.info("--- SimpleDateFormat State ---");
    logger.info("dateFormat Class: {}", dateFormat.getClass().getName());
    logger.info("dateFormat Pattern: {}", dateFormat.toPattern());
    logger.info("dateFormat 2DigitYearStart: {}", dateFormat.get2DigitYearStart());
    try {
      // NumberFormat can be complex, just print its toString representation
      NumberFormat nf = dateFormat.getNumberFormat();
      logger.info("dateFormat NumberFormat: {}", (nf != null ? nf.toString() : "null"));
    } catch (Exception e) {
      logger.info("dateFormat NumberFormat: Error getting - " + e.getMessage());
    }
  }

  private static void printCalendarDetails(Calendar internalCalendar) {
    logger.info("--- Calendar Used By SimpleDateFormat (via getCalendar()) ---");
    logger.info("Internal Calendar Class: {}", internalCalendar.getClass().getName());
    TimeZone internalTimeZone = internalCalendar.getTimeZone();
    logger.info("Internal Calendar TimeZone ID: {}", internalTimeZone.getID());
    logger.info("Internal Calendar TimeZone Raw Offset (ms): {}", internalTimeZone.getRawOffset());
    logger.info("Internal Calendar TimeZone Uses DST: {}", internalTimeZone.useDaylightTime());
    logger.info("Internal Calendar Is Lenient: {}", internalCalendar.isLenient());
    logger.info("Internal Calendar First Day Of Week: {}", internalCalendar.getFirstDayOfWeek() + " (Sunday=" + Calendar.SUNDAY + ", Monday=" + Calendar.MONDAY + ")");
    logger.info("Internal Calendar Minimal Days In First Week: {}", internalCalendar.getMinimalDaysInFirstWeek());
    try {
      // GregorianCalendar specific detail
      if (internalCalendar instanceof GregorianCalendar) {
        logger.info("Internal Calendar Gregorian Change Date: {}", ((GregorianCalendar) internalCalendar).getGregorianChange());
      }
    } catch (Exception e) {
      logger.info("Internal Calendar Gregorian Change Date: Error getting - " + e.getMessage());
    }
  }

  private static void printSystemInfo() {
    // --- Dump Environment Information ---
    logger.info("--- Java Environment Info ---");
    // Core Java Version/Vendor (Most Important for Bug Identification)
    logger.info("java.version: {}", System.getProperty("java.version"));
    logger.info("java.runtime.version: {}", System.getProperty("java.runtime.version"));
    logger.info("java.vendor: {}", System.getProperty("java.vendor"));
    logger.info("java.vm.name: {}", System.getProperty("java.vm.name"));
    logger.info("java.vm.version: {}", System.getProperty("java.vm.version"));
    logger.info("java.vm.vendor: {}", System.getProperty("java.vm.vendor"));

    // Operating System
    logger.info("os.name: {}", System.getProperty("os.name"));
    logger.info("os.version: {}", System.getProperty("os.version"));
    logger.info("os.arch: {}", System.getProperty("os.arch"));

    // Locale Settings (Affects default formatting patterns)
    logger.info("Default Locale: {}", Locale.getDefault().toString());
    logger.info("user.language: {}", System.getProperty("user.language"));
    // Use user.region if available (newer property), fallback to user.country
    String region = System.getProperty("user.region");
    if (region == null || region.isEmpty()) {
      region = System.getProperty("user.country");
    }
    logger.info("user.region/country: {}", region);
    logger.info("user.script: {}", System.getProperty("user.script")); // Less common, but relevant for locale
    logger.info("user.variant: {}", System.getProperty("user.variant")); // Less common

    // Timezone Settings (Capture the *initial* default)
    TimeZone initialDefaultTimeZone = TimeZone.getDefault();
    logger.info("Initial Default TimeZone ID: {}", initialDefaultTimeZone.getID());
    logger.info("Initial Default TimeZone Offset (ms): {}", initialDefaultTimeZone.getRawOffset());
    logger.info("Initial Default TimeZone Uses DST: {}", initialDefaultTimeZone.useDaylightTime());
    logger.info("user.timezone property: {}", System.getProperty("user.timezone")); // May reflect launch setting

    // Other potentially relevant properties
    logger.info("file.encoding: {}", System.getProperty("file.encoding"));
    logger.info("-----------------------------");
    logger.info("Starting specific test logic now...");
  }

  private static void printTzdbInfo(String zone) {
    logger.info("--- Timezone Database (TZDB) Info for {}---", zone);
    try {
      java.time.zone.ZoneRulesProvider.getVersions(zone);
      java.util.NavigableMap<String, java.time.zone.ZoneRules> versions = java.time.zone.ZoneRulesProvider.getVersions(zone);

      if (versions != null && !versions.isEmpty()) {
        String latestVersionId = versions.lastKey();
        logger.info("TZDB Version (from ZoneRulesProvider): {}", latestVersionId);
      } else {
        logger.info("TZDB Version (from ZoneRulesProvider): Not available or empty map.");
      }
    } catch (Throwable t) {
      logger.info("TZDB Version (from ZoneRulesProvider): Failed to retrieve (" + t.getClass().getName() + ")");
    }
  }
}
