/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import com.fasterxml.jackson.databind.JsonNode;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.*;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeUtil;
import net.snowflake.client.log.ArgSupplier;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.common.core.SFTime;
import net.snowflake.common.core.SFTimestamp;
import net.snowflake.common.core.SnowflakeDateTimeFormat;
import net.snowflake.common.util.TimeUtil;

public class ResultUtil {
  static final SFLogger logger = SFLoggerFactory.getLogger(ResultUtil.class);

  public static final int MILLIS_IN_ONE_DAY = 86400000;
  public static final int DEFAULT_SCALE_OF_SFTIME_FRACTION_SECONDS =
      3; // default scale for sftime fraction seconds

  // Construct a default UTC zone for TIMESTAMPNTZ
  private static TimeZone timeZoneUTC = TimeZone.getTimeZone("UTC");

  // Map of default parameter values, used by effectiveParamValue().
  private static final Map<String, Object> defaultParameters;

  static {
    Map<String, Object> map = new HashMap<>();

    // IMPORTANT: This must be consistent with CommonParameterEnum
    map.put("TIMEZONE", "America/Los_Angeles");
    map.put("TIMESTAMP_OUTPUT_FORMAT", "DY, DD MON YYYY HH24:MI:SS TZHTZM");
    map.put("TIMESTAMP_NTZ_OUTPUT_FORMAT", "");
    map.put("TIMESTAMP_LTZ_OUTPUT_FORMAT", "");
    map.put("TIMESTAMP_TZ_OUTPUT_FORMAT", "");
    map.put("DATE_OUTPUT_FORMAT", "YYYY-MM-DD");
    map.put("TIME_OUTPUT_FORMAT", "HH24:MI:SS");
    map.put("CLIENT_HONOR_CLIENT_TZ_FOR_TIMESTAMP_NTZ", Boolean.TRUE);
    map.put("CLIENT_DISABLE_INCIDENTS", Boolean.TRUE);
    map.put("BINARY_OUTPUT_FORMAT", "HEX");
    defaultParameters = map;
  }

  /**
   * Returns the effective parameter value, using the value explicitly provided in parameters, or
   * the default if absent
   *
   * @param parameters keyed in parameter name and valued in parameter value
   * @param paramName Parameter to return the value of
   * @return Effective value
   */
  public static Object effectiveParamValue(Map<String, Object> parameters, String paramName) {
    String upper = paramName.toUpperCase();
    Object value = parameters.get(upper);

    if (value != null) {
      return value;
    }

    value = defaultParameters.get(upper);
    if (value != null) {
      return value;
    }

    logger.debug("Unknown Common Parameter: {}", paramName);
    return null;
  }

  /**
   * Helper function building a formatter for a specialized timestamp type. Note that it will be
   * based on either the 'param' value if set, or the default format provided.
   */
  public static SnowflakeDateTimeFormat specializedFormatter(
      Map<String, Object> parameters, String id, String param, String defaultFormat) {
    String sqlFormat =
        SnowflakeDateTimeFormat.effectiveSpecializedTimestampFormat(
            (String) effectiveParamValue(parameters, param), defaultFormat);
    SnowflakeDateTimeFormat formatter = SnowflakeDateTimeFormat.fromSqlFormat(sqlFormat);
    logger.debug(
        "sql {} format: {}, java {} format: {}",
        id,
        sqlFormat,
        id,
        (ArgSupplier) formatter::toSimpleDateTimePattern);
    return formatter;
  }

  /**
   * Adjust timestamp for dates before 1582-10-05
   *
   * @param timestamp needs to be adjusted
   * @return adjusted timestamp
   */
  public static Timestamp adjustTimestamp(Timestamp timestamp) {
    long milliToAdjust = ResultUtil.msDiffJulianToGregorian(timestamp);

    if (milliToAdjust != 0) {
      logger.debug(
          "adjust timestamp by {} days", (ArgSupplier) () -> milliToAdjust / MILLIS_IN_ONE_DAY);

      Timestamp newTimestamp = new Timestamp(timestamp.getTime() + milliToAdjust);

      newTimestamp.setNanos(timestamp.getNanos());

      return newTimestamp;
    } else {
      return timestamp;
    }
  }

  /**
   * For dates before 1582-10-05, calculate the number of millis to adjust.
   *
   * @param date date before 1582-10-05
   * @return millis needs to be adjusted
   */
  public static long msDiffJulianToGregorian(java.util.Date date) {
    // if date is before 1582-10-05, apply the difference
    // by (H-(H/4)-2) where H is the hundreds digit of the year according to:
    // http://en.wikipedia.org/wiki/Gregorian_calendar
    if (date.getTime() < -12220156800000L) {
      // get the year of the date
      Calendar cal = Calendar.getInstance();
      cal.setTime(date);
      int year = cal.get(Calendar.YEAR);
      int month = cal.get(Calendar.MONTH);
      int dayOfMonth = cal.get(Calendar.DAY_OF_MONTH);

      // for dates on or before 02/28, use the previous year otherwise use
      // current year.
      // TODO: we need to revisit this since there is a potential issue using
      // the year/month/day from the calendar since that may not be the same
      // year/month/day as the original date (which is the problem we are
      // trying to solve here).

      if (month == 0 || (month == 1 && dayOfMonth <= 28)) {
        year = year - 1;
      }

      int hundreds = year / 100;
      int differenceInDays = hundreds - (hundreds / 4) - 2;

      return differenceInDays * MILLIS_IN_ONE_DAY;
    } else {
      return 0;
    }
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
   * @param session session object
   * @return converted snowflake timestamp object
   * @throws SFException if timestampStr is an invalid timestamp
   */
  public static SFTimestamp getSFTimestamp(
      String timestampStr,
      int scale,
      int internalColumnType,
      long resultVersion,
      TimeZone sessionTZ,
      SFBaseSession session)
      throws SFException {
    logger.debug("public Timestamp getTimestamp(int columnIndex)");

    try {
      TimeUtil.TimestampType tsType = null;

      switch (internalColumnType) {
        case Types.TIMESTAMP:
          tsType = TimeUtil.TimestampType.TIMESTAMP_NTZ;
          break;
        case SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_TZ:
          tsType = TimeUtil.TimestampType.TIMESTAMP_TZ;
          logger.trace(
              "Handle timestamp with timezone {} encoding: {}",
              (resultVersion > 0 ? "new" : "old"),
              timestampStr);
          break;
        case SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_LTZ:
          tsType = TimeUtil.TimestampType.TIMESTAMP_LTZ;
          break;
      }

      // Construct a timestamp
      return TimeUtil.getSFTimestamp(timestampStr, scale, tsType, resultVersion, sessionTZ);
    } catch (IllegalArgumentException ex) {
      throw (SFException)
          IncidentUtil.generateIncidentV2WithException(
              session,
              new SFException(ErrorCode.IO_ERROR, "Invalid timestamp value: " + timestampStr),
              null,
              null);
    }
  }

  /**
   * Convert a time internal value (scaled number of seconds + fractional seconds) into an SFTime.
   *
   * <p>Example: getSFTime("123.456", 5) returns an SFTime for 00:02:03.45600.
   *
   * @param obj time object
   * @param scale time scale
   * @param session session object
   * @return snowflake time object
   * @throws SFException if time is invalid
   */
  public static SFTime getSFTime(String obj, int scale, SFBaseSession session) throws SFException {
    try {
      return TimeUtil.getSFTime(obj, scale);
    } catch (IllegalArgumentException ex) {
      throw (SFException)
          IncidentUtil.generateIncidentV2WithException(
              session,
              new SFException(ErrorCode.INTERNAL_ERROR, "Invalid time value: " + obj),
              null,
              null);
    }
  }

  /**
   * Convert a time value into a string
   *
   * @param sft snowflake time object
   * @param scale time scale
   * @param timeFormatter time formatter
   * @return time in string
   */
  public static String getSFTimeAsString(
      SFTime sft, int scale, SnowflakeDateTimeFormat timeFormatter) {
    return timeFormatter.format(sft, scale);
  }

  /**
   * Convert a boolean to a string
   *
   * @param bool boolean
   * @return boolean in string
   */
  public static String getBooleanAsString(boolean bool) {
    return bool ? "TRUE" : "FALSE";
  }

  /**
   * Convert a SFTimestamp to a string value.
   *
   * @param sfTS snowflake timestamp object
   * @param columnType internal snowflake t
   * @param scale timestamp scale
   * @param timestampNTZFormatter snowflake timestamp ntz format
   * @param timestampLTZFormatter snowflake timestamp ltz format
   * @param timestampTZFormatter snowflake timestamp tz format
   * @param session session object
   * @return timestamp in string in desired format
   * @throws SFException timestamp format is missing
   */
  public static String getSFTimestampAsString(
      SFTimestamp sfTS,
      int columnType,
      int scale,
      SnowflakeDateTimeFormat timestampNTZFormatter,
      SnowflakeDateTimeFormat timestampLTZFormatter,
      SnowflakeDateTimeFormat timestampTZFormatter,
      SFBaseSession session)
      throws SFException {
    // Derive the timestamp formatter to use
    SnowflakeDateTimeFormat formatter;
    if (columnType == Types.TIMESTAMP) {
      formatter = timestampNTZFormatter;
    } else if (columnType == SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_LTZ) {
      formatter = timestampLTZFormatter;
    } else // TZ
    {
      formatter = timestampTZFormatter;
    }

    if (formatter == null) {
      throw (SFException)
          IncidentUtil.generateIncidentV2WithException(
              session,
              new SFException(ErrorCode.INTERNAL_ERROR, "missing timestamp formatter"),
              null,
              null);
    }

    try {
      Timestamp adjustedTimestamp = ResultUtil.adjustTimestamp(sfTS.getTimestamp());

      return formatter.format(adjustedTimestamp, sfTS.getTimeZone(), scale);
    } catch (SFTimestamp.TimestampOperationNotAvailableException e) {
      // this timestamp doesn't fit into a Java timestamp, and therefore we
      // can't format it (for now). Just print it out as seconds since epoch.

      BigDecimal nanosSinceEpoch = sfTS.getNanosSinceEpoch();

      BigDecimal secondsSinceEpoch = nanosSinceEpoch.scaleByPowerOfTen(-9);

      return secondsSinceEpoch.setScale(scale).toPlainString();
    }
  }

  /**
   * Convert a date value into a string
   *
   * @param date date will be converted
   * @param dateFormatter date format
   * @return date in string
   */
  public static String getDateAsString(Date date, SnowflakeDateTimeFormat dateFormatter) {
    return dateFormatter.format(date, TimeZone.getDefault());
  }

  /**
   * Adjust date for before 1582-10-05
   *
   * @param date date before 1582-10-05
   * @return adjusted date
   */
  public static Date adjustDate(Date date) {
    long milliToAdjust = ResultUtil.msDiffJulianToGregorian(date);

    if (milliToAdjust != 0) {
      // add the difference to the new date
      return new Date(date.getTime() + milliToAdjust);
    } else {
      return date;
    }
  }

  /**
   * Convert a date internal object to a Date object in specified timezone.
   *
   * @param str snowflake date object
   * @param tz timezone we want convert to
   * @param session snowflake session object
   * @return java date object
   * @throws SFException if date is invalid
   */
  @Deprecated
  public static Date getDate(String str, TimeZone tz, SFBaseSession session) throws SFException {
    try {
      long milliSecsSinceEpoch = Long.valueOf(str) * MILLIS_IN_ONE_DAY;

      SFTimestamp tsInUTC =
          SFTimestamp.fromDate(new Date(milliSecsSinceEpoch), 0, TimeZone.getTimeZone("UTC"));

      SFTimestamp tsInClientTZ = tsInUTC.moveToTimeZone(tz);

      logger.debug(
          "getDate: tz offset={}",
          (ArgSupplier) () -> tsInClientTZ.getTimeZone().getOffset(tsInClientTZ.getTime()));

      // return the date adjusted to the JVM default time zone
      Date preDate = new Date(tsInClientTZ.getTime());

      // if date is on or before 1582-10-04, apply the difference
      // by (H-H/4-2) where H is the hundreds digit of the year according to:
      // http://en.wikipedia.org/wiki/Gregorian_calendar
      Date newDate = adjustDate(preDate);
      logger.debug(
          "Adjust date from {} to {}",
          (ArgSupplier) preDate::toString,
          (ArgSupplier) newDate::toString);
      return newDate;
    } catch (NumberFormatException ex) {
      throw (SFException)
          IncidentUtil.generateIncidentV2WithException(
              session,
              new SFException(ErrorCode.INTERNAL_ERROR, "Invalid date value: " + str),
              null,
              null);
    }
  }

  /**
   * Convert snowflake bool to java boolean
   *
   * @param str boolean type in string representation
   * @return true if the value indicates true otherwise false
   */
  public static boolean getBoolean(String str) {
    return str.equalsIgnoreCase(Boolean.TRUE.toString()) || str.equals("1");
  }

  /**
   * Calculate number of rows updated given a result set Interpret result format based on result
   * set's statement type
   *
   * @param resultSet result set to extract update count from
   * @return the number of rows updated
   * @throws SFException if failed to calculate update count
   * @throws SQLException if failed to calculate update count
   */
  public static long calculateUpdateCount(SFBaseResultSet resultSet)
      throws SFException, SQLException {
    long updateCount = 0;
    SFStatementType statementType = resultSet.getStatementType();
    if (statementType.isDML()) {
      while (resultSet.next()) {
        if (statementType == SFStatementType.COPY) {
          SFResultSetMetaData resultSetMetaData = resultSet.getMetaData();

          int columnIndex = resultSetMetaData.getColumnIndex("rows_loaded");
          updateCount += columnIndex == -1 ? 0 : resultSet.getLong(columnIndex + 1);
        } else if (statementType == SFStatementType.INSERT
            || statementType == SFStatementType.UPDATE
            || statementType == SFStatementType.DELETE
            || statementType == SFStatementType.MERGE
            || statementType == SFStatementType.MULTI_INSERT) {
          int columnCount = resultSet.getMetaData().getColumnCount();
          for (int i = 0; i < columnCount; i++)
            updateCount += resultSet.getLong(i + 1); // add up number of rows updated
        } else {
          updateCount = 0;
        }
      }
    } else {
      updateCount = statementType.isGenerateResultSet() ? -1 : 0;
    }

    return updateCount;
  }

  /**
   * Given a list of String, do a case insensitive search for target string Used by
   * resultsetMetadata to search for target column name
   *
   * @param source source string list
   * @param target target string to match
   * @return index in the source string list that matches the target string index starts from zero
   */
  public static int listSearchCaseInsensitive(List<String> source, String target) {
    for (int i = 0; i < source.size(); i++) {
      if (target.equalsIgnoreCase(source.get(i))) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Return the list of result IDs provided in a result, if available; otherwise return an empty
   * list.
   *
   * @param result result json
   * @return list of result IDs which can be used for result scans
   */
  private static List<String> getResultIds(JsonNode result) {
    JsonNode resultIds = result.path("data").path("resultIds");
    if (resultIds.isNull() || resultIds.isMissingNode() || resultIds.asText().isEmpty()) {
      return Collections.emptyList();
    }
    return new ArrayList<>(Arrays.asList(resultIds.asText().split(",")));
  }

  /**
   * Return the list of result types provided in a result, if available; otherwise return an empty
   * list.
   *
   * @param result result json
   * @return list of result IDs which can be used for result scans
   */
  private static List<SFStatementType> getResultTypes(JsonNode result) {
    JsonNode resultTypes = result.path("data").path("resultTypes");
    if (resultTypes.isNull() || resultTypes.isMissingNode() || resultTypes.asText().isEmpty()) {
      return Collections.emptyList();
    }

    String[] typeStrs = resultTypes.asText().split(",");

    List<SFStatementType> res = new ArrayList<>();
    for (String typeStr : typeStrs) {
      long typeId = Long.valueOf(typeStr);
      res.add(SFStatementType.lookUpTypeById(typeId));
    }
    return res;
  }

  /**
   * Return the list of child results provided in a result, if available; otherwise return an empty
   * list
   *
   * @param session the current session
   * @param requestId the current request id
   * @param result result json
   * @return list of child results
   * @throws SFException if the number of child IDs does not match child statement types
   */
  public static List<SFChildResult> getChildResults(
      SFBaseSession session, String requestId, JsonNode result) throws SFException {
    List<String> ids = getResultIds(result);
    List<SFStatementType> types = getResultTypes(result);

    if (ids.size() != types.size()) {
      throw (SFException)
          IncidentUtil.generateIncidentV2WithException(
              session,
              new SFException(
                  ErrorCode.CHILD_RESULT_IDS_AND_TYPES_DIFFERENT_SIZES, ids.size(), types.size()),
              null,
              requestId);
    }

    List<SFChildResult> res = new ArrayList<>();
    for (int i = 0; i < ids.size(); i++) {
      res.add(new SFChildResult(ids.get(i), types.get(i)));
    }

    return res;
  }
}
