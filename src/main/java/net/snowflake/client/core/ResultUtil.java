/*
 * Copyright (c) 2012-2018 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import com.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeChunkDownloader;
import net.snowflake.client.jdbc.SnowflakeColumnMetadata;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.SnowflakeUtil;
import net.snowflake.common.core.SFBinaryFormat;
import net.snowflake.common.core.SFTime;
import net.snowflake.common.core.SFTimestamp;
import net.snowflake.common.core.SnowflakeDateTimeFormat;
import net.snowflake.common.util.TimeUtil;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.sql.Types;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

/**
 * Created by jhuang on 2/1/16.
 */
public class ResultUtil
{
  static final SFLogger logger = SFLoggerFactory.getLogger(ResultUtil.class);

  // Construct a default UTC zone for TIMESTAMPNTZ
  private static TimeZone timeZoneUTC = TimeZone.getTimeZone("UTC");

  static public class ResultInput
  {
    JsonNode resultJSON;

    // These fields are used for building a result downloader which
    // uses http client
    int connectionTimeout;
    int socketTimeout;
    int networkTimeoutInMilli;

    public ResultInput setResultJSON(JsonNode resultJSON)
    {
      this.resultJSON = resultJSON;
      return this;
    }

    public ResultInput setConnectionTimeout(int connectionTimeout)
    {
      this.connectionTimeout = connectionTimeout;
      return this;
    }

    public ResultInput setSocketTimeout(int socketTimeout)
    {
      this.socketTimeout = socketTimeout;
      return this;
    }

    public ResultInput setNetworkTimeoutInMilli(int networkTimeoutInMilli)
    {
      this.networkTimeoutInMilli = networkTimeoutInMilli;
      return this;
    }
  }

  static public class ResultOutput
  {
    long chunkCount;
    String queryId;
    String finalDatabaseName;
    String finalSchemaName;
    String finalRoleName;
    String finalWarehouseName;
    SFStatementType statementType;
    boolean totalRowCountTruncated;
    Map<String, Object> parameters = new HashMap<>();
    int columnCount;
    private List<SnowflakeColumnMetadata> resultColumnMetadata =
        new ArrayList<SnowflakeColumnMetadata>();
    private JsonNode currentChunkRowset = null;
    int currentChunkRowCount;
    long resultVersion;
    int numberOfBinds;
    boolean arrayBindSupported;
    SnowflakeChunkDownloader chunkDownloader;
    SnowflakeDateTimeFormat timestampNTZFormatter;
    SnowflakeDateTimeFormat timestampLTZFormatter;
    SnowflakeDateTimeFormat timestampTZFormatter;
    SnowflakeDateTimeFormat dateFormatter;
    SnowflakeDateTimeFormat timeFormatter;
    TimeZone timeZone;
    boolean honorClientTZForTimestampNTZ;
    SFBinaryFormat binaryFormatter;
    long sendResultTime;

    public long getChunkCount()
    {
      return chunkCount;
    }

    public boolean isArrayBindSupported()
    {
      return arrayBindSupported;
    }

    public String getQueryId()
    {
      return queryId;
    }

    public String getFinalDatabaseName()
    {
      return finalDatabaseName;
    }

    public String getFinalSchemaName()
    {
      return finalSchemaName;
    }

    String getFinalRoleName()
    {
      return finalRoleName;
    }

    String getFinalWarehouseName()
    {
      return finalWarehouseName;
    }

    public SFStatementType getStatementType()
    {
      return statementType;
    }

    public boolean isTotalRowCountTruncated()
    {
      return totalRowCountTruncated;
    }

    public Map<String, Object> getParameters()
    {
      return parameters;
    }

    public int getColumnCount()
    {
      return columnCount;
    }

    public List<SnowflakeColumnMetadata> getResultColumnMetadata()
    {
      return resultColumnMetadata;
    }

    public JsonNode getAndClearCurrentChunkRowset()
    {
      JsonNode currentChunkRowset = this.currentChunkRowset;
      this.currentChunkRowset = null;
      return currentChunkRowset;
    }

    public int getCurrentChunkRowCount()
    {
      return currentChunkRowCount;
    }

    public long getResultVersion()
    {
      return resultVersion;
    }

    public int getNumberOfBinds()
    {
      return numberOfBinds;
    }

    public SnowflakeChunkDownloader getChunkDownloader()
    {
      return chunkDownloader;
    }

    public SnowflakeDateTimeFormat getTimestampNTZFormatter()
    {
      return timestampNTZFormatter;
    }

    public SnowflakeDateTimeFormat getTimestampLTZFormatter()
    {
      return timestampLTZFormatter;
    }

    public SnowflakeDateTimeFormat getTimestampTZFormatter()
    {
      return timestampTZFormatter;
    }

    public SnowflakeDateTimeFormat getDateFormatter()
    {
      return dateFormatter;
    }

    public SnowflakeDateTimeFormat getTimeFormatter()
    {
      return timeFormatter;
    }

    public TimeZone getTimeZone()
    {
      return timeZone;
    }

    public boolean isHonorClientTZForTimestampNTZ()
    {
      return honorClientTZForTimestampNTZ;
    }

    public SFBinaryFormat getBinaryFormatter()
    {
      return binaryFormatter;
    }

    public long getSendResultTime()
    {
      return sendResultTime;
    }
  }

  /**
   * A common helper to process result response
   *
   * @param resultData wrapper object over simple json result
   * @param sfSession the Snowflake session
   * @return processed result output
   * @throws SnowflakeSQLException if failed to get number of columns
   */
  static public ResultOutput processResult(ResultInput resultData,
                                           SFSession sfSession)
      throws SnowflakeSQLException
  {
    ResultOutput resultOutput = new ResultOutput();

    logger.debug("Entering processResult");
    JsonNode rootNode = resultData.resultJSON;

    SnowflakeUtil.checkErrorAndThrowException(rootNode);

    // get the query id
    resultOutput.queryId = rootNode.path("data").path("queryId").asText();

    JsonNode databaseNode = rootNode.path("data").path("finalDatabaseName");
    resultOutput.finalDatabaseName = databaseNode.isNull() ? null : databaseNode.asText();

    JsonNode schemaNode = rootNode.path("data").path("finalSchemaName");
    resultOutput.finalSchemaName = schemaNode.isNull() ? null : schemaNode.asText();

    JsonNode roleNode = rootNode.path("data").path("finalRoleName");
    resultOutput.finalRoleName = roleNode.isNull() ? null : roleNode.asText();

    JsonNode warehouseNode = rootNode.path("data").path("finalWarehouseName");
    resultOutput.finalWarehouseName = warehouseNode.isNull() ? null : warehouseNode.asText();

    resultOutput.statementType = SFStatementType.lookUpTypeById(
        rootNode.path("data").path("statementTypeId").asLong());

    resultOutput.totalRowCountTruncated
        = rootNode.path("data").path("totalTruncated").asBoolean();

    if (logger.isDebugEnabled())
      logger.debug("query id: {}", resultOutput.queryId);

    // extrace parameters
    resultOutput.parameters =
        SessionUtil.getCommonParams(rootNode.path("data").path("parameters"));

    // initialize column metadata
    resultOutput.columnCount = rootNode.path("data").path("rowtype").size();

    for (int i = 0; i < resultOutput.columnCount; i++)
    {
      JsonNode colNode = rootNode.path("data").path("rowtype").path(i);

      SnowflakeColumnMetadata columnMetadata
          = SnowflakeUtil.extractColumnMetadata(
              colNode, sfSession.isJdbcTreatDecimalAsInt());

      resultOutput.resultColumnMetadata.add(columnMetadata);

      if (logger.isDebugEnabled())
        logger.debug("Get column metadata: {}",
             columnMetadata.toString());
    }

    resultOutput.currentChunkRowset = rootNode.path("data").path("rowset");

    if (resultOutput.currentChunkRowset == null ||
        resultOutput.currentChunkRowset.isMissingNode())
    {
      resultOutput.currentChunkRowCount = 0;
    }
    else
    {
      resultOutput.currentChunkRowCount = resultOutput.currentChunkRowset.size();
    }

    logger.debug("First chunk row count: {}",
        resultOutput.currentChunkRowCount);

    JsonNode chunksNode = rootNode.path("data").path("chunks");

    if (!chunksNode.isMissingNode())
    {
      resultOutput.chunkCount = chunksNode.size();

      // Try to get the Query Result Master Key
      JsonNode qrmkNode = rootNode.path("data").path("qrmk");
      final String qrmk = qrmkNode.isMissingNode() ?
          null : qrmkNode.textValue();

      JsonNode chunkHeaders = rootNode.path("data").path("chunkHeaders");

      // the initialization of chunk downloader will start prefetching
      // first few chunks
      if (resultOutput.chunkCount > 0)
      {
        logger.debug("#chunks={}, initialize chunk downloader",
            resultOutput.chunkCount);

        // prefetch threads
        int resultPrefetchThreads = 4;
        if (resultOutput.parameters.get("CLIENT_PREFETCH_THREADS")
            != null)
        {
          resultPrefetchThreads =
              (int) resultOutput.parameters.get(
                  "CLIENT_PREFETCH_THREADS");
        }

        /*
         * Should JsonParser be used instead of the original Json deserializer.
         */
        boolean useJsonParser = true;
        if (resultOutput.parameters.get("JDBC_USE_JSON_PARSER") != null)
        {
          useJsonParser =
              (boolean) resultOutput.parameters.get("JDBC_USE_JSON_PARSER");
        }

        int memoryUsage = 1536;
        if (resultOutput.parameters.get("CLIENT_MEMORY_LIMIT") != null)
        {
          memoryUsage =
              (int) resultOutput.parameters.get("CLIENT_MEMORY_LIMIT");
        }

        boolean efficientChunkStorage = false;
        if (resultOutput.parameters.get("JDBC_EFFICIENT_CHUNK_STORAGE") != null)
        {
          efficientChunkStorage = (boolean)
              resultOutput.parameters.get("JDBC_EFFICIENT_CHUNK_STORAGE");
        }

        if (useJsonParser && efficientChunkStorage)
        {
          resultPrefetchThreads =
              Math.max(Math.min(resultPrefetchThreads,
                                Runtime.getRuntime().availableProcessors() / 2),
                       2);
        }
        else
        {
          resultPrefetchThreads = 1;
        }
        // initialize the chunk downloader
        resultOutput.chunkDownloader =
            new SnowflakeChunkDownloader(resultOutput.columnCount,
                                         chunksNode,
                                         resultPrefetchThreads,
                                         qrmk,
                                         chunkHeaders,
                                         resultData.networkTimeoutInMilli,
                                         useJsonParser,
                                         memoryUsage * 1024 * 1024,
                                         efficientChunkStorage);
      }
    }

    String sqlTimestampFormat = (String) effectiveParamValue(
        resultOutput.parameters, "TIMESTAMP_OUTPUT_FORMAT");

    // Special handling of specialized formatters, use a helper function
    resultOutput.timestampNTZFormatter = specializedFormatter(
        resultOutput.parameters,
        "timestamp_ntz",
        "TIMESTAMP_NTZ_OUTPUT_FORMAT",
        sqlTimestampFormat);

    resultOutput.timestampLTZFormatter = specializedFormatter(
        resultOutput.parameters,
        "timestamp_ltz",
        "TIMESTAMP_LTZ_OUTPUT_FORMAT",
        sqlTimestampFormat);

    resultOutput.timestampTZFormatter = specializedFormatter(
        resultOutput.parameters,
        "timestamp_tz",
        "TIMESTAMP_TZ_OUTPUT_FORMAT",
        sqlTimestampFormat);

    String sqlDateFormat = (String) effectiveParamValue(
        resultOutput.parameters,
        "DATE_OUTPUT_FORMAT");

    resultOutput.dateFormatter = new SnowflakeDateTimeFormat(sqlDateFormat);

    if (logger.isDebugEnabled())
      logger.debug("sql date format: {}, java date format: {}",
          sqlDateFormat,
          resultOutput.dateFormatter.toSimpleDateTimePattern());

    String sqlTimeFormat = (String) effectiveParamValue(
        resultOutput.parameters,
        "TIME_OUTPUT_FORMAT");

    resultOutput.timeFormatter = new SnowflakeDateTimeFormat(sqlTimeFormat);
    if (logger.isDebugEnabled())
      logger.debug("sql time format: {}, java time format: {}",
          sqlTimeFormat,
          resultOutput.timeFormatter.toSimpleDateTimePattern());

    String timeZoneName = (String) effectiveParamValue(
        resultOutput.parameters, "TIMEZONE");
    resultOutput.timeZone = TimeZone.getTimeZone(timeZoneName);

    resultOutput.honorClientTZForTimestampNTZ =
        (boolean) effectiveParamValue(
            resultOutput.parameters,
            "CLIENT_HONOR_CLIENT_TZ_FOR_TIMESTAMP_NTZ");

    logger.debug("Honoring client TZ for timestamp_ntz? {}",
        resultOutput.honorClientTZForTimestampNTZ);

    String binaryFmt = (String) effectiveParamValue(
        resultOutput.parameters, "BINARY_OUTPUT_FORMAT");
    resultOutput.binaryFormatter =
        SFBinaryFormat.getSafeOutputFormat(binaryFmt);

    // result version
    JsonNode versionNode = rootNode.path("data").path("version");

    if (!versionNode.isMissingNode())
      resultOutput.resultVersion = versionNode.longValue();

    // number of binds
    JsonNode numberOfBindsNode = rootNode.path("data").path("numberOfBinds");

    if (!numberOfBindsNode.isMissingNode())
      resultOutput.numberOfBinds = numberOfBindsNode.intValue();

    JsonNode arrayBindSupported = rootNode.path("data")
              .path("arrayBindSupported");
    resultOutput.arrayBindSupported = !arrayBindSupported.isMissingNode()
                                      && arrayBindSupported.asBoolean();

    // time result sent by GS (epoch time in millis)
    JsonNode sendResultTimeNode = rootNode.path("data").path("sendResultTime");
    if (!sendResultTimeNode.isMissingNode())
    {
      resultOutput.sendResultTime = sendResultTimeNode.longValue();
    }

    logger.debug("result version={}", resultOutput.resultVersion);

    return resultOutput;
  }

  // Map of default parameter values, used by effectiveParamValue().
  private static final Map<String, Object> defaultParameters;
  static
  {
    Map<String, Object> map = new HashMap<>();

    // IMPORTANT: This must be consistent with CommonParameterEnum
    map.put("TIMEZONE", "America/Los_Angeles");
    map.put("TIMESTAMP_OUTPUT_FORMAT", "DY, DD MON YYYY HH24:MI:SS TZHTZM");
    map.put("TIMESTAMP_NTZ_OUTPUT_FORMAT", "");
    map.put("TIMESTAMP_LTZ_OUTPUT_FORMAT", "");
    map.put("TIMESTAMP_TZ_OUTPUT_FORMAT", "");
    map.put("DATE_OUTPUT_FORMAT", "YYYY-MM-DD");
    map.put("TIME_OUTPUT_FORMAT", "HH24:MI:SS");
    map.put("CLIENT_RESULT_PREFETCH_SLOTS", 2);
    map.put("CLIENT_RESULT_PREFETCH_THREADS", 1);
    map.put("CLIENT_HONOR_CLIENT_TZ_FOR_TIMESTAMP_NTZ", Boolean.TRUE);
    map.put("JDBC_EXECUTE_RETURN_COUNT_FOR_DML", Boolean.FALSE);
    map.put("CLIENT_DISABLE_INCIDENTS", Boolean.TRUE);
    map.put("BINARY_OUTPUT_FORMAT", "HEX");

    defaultParameters = map;
  }

  /**
   * Returns the effective parameter value, using the value explicitly
   * provided in parameters, or the default if absent
   *
   * @param parameters keyed in parameter name and valued in parameter value
   * @param paramName  Parameter to return the value of
   * @return Effective value
   */
  static private Object effectiveParamValue(
      Map<String, Object> parameters,
      String paramName)
  {
    String upper = paramName.toUpperCase();
    Object value = parameters.get(upper);

    if (value != null)
      return value;

    value = defaultParameters.get(upper);
    if (value != null)
      return value;

    logger.debug("Unknown Common Parameter: {}", paramName);
    return null;
  }

  /**
   * Helper function building a formatter for a specialized timestamp type.
   * Note that it will be based on either the 'param' value if set,
   * or the default format provided.
   */
  static private SnowflakeDateTimeFormat specializedFormatter(
      Map<String, Object> parameters,
      String id,
      String param,
      String defaultFormat)
  {
    String sqlFormat =
        SnowflakeDateTimeFormat.effectiveSpecializedTimestampFormat(
            (String) effectiveParamValue(parameters, param),
            defaultFormat);
    SnowflakeDateTimeFormat formatter = new SnowflakeDateTimeFormat(sqlFormat);
    if (logger.isDebugEnabled())
      logger.debug("sql {} format: {}, java {} format: {}",
          id, sqlFormat,
          id, formatter.toSimpleDateTimePattern());
    return formatter;
  }

  /**
   * Adjust timestamp for dates before 1582-10-05
   *
   * @param timestamp needs to be adjusted
   * @return adjusted timestamp
   */
  static public Timestamp adjustTimestamp(Timestamp timestamp)
  {
    long milliToAdjust = ResultUtil.msDiffJulianToGregorian(timestamp);

    if (milliToAdjust != 0)
    {

      if (logger.isDebugEnabled())
        logger.debug("adjust timestamp by {} days", milliToAdjust / 86400000);

      Timestamp newTimestamp = new Timestamp(timestamp.getTime()
           + milliToAdjust);

      newTimestamp.setNanos(timestamp.getNanos());

      return newTimestamp;
    }
	  else
    {
      return timestamp;
    }
  }

  /**
   * For dates before 1582-10-05, calculate the number of millis to adjust.
   *
   * @param date date before 1582-10-05
   * @return millis needs to be adjusted
   */
  static public long msDiffJulianToGregorian(java.util.Date date)
  {
    // get the year of the date
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    int year = cal.get(Calendar.YEAR);
    int month = cal.get(Calendar.MONTH);
    int dayOfMonth = cal.get(Calendar.DAY_OF_MONTH);

    // if date is before 1582-10-05, apply the difference
    // by (H-(H/4)-2) where H is the hundreds digit of the year according to:
    // http://en.wikipedia.org/wiki/Gregorian_calendar
    if (date.getTime() < -12220156800000L)
    {
      // for dates on or before 02/28, use the previous year otherwise use
      // current year.
      // TODO: we need to revisit this since there is a potential issue using
      // the year/month/day from the calendar since that may not be the same
      // year/month/day as the original date (which is the problem we are
      // trying to solve here).

      if (month == 0 || (month == 1 && dayOfMonth <= 28))
      {
        year = year - 1;
      }

      int hundreds = year / 100;
      int differenceInDays = hundreds - (hundreds / 4) - 2;

      return differenceInDays * 86400000;
    }
    else
    {
      return 0;
    }
  }

  /**
   * Convert a timestamp internal value (scaled number of seconds + fractional
   * seconds) into a SFTimestamp.
   *
   * @param timestampStr timestamp object
   * @param scale timestamp scale
   * @param internalColumnType snowflake timestamp type
   * @param resultVersion For new result version, timestamp with timezone is formatted as
   *                      the seconds since epoch with fractional part in the decimal followed
   *                      by time zone index. E.g.: "123.456 1440". Here 123.456 is the * number
   *                      of seconds since epoch and 1440 is the timezone index.
   * @param sessionTZ session timezone
   * @param session session object
   * @return converted snowflake timestamp object
   * @throws SFException if timestampStr is an invalid timestamp
   */
  static public SFTimestamp getSFTimestamp(String timestampStr, int scale,
                                           int internalColumnType,
                                           long resultVersion,
                                           TimeZone sessionTZ,
                                           SFSession session)
      throws SFException
  {
    logger.debug(
        "public Timestamp getTimestamp(int columnIndex)");

    try
    {
      TimeUtil.TimestampType tsType = null;

      switch (internalColumnType)
      {
        case Types.TIMESTAMP:
          tsType = TimeUtil.TimestampType.TIMESTAMP_NTZ;
          break;
        case SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_TZ:
          tsType = TimeUtil.TimestampType.TIMESTAMP_TZ;
          logger.trace(
              "Handle timestamp with timezone {} encoding: {}",
              (resultVersion > 0 ? "new" : "old"), timestampStr);
          break;
        case SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_LTZ:
          tsType = TimeUtil.TimestampType.TIMESTAMP_LTZ;
          break;
      }

      // Construct a timestamp
      return TimeUtil.getSFTimestamp(timestampStr, scale, 
                                     tsType, resultVersion, sessionTZ);
    }
    catch(IllegalArgumentException ex)
    {
      throw IncidentUtil.
          generateIncidentWithSignatureAndException(
                  session, null, null,
                  "Invalid timestamp value",
                  ErrorCode.IO_ERROR,
                  "Invalid timestamp value: " + timestampStr);
    }
  }

  /**
   * Convert a time internal value (scaled number of seconds + fractional
   * seconds) into an SFTime.
   *
   * Example: getSFTime("123.456", 5) returns an SFTime for 00:02:03.45600.
   *
   * @param obj time object
   * @param scale time scale
   * @param session session object
   * @return snowflake time object
   * @throws SFException if time is invalid
   */
  static public SFTime getSFTime(String obj, int scale, SFSession session)
      throws SFException
  {
    try
    {
        return TimeUtil.getSFTime(obj, scale);
    }
    catch(IllegalArgumentException ex)
    {
      throw IncidentUtil.
          generateIncidentWithSignatureAndException(
                  session, null, null,
                  "Invalid time value",
                  ErrorCode.INTERNAL_ERROR,
                  "Invalid time value: " + obj);
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
  static public String getSFTimeAsString(
      SFTime sft, int scale, SnowflakeDateTimeFormat timeFormatter)
  {
    return timeFormatter.format(sft, scale);
  }

  /**
   * Convert a boolean to a string
   *
   * @param bool boolean
   * @return boolean in string
   */
  static public String getBooleanAsString(boolean bool)
  {
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
  static public String getSFTimestampAsString(
      SFTimestamp sfTS, int columnType, int scale,
      SnowflakeDateTimeFormat timestampNTZFormatter,
      SnowflakeDateTimeFormat timestampLTZFormatter,
      SnowflakeDateTimeFormat timestampTZFormatter,
      SFSession session) throws SFException
  {
    // Derive the timestamp formatter to use
    SnowflakeDateTimeFormat formatter;
    if (columnType == Types.TIMESTAMP)
    {
      formatter = timestampNTZFormatter;
    }
    else if (columnType == SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_LTZ)
    {
      formatter = timestampLTZFormatter;
    }
    else // TZ
    {
      formatter = timestampTZFormatter;
    }

    if (formatter == null)
    {
      throw IncidentUtil.
          generateIncidentWithException(session, null, null,
                                        ErrorCode.INTERNAL_ERROR,
                                        "missing timestamp formatter");
    }

    try
    {
      Timestamp adjustedTimestamp =
          ResultUtil.adjustTimestamp(sfTS.getTimestamp());

      return formatter.format(
          adjustedTimestamp, sfTS.getTimeZone(), scale);
    }
    catch (SFTimestamp.TimestampOperationNotAvailableException e)
    {
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
  static public String getDateAsString(
      Date date, SnowflakeDateTimeFormat dateFormatter)
  {
    return dateFormatter.format(date, timeZoneUTC);
  }

  /**
   * Adjust date for before 1582-10-05
   *
   * @param date date before 1582-10-05
   * @return adjusted date
   */
   static public Date adjustDate(Date date)
   {
     long milliToAdjust = ResultUtil.msDiffJulianToGregorian(date);

     if (milliToAdjust != 0)
     {
       // add the difference to the new date
       return new Date(date.getTime() + milliToAdjust);
     }
     else
     {
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
  static public Date getDate(String str, TimeZone tz, SFSession session) throws SFException
  {
    try
    {
      long milliSecsSinceEpoch = Long.valueOf(str) * 86400000;

      SFTimestamp tsInUTC = SFTimestamp.fromDate(new Date(milliSecsSinceEpoch),
          0, TimeZone.getTimeZone("UTC"));

      SFTimestamp tsInClientTZ = tsInUTC.moveToTimeZone(tz);

      if (logger.isDebugEnabled())
      {
        logger.debug("getDate: tz offset={}",
            tsInClientTZ.getTimeZone().getOffset(tsInClientTZ.getTime()));
      }
      // return the date adjusted to the JVM default time zone
      Date preDate = new Date(tsInClientTZ.getTime());

      // if date is on or before 1582-10-04, apply the difference
      // by (H-H/4-2) where H is the hundreds digit of the year according to:
      // http://en.wikipedia.org/wiki/Gregorian_calendar
      Date newDate = adjustDate(preDate);
      if (logger.isDebugEnabled())
      {
        logger.debug("Adjust date from {} to {}",
            preDate.toString(), newDate.toString());
      }
      return newDate;
    }
    catch (NumberFormatException ex)
    {
      SFException sfe = new SFException(ErrorCode.INTERNAL_ERROR,
              "Invalid date value: " + str);

      IncidentUtil.generateIncident(session, "Invalid date value",
                                    null, null, null, sfe);
      throw sfe;
    }
  }

  /**
   * Convert snowflake bool to java boolean
   *
   * @param str boolean type in string representation
   * @return true if the value indicates true otherwise false
   */
  public static boolean getBoolean(String str)
  {
    return str.equalsIgnoreCase("true") ||
        str.equals("1");
  }

  /**
   * Calculate number of rows updated given a result set
   * Interpret result format based on result set's statement type
   *
   * @param resultSet result set to extract update count from
   * @return the number of rows updated
   * @throws SFException if failed to calculate update count
   * @throws SQLException if failed to calculate update count
   */
  static public int calculateUpdateCount(SFBaseResultSet resultSet)
                                         throws SFException, SQLException
  {
    int updateCount = 0;
    SFStatementType statementType = resultSet.getStatementType();
    if (statementType.isDML())
    {
      while (resultSet.next())
      {
        if (statementType == SFStatementType.COPY)
        {
          SFResultSetMetaData resultSetMetaData = resultSet.getMetaData();

          int columnIndex = resultSetMetaData.getColumnIndex("rows_loaded");
          updateCount += columnIndex == -1 ? 0 : resultSet.getInt(columnIndex + 1);
        }
        else if (statementType == SFStatementType.INSERT ||
                 statementType == SFStatementType.UPDATE ||
                 statementType == SFStatementType.DELETE ||
                 statementType == SFStatementType.MERGE  ||
                 statementType == SFStatementType.MULTI_INSERT)
        {
          int columnCount = resultSet.getMetaData().getColumnCount();
          for(int i=0; i<columnCount; i++)
            updateCount += resultSet.getLong(i+1); // add up number of rows updated
        }
        else
        {
          updateCount = 0;
        }
      }
    }
    else
    {
      updateCount = statementType.isGenerateResultSet() ? -1 : 0;
    }

    return updateCount;
  }

  /**
   * Given a list of String, do a case insensitive search for target string
   * Used by resultsetMetadata to search for target column name
   * @param source source string list
   * @param target target string to match
   * @return index in the source string list that matches the target string
   *         index starts from zero
   */
  public static int listSearchCaseInsensitive(List<String> source, String target)
  {
    for (int i=0; i<source.size(); i++)
    {
      if (target.equalsIgnoreCase(source.get(i)))
        return i;
    }
    return -1;
  }

  /**
   * Return the list of result IDs provided in a result, if available; otherwise
   * return an empty list.
   * @param result result json
   * @return list of result IDs which can be used for result scans
   */
  private static List<String> getResultIds(JsonNode result)
  {
    JsonNode resultIds = result.path("data").path("resultIds");
    if (resultIds.isNull() ||
        resultIds.isMissingNode() ||
        resultIds.asText().isEmpty())
    {
      return Collections.emptyList();
    }
    return new ArrayList<>(Arrays.asList(resultIds.asText().split(",")));
  }

  /**
   * Return the list of result types provided in a result, if available; otherwise
   * return an empty list.
   * @param result result json
   * @return list of result IDs which can be used for result scans
   */
  private static List<SFStatementType> getResultTypes(JsonNode result)
  {
    JsonNode resultTypes = result.path("data").path("resultTypes");
    if (resultTypes.isNull() ||
        resultTypes.isMissingNode() ||
        resultTypes.asText().isEmpty())
    {
      return Collections.emptyList();
    }

    String[] typeStrs = resultTypes.asText().split(",");

    List<SFStatementType> res = new ArrayList<>();
    for (String typeStr : typeStrs)
    {
      long typeId = Long.valueOf(typeStr);
      res.add(SFStatementType.lookUpTypeById(typeId));
    }
    return res;
  }

  /**
   * Return the list of child results provided in a result, if available; otherwise
   * return an empty list
   * @param session the current session
   * @param requestId the current request id
   * @param result result json
   * @return list of child results
   * @throws SFException if the number of child IDs does not match
   *   child statement types
   */
  public static List<SFChildResult> getChildResults(SFSession session,
                                                    String requestId,
                                                    JsonNode result)
  throws SFException
  {
    List<String> ids = getResultIds(result);
    List<SFStatementType> types = getResultTypes(result);

    if (ids.size() != types.size())
    {
      throw IncidentUtil.
          generateIncidentWithException(
              session, requestId, null,
              ErrorCode.CHILD_RESULT_IDS_AND_TYPES_DIFFERENT_SIZES,
              ids.size(),
              types.size());
    }

    List<SFChildResult> res = new ArrayList<>();
    for (int i = 0; i < ids.size(); i++)
    {
      res.add(new SFChildResult(ids.get(i), types.get(i)));
    }

    return res;
  }
}
