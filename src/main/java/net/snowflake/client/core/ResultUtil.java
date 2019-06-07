/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import com.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeChunkDownloader;
import net.snowflake.client.jdbc.SnowflakeColumnMetadata;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.SnowflakeUtil;
import net.snowflake.client.log.ArgSupplier;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.common.core.SFBinaryFormat;
import net.snowflake.common.core.SFTime;
import net.snowflake.common.core.SFTimestamp;
import net.snowflake.common.core.SnowflakeDateTimeFormat;
import net.snowflake.common.util.TimeUtil;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;

import static net.snowflake.client.core.SessionUtil.CLIENT_ENABLE_CONSERVATIVE_MEMORY_USAGE;
import static net.snowflake.client.core.SessionUtil.CLIENT_MEMORY_LIMIT;
import static net.snowflake.client.core.SessionUtil.CLIENT_PREFETCH_THREADS;
import static net.snowflake.client.core.SessionUtil.CLIENT_RESULT_CHUNK_SIZE;
import static net.snowflake.client.core.SessionUtil.DEFAULT_CLIENT_MEMORY_LIMIT;
import static net.snowflake.client.core.SessionUtil.DEFAULT_CLIENT_PREFETCH_THREADS;

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
    private List<SnowflakeColumnMetadata> resultColumnMetadata = new ArrayList<>();
    private JsonNode currentChunkRowset = null;
    private String rowsetBase64;
    int currentChunkRowCount;
    long resultVersion;
    int numberOfBinds;
    boolean arrayBindSupported;
    ChunkDownloader chunkDownloader;
    SnowflakeDateTimeFormat timestampNTZFormatter;
    SnowflakeDateTimeFormat timestampLTZFormatter;
    SnowflakeDateTimeFormat timestampTZFormatter;
    SnowflakeDateTimeFormat dateFormatter;
    SnowflakeDateTimeFormat timeFormatter;
    TimeZone timeZone;
    boolean honorClientTZForTimestampNTZ;
    SFBinaryFormat binaryFormatter;
    long sendResultTime;
    List<MetaDataOfBinds> metaDataOfBinds = new ArrayList<>();
    QueryResultFormat queryResultFormat;

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

    public ChunkDownloader getChunkDownloader()
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

    public List<MetaDataOfBinds> getMetaDataOfBinds()
    {
      return metaDataOfBinds;
    }

    String getRowsetBase64()
    {
      return rowsetBase64;
    }
  }


  /**
   * A common helper to process result response
   *
   * @param resultData  wrapper object over simple json result
   * @param sfStatement the Snowflake statement
   * @return processed result output
   * @throws SnowflakeSQLException if failed to get number of columns
   */
  static public ResultOutput processResult(ResultInput resultData,
                                           SFStatement sfStatement)
  throws SnowflakeSQLException
  {
    SFSession sfSession = sfStatement.getSession();
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

    logger.debug("query id: {}", resultOutput.queryId);

    Optional<QueryResultFormat> queryResultFormat = QueryResultFormat
        .lookupByName(rootNode.path("data").path("queryResultFormat").asText());
    resultOutput.queryResultFormat =
        queryResultFormat.orElse(QueryResultFormat.JSON);

    // extract parameters
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

      logger.debug("Get column metadata: {}",
                   (ArgSupplier) () -> columnMetadata.toString());
    }

    if (resultOutput.queryResultFormat == QueryResultFormat.ARROW)
    {
      resultOutput.rowsetBase64 =
          rootNode.path("data").path("rowsetBase64").asText();
    }
    else
    {
      resultOutput.currentChunkRowset = rootNode.path("data").path("rowset");

      if (resultOutput.currentChunkRowset == null ||
          resultOutput.currentChunkRowset.isMissingNode())
      {
        resultOutput.currentChunkRowCount = 0;
      }
      else
      {
        resultOutput.currentChunkRowCount =
            resultOutput.currentChunkRowset.size();
      }

      logger.debug("First chunk row count: {}",
                   resultOutput.currentChunkRowCount);
    }

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

        int resultPrefetchThreads = DEFAULT_CLIENT_PREFETCH_THREADS;
        long memoryLimit;
        if (resultOutput.statementType.isSelect()
            && resultOutput.parameters.containsKey(CLIENT_ENABLE_CONSERVATIVE_MEMORY_USAGE)
            && (boolean) resultOutput.parameters.get(CLIENT_ENABLE_CONSERVATIVE_MEMORY_USAGE))
        {
          // use conservative memory settings
          resultPrefetchThreads = sfStatement.getConservativePrefetchThreads();
          memoryLimit = sfStatement.getConservativeMemoryLimit();
          int chunkSize = (int) resultOutput.parameters.get(CLIENT_RESULT_CHUNK_SIZE);
          logger.debug("enable conservative memory usage with prefetchThreads = {} and memoryLimit = {} and " +
                       "resultChunkSize = {}",
                       resultPrefetchThreads, memoryLimit, chunkSize);
        }
        else
        {
          // prefetch threads
          if (resultOutput.parameters.get(CLIENT_PREFETCH_THREADS) != null)
          {
            resultPrefetchThreads =
                (int) resultOutput.parameters.get(CLIENT_PREFETCH_THREADS);
          }
          memoryLimit = initMemoryLimit(resultOutput);
        }

        /*
         * Should JsonParser be used instead of the original Json deserializer.
         */
        boolean useJsonParserV2 = false;
        if (resultOutput.parameters.get("JDBC_USE_JSON_PARSER") != null)
        {
          useJsonParserV2 =
              (boolean) resultOutput.parameters.get("JDBC_USE_JSON_PARSER");
        }
        // initialize the chunk downloader
        resultOutput.chunkDownloader =
            new SnowflakeChunkDownloader(resultOutput.columnCount,
                                         chunksNode,
                                         resultPrefetchThreads,
                                         qrmk,
                                         chunkHeaders,
                                         resultData.networkTimeoutInMilli,
                                         useJsonParserV2,
                                         memoryLimit,
                                         resultOutput.queryResultFormat);
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

    logger.debug("sql date format: {}, java date format: {}",
                 sqlDateFormat,
                 (ArgSupplier) () ->
                     resultOutput.dateFormatter.toSimpleDateTimePattern());

    String sqlTimeFormat = (String) effectiveParamValue(
        resultOutput.parameters,
        "TIME_OUTPUT_FORMAT");

    resultOutput.timeFormatter = new SnowflakeDateTimeFormat(sqlTimeFormat);

    logger.debug("sql time format: {}, java time format: {}",
                 sqlTimeFormat,
                 (ArgSupplier) () ->
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
    {
      resultOutput.resultVersion = versionNode.longValue();
    }

    // number of binds
    JsonNode numberOfBindsNode = rootNode.path("data").path("numberOfBinds");

    if (!numberOfBindsNode.isMissingNode())
    {
      resultOutput.numberOfBinds = numberOfBindsNode.intValue();
    }

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

    // Bind parameter metadata
    JsonNode bindData = rootNode.path("data").path("metaDataOfBinds");
    if (!bindData.isMissingNode())
    {
      List<MetaDataOfBinds> returnVal = new ArrayList<>();
      for (JsonNode child : bindData)
      {
        int precision = child.path("precision").asInt();
        boolean nullable = child.path("nullable").asBoolean();
        int scale = child.path("scale").asInt();
        int byteLength = child.path("byteLength").asInt();
        int length = child.path("length").asInt();
        String name = child.path("name").asText();
        String type = child.path("type").asText();
        MetaDataOfBinds param = new MetaDataOfBinds(precision, nullable, scale, byteLength, length, name, type);
        returnVal.add(param);
      }
      resultOutput.metaDataOfBinds = returnVal;
    }

    return resultOutput;
  }


  /**
   * initialize memory limit in bytes
   *
   * @param resultOutput The output from the request to the server
   * @return memory limit in bytes
   */
  private static long initMemoryLimit(final ResultOutput resultOutput)
  {
    // default setting
    long memoryLimit = DEFAULT_CLIENT_MEMORY_LIMIT * 1024 * 1024;
    if (resultOutput.parameters.get(CLIENT_MEMORY_LIMIT) != null)
    {
      // use the settings from the customer
      memoryLimit =
          (int) resultOutput.parameters.get(CLIENT_MEMORY_LIMIT) * 1024L * 1024L;
    }

    long maxMemoryToUse = Runtime.getRuntime().maxMemory() * 8 / 10;
    if ((int) resultOutput.parameters.get(CLIENT_MEMORY_LIMIT)
        == DEFAULT_CLIENT_MEMORY_LIMIT)
    {
      // if the memory limit is the default value and best effort memory is enabled
      // set the memory limit to 80% of the maximum as the best effort
      memoryLimit = Math.max(memoryLimit, maxMemoryToUse);
    }

    // always make sure memoryLimit <= 80% of the maximum
    memoryLimit = Math.min(memoryLimit, maxMemoryToUse);

    logger.debug("Set allowed memory usage to {} bytes", memoryLimit);
    return memoryLimit;
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
    map.put("CLIENT_HONOR_CLIENT_TZ_FOR_TIMESTAMP_NTZ", Boolean.TRUE);
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
    {
      return value;
    }

    value = defaultParameters.get(upper);
    if (value != null)
    {
      return value;
    }

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
    logger.debug("sql {} format: {}, java {} format: {}",
                 id, sqlFormat,
                 id, (ArgSupplier) formatter::toSimpleDateTimePattern);
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
      logger.debug("adjust timestamp by {} days",
                   (ArgSupplier) () -> milliToAdjust / 86400000);

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
   * @param timestampStr       timestamp object
   * @param scale              timestamp scale
   * @param internalColumnType snowflake timestamp type
   * @param resultVersion      For new result version, timestamp with timezone is formatted as
   *                           the seconds since epoch with fractional part in the decimal followed
   *                           by time zone index. E.g.: "123.456 1440". Here 123.456 is the * number
   *                           of seconds since epoch and 1440 is the timezone index.
   * @param sessionTZ          session timezone
   * @param session            session object
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
    catch (IllegalArgumentException ex)
    {
      throw (SFException) IncidentUtil.generateIncidentV2WithException(
          session,
          new SFException(ErrorCode.IO_ERROR,
                          "Invalid timestamp value: " + timestampStr),
          null,
          null);
    }
  }

  /**
   * Convert a time internal value (scaled number of seconds + fractional
   * seconds) into an SFTime.
   * <p>
   * Example: getSFTime("123.456", 5) returns an SFTime for 00:02:03.45600.
   *
   * @param obj     time object
   * @param scale   time scale
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
    catch (IllegalArgumentException ex)
    {
      throw (SFException) IncidentUtil.generateIncidentV2WithException(
          session,
          new SFException(ErrorCode.INTERNAL_ERROR,
                          "Invalid time value: " + obj),
          null,
          null);
    }
  }

  /**
   * Convert a time value into a string
   *
   * @param sft           snowflake time object
   * @param scale         time scale
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
   * @param sfTS                  snowflake timestamp object
   * @param columnType            internal snowflake t
   * @param scale                 timestamp scale
   * @param timestampNTZFormatter snowflake timestamp ntz format
   * @param timestampLTZFormatter snowflake timestamp ltz format
   * @param timestampTZFormatter  snowflake timestamp tz format
   * @param session               session object
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
      throw (SFException) IncidentUtil.generateIncidentV2WithException(
          session,
          new SFException(ErrorCode.INTERNAL_ERROR,
                          "missing timestamp formatter"),
          null,
          null);
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
   * @param date          date will be converted
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
   * @param str     snowflake date object
   * @param tz      timezone we want convert to
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

      logger.debug("getDate: tz offset={}", (ArgSupplier) () ->
          tsInClientTZ.getTimeZone().getOffset(tsInClientTZ.getTime()));

      // return the date adjusted to the JVM default time zone
      Date preDate = new Date(tsInClientTZ.getTime());

      // if date is on or before 1582-10-04, apply the difference
      // by (H-H/4-2) where H is the hundreds digit of the year according to:
      // http://en.wikipedia.org/wiki/Gregorian_calendar
      Date newDate = adjustDate(preDate);
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
                          "Invalid date value: " + str),
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
   * @throws SFException  if failed to calculate update count
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
                 statementType == SFStatementType.MERGE ||
                 statementType == SFStatementType.MULTI_INSERT)
        {
          int columnCount = resultSet.getMetaData().getColumnCount();
          for (int i = 0; i < columnCount; i++)
            updateCount += resultSet.getLong(i + 1); // add up number of rows updated
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
   *
   * @param source source string list
   * @param target target string to match
   * @return index in the source string list that matches the target string
   * index starts from zero
   */
  public static int listSearchCaseInsensitive(List<String> source, String target)
  {
    for (int i = 0; i < source.size(); i++)
    {
      if (target.equalsIgnoreCase(source.get(i)))
      {
        return i;
      }
    }
    return -1;
  }

  /**
   * Return the list of result IDs provided in a result, if available; otherwise
   * return an empty list.
   *
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
   *
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
   *
   * @param session   the current session
   * @param requestId the current request id
   * @param result    result json
   * @return list of child results
   * @throws SFException if the number of child IDs does not match
   *                     child statement types
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
      throw (SFException) IncidentUtil.generateIncidentV2WithException(
          session,
          new SFException(ErrorCode.CHILD_RESULT_IDS_AND_TYPES_DIFFERENT_SIZES,
                          ids.size(),
                          types.size()),
          null,
          requestId);
    }

    List<SFChildResult> res = new ArrayList<>();
    for (int i = 0; i < ids.size(); i++)
    {
      res.add(new SFChildResult(ids.get(i), types.get(i)));
    }

    return res;
  }
}
