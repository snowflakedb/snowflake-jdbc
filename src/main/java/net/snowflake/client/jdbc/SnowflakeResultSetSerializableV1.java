/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import com.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.core.ChunkDownloader;
import net.snowflake.client.core.MetaDataOfBinds;
import net.snowflake.client.core.OCSPMode;
import net.snowflake.client.core.QueryResultFormat;
import net.snowflake.client.core.SFSession;
import net.snowflake.client.core.SFStatementType;
import net.snowflake.client.core.SessionUtil;
import net.snowflake.client.log.ArgSupplier;
import net.snowflake.common.core.SFBinaryFormat;

import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;

import net.snowflake.client.core.ResultUtil;
import net.snowflake.client.core.SFStatement;
import net.snowflake.common.core.SnowflakeDateTimeFormat;
import org.apache.arrow.memory.RootAllocator;

import static net.snowflake.client.core.SessionUtil.CLIENT_ENABLE_CONSERVATIVE_MEMORY_USAGE;
import static net.snowflake.client.core.SessionUtil.CLIENT_MEMORY_LIMIT;
import static net.snowflake.client.core.SessionUtil.CLIENT_PREFETCH_THREADS;
import static net.snowflake.client.core.SessionUtil.CLIENT_RESULT_CHUNK_SIZE;
import static net.snowflake.client.core.SessionUtil.DEFAULT_CLIENT_MEMORY_LIMIT;
import static net.snowflake.client.core.SessionUtil.DEFAULT_CLIENT_PREFETCH_THREADS;


/**
 * This object is an intermediate object between result JSON from GS and
 * ResultSet. Originally, it is created from result JSON. And it can
 * also be serializable. Logically, it stands for a part of ResultSet.
 * <p>
 * A typical result JSON data section is consist of the content of first chunk
 * file and file metadata for the rest of chunk files e.g. URL, chunk size, etc.
 * So this object consist of one chunk data and a list of chunk file entries.
 * <p>
 * This object is serializable, so it can be distributed to other threads or
 * workder nodes for distributed processing.
 * <p>
 * Created by mrui on Aug 20 2019
 */
public class SnowflakeResultSetSerializableV1 implements SnowflakeResultSetSerializable
{
  static final SFLogger logger = SFLoggerFactory.getLogger(SnowflakeResultSetSerializableV1.class);

  /**
   * An Entity class to represent a chunk file metadata.
   */
  static public class ChunkFileEntry
  {
    String fileURL;
    int rowCount;
    int compressedByteSize;
    int uncompressedByteSize;

    public ChunkFileEntry(String fileURL,
                          int rowCount,
                          int compressedByteSize,
                          int uncompressedByteSize)
    {
      this.fileURL = fileURL;
      this.rowCount = rowCount;
      this.compressedByteSize = compressedByteSize;
      this.uncompressedByteSize = uncompressedByteSize;
    }

    public String getFileURL()
    {
      return fileURL;
    }

    public int getRowCount()
    {
      return rowCount;
    }

    public int getCompressedByteSize()
    {
      return compressedByteSize;
    }

    public int getUncompressedByteSize()
    {
      return uncompressedByteSize;
    }
  }

  // Below fields are for the data fields that this object wraps
  String firstChunkStringData;
  int firstChunkRowCount;
  int chunkFileCount;
  List<ChunkFileEntry> chunkFileEntries = new ArrayList<>();

  // below fields are used for building a ChunkDownloader which
  // uses http client to download chunk files
  boolean useJsonParserV2;
  int resultPrefetchThreads;
  long memoryLimit;
  String qrmk;
  Map<String, String> chunkHeadersMap = new HashMap<>();
  // Below fields are from session or statement, they are required to
  // ChunkDownloader
  SnowflakeConnectString snowflakeConnectionString;
  OCSPMode ocspMode;
  int networkTimeoutInMilli;

  // Below are some metadata fields parsed from the result JSON
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
      new ArrayList<>();
  long resultVersion;
  int numberOfBinds;
  boolean arrayBindSupported;
  long sendResultTime;
  List<MetaDataOfBinds> metaDataOfBinds = new ArrayList<>();
  QueryResultFormat queryResultFormat;

  // Below fields are transient, they are generated from parameters
  transient TimeZone timeZone;
  transient boolean honorClientTZForTimestampNTZ;
  transient SnowflakeDateTimeFormat timestampNTZFormatter;
  transient SnowflakeDateTimeFormat timestampLTZFormatter;
  transient SnowflakeDateTimeFormat timestampTZFormatter;
  transient SnowflakeDateTimeFormat dateFormatter;
  transient SnowflakeDateTimeFormat timeFormatter;
  transient SFBinaryFormat binaryFormatter;

  // Below fields are transient, they are generated on the fly.
  transient JsonNode firstChunkRowset = null; // only used for JSON result
  transient ChunkDownloader chunkDownloader = null;
  transient RootAllocator rootAllocator = null; // only used for ARROW result

  public void setRootAllocator(RootAllocator rootAllocator)
  {
    this.rootAllocator = rootAllocator;
  }

  public void setQueryResultFormat(QueryResultFormat queryResultFormat)
  {
    this.queryResultFormat = queryResultFormat;
  }

  public void setChunkFileCount(int chunkFileCount)
  {
    this.chunkFileCount = chunkFileCount;
  }

  public void setFristChunkStringData(String firstChunkStringData)
  {
    this.firstChunkStringData = firstChunkStringData;
  }

  public void setChunkDownloader(ChunkDownloader chunkDownloader)
  {
    this.chunkDownloader = chunkDownloader;
  }

  public SnowflakeConnectString getSnowflakeConnectString()
  {
    return snowflakeConnectionString;
  }

  public OCSPMode getOCSPMode()
  {
    return ocspMode;
  }

  public String getQrmk()
  {
    return qrmk;
  }

  public int getNetworkTimeoutInMilli()
  {
    return networkTimeoutInMilli;
  }

  public int getResultPrefetchThreads()
  {
    return resultPrefetchThreads;
  }

  public boolean getUseJsonParserV2()
  {
    return useJsonParserV2;
  }

  public long getMemoryLimit()
  {
    return memoryLimit;
  }

  public Map<String, String> getChunkHeadersMap()
  {
    return chunkHeadersMap;
  }

  public List<ChunkFileEntry> getChunkFileEntries()
  {
    return chunkFileEntries;
  }

  public RootAllocator getRootAllocator()
  {
    return rootAllocator;
  }

  public QueryResultFormat getQueryResultFormat()
  {
    return queryResultFormat;
  }

  public int getChunkFileCount()
  {
    return chunkFileCount;
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

  public String getFinalRoleName()
  {
    return finalRoleName;
  }

  public String getFinalWarehouseName()
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

  public JsonNode getAndClearFirstChunkRowset()
  {
    JsonNode firstChunkRowset = this.firstChunkRowset;
    this.firstChunkRowset = null;
    return firstChunkRowset;
  }

  public int getFirstChunkRowCount()
  {
    return firstChunkRowCount;
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

  public String getFirstChunkStringData()
  {
    return firstChunkStringData;
  }

  /**
   * A factory function to process result JSON.
   *
   * @param rootNode    wrapper object over simple json result
   * @param sfSession   the Snowflake session
   * @param sfStatement the Snowflake statement
   * @return processed ResultSetSerializable object
   * @throws SnowflakeSQLException if failed to parse the json result
   */
  static public SnowflakeResultSetSerializableV1 processResult(
      JsonNode rootNode,
      SFSession sfSession,
      SFStatement sfStatement)
  throws SnowflakeSQLException
  {
    SnowflakeResultSetSerializableV1 resultSetSerializable =
        new SnowflakeResultSetSerializableV1();
    logger.debug("Entering processResult");

    SnowflakeUtil.checkErrorAndThrowException(rootNode);

    // get the query id
    resultSetSerializable.queryId =
        rootNode.path("data").path("queryId").asText();

    JsonNode databaseNode = rootNode.path("data").path("finalDatabaseName");
    resultSetSerializable.finalDatabaseName =
        databaseNode.isNull() ? null : databaseNode.asText();

    JsonNode schemaNode = rootNode.path("data").path("finalSchemaName");
    resultSetSerializable.finalSchemaName =
        schemaNode.isNull() ? null : schemaNode.asText();

    JsonNode roleNode = rootNode.path("data").path("finalRoleName");
    resultSetSerializable.finalRoleName =
        roleNode.isNull() ? null : roleNode.asText();

    JsonNode warehouseNode = rootNode.path("data").path("finalWarehouseName");
    resultSetSerializable.finalWarehouseName =
        warehouseNode.isNull() ? null : warehouseNode.asText();

    resultSetSerializable.statementType = SFStatementType.lookUpTypeById(
        rootNode.path("data").path("statementTypeId").asLong());

    resultSetSerializable.totalRowCountTruncated
        = rootNode.path("data").path("totalTruncated").asBoolean();

    logger.debug("query id: {}", resultSetSerializable.queryId);

    Optional<QueryResultFormat> queryResultFormat = QueryResultFormat
        .lookupByName(rootNode.path("data").path("queryResultFormat").asText());
    resultSetSerializable.queryResultFormat =
        queryResultFormat.orElse(QueryResultFormat.JSON);

    // extract parameters
    resultSetSerializable.parameters =
        SessionUtil.getCommonParams(rootNode.path("data").path("parameters"));

    // initialize column metadata
    resultSetSerializable.columnCount =
        rootNode.path("data").path("rowtype").size();

    for (int i = 0; i < resultSetSerializable.columnCount; i++)
    {
      JsonNode colNode = rootNode.path("data").path("rowtype").path(i);

      SnowflakeColumnMetadata columnMetadata
          = SnowflakeUtil.extractColumnMetadata(
          colNode, sfSession.isJdbcTreatDecimalAsInt());

      resultSetSerializable.resultColumnMetadata.add(columnMetadata);

      logger.debug("Get column metadata: {}",
                   (ArgSupplier) () -> columnMetadata.toString());
    }

    // process the content of first chunk.
    if (resultSetSerializable.queryResultFormat == QueryResultFormat.ARROW)
    {
      resultSetSerializable.firstChunkStringData =
          rootNode.path("data").path("rowsetBase64").asText();
      resultSetSerializable.rootAllocator =
          new RootAllocator(Integer.MAX_VALUE);
    }
    else
    {
      resultSetSerializable.firstChunkRowset =
          rootNode.path("data").path("rowset");

      if (resultSetSerializable.firstChunkRowset == null ||
          resultSetSerializable.firstChunkRowset.isMissingNode())
      {
        resultSetSerializable.firstChunkRowCount = 0;
        resultSetSerializable.firstChunkStringData = null;
      }
      else
      {
        resultSetSerializable.firstChunkRowCount =
            resultSetSerializable.firstChunkRowset.size();
        resultSetSerializable.firstChunkStringData =
            resultSetSerializable.firstChunkRowset.toString();
      }

      logger.debug("First chunk row count: {}",
                   resultSetSerializable.firstChunkRowCount);
    }

    // parse file chunks
    resultSetSerializable.parseChunkFiles(rootNode, sfStatement);

    // result version
    JsonNode versionNode = rootNode.path("data").path("version");

    if (!versionNode.isMissingNode())
    {
      resultSetSerializable.resultVersion = versionNode.longValue();
    }

    // number of binds
    JsonNode numberOfBindsNode = rootNode.path("data").path("numberOfBinds");

    if (!numberOfBindsNode.isMissingNode())
    {
      resultSetSerializable.numberOfBinds = numberOfBindsNode.intValue();
    }

    JsonNode arrayBindSupported = rootNode.path("data")
        .path("arrayBindSupported");
    resultSetSerializable.arrayBindSupported =
        !arrayBindSupported.isMissingNode() && arrayBindSupported.asBoolean();

    // time result sent by GS (epoch time in millis)
    JsonNode sendResultTimeNode = rootNode.path("data").path("sendResultTime");
    if (!sendResultTimeNode.isMissingNode())
    {
      resultSetSerializable.sendResultTime = sendResultTimeNode.longValue();
    }

    logger.debug("result version={}", resultSetSerializable.resultVersion);

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
        MetaDataOfBinds param =
            new MetaDataOfBinds(precision, nullable, scale, byteLength, length,
                                name, type);
        returnVal.add(param);
      }
      resultSetSerializable.metaDataOfBinds = returnVal;
    }

    // setup fields from sessions.
    resultSetSerializable.ocspMode = sfSession.getOCSPMode();
    resultSetSerializable.snowflakeConnectionString =
        sfSession.getSnowflakeConnectionString();
    resultSetSerializable.networkTimeoutInMilli =
        sfSession.getNetworkTimeoutInMilli();

    // setup transient fields from parameter
    resultSetSerializable.setupFieldsFromParameters();

    // Initialize the chunk downloader if necessary
    if (resultSetSerializable.chunkFileCount > 0)
    {
      // The chunk downloader will start prefetching
      // first few chunk files in background thread(s)
      resultSetSerializable.chunkDownloader =
          new SnowflakeChunkDownloader(resultSetSerializable);
    }

    return resultSetSerializable;
  }

  /**
   * Some fields are generated from this.parameters, so generate them from
   * this.parameters instead of serializing them.
   */
  private void setupFieldsFromParameters()
  {
    String sqlTimestampFormat = (String) ResultUtil.effectiveParamValue(
        this.parameters, "TIMESTAMP_OUTPUT_FORMAT");

    // Special handling of specialized formatters, use a helper function
    this.timestampNTZFormatter = ResultUtil.specializedFormatter(
        this.parameters,
        "timestamp_ntz",
        "TIMESTAMP_NTZ_OUTPUT_FORMAT",
        sqlTimestampFormat);

    this.timestampLTZFormatter = ResultUtil.specializedFormatter(
        this.parameters,
        "timestamp_ltz",
        "TIMESTAMP_LTZ_OUTPUT_FORMAT",
        sqlTimestampFormat);

    this.timestampTZFormatter = ResultUtil.specializedFormatter(
        this.parameters,
        "timestamp_tz",
        "TIMESTAMP_TZ_OUTPUT_FORMAT",
        sqlTimestampFormat);

    String sqlDateFormat = (String) ResultUtil.effectiveParamValue(
        this.parameters,
        "DATE_OUTPUT_FORMAT");

    this.dateFormatter = new SnowflakeDateTimeFormat(sqlDateFormat);

    logger.debug("sql date format: {}, java date format: {}",
                 sqlDateFormat,
                 (ArgSupplier) () ->
                     this.dateFormatter.toSimpleDateTimePattern());

    String sqlTimeFormat = (String) ResultUtil.effectiveParamValue(
        this.parameters,
        "TIME_OUTPUT_FORMAT");

    this.timeFormatter = new SnowflakeDateTimeFormat(sqlTimeFormat);

    logger.debug("sql time format: {}, java time format: {}",
                 sqlTimeFormat,
                 (ArgSupplier) () ->
                     this.timeFormatter.toSimpleDateTimePattern());

    String timeZoneName = (String) ResultUtil.effectiveParamValue(
        this.parameters, "TIMEZONE");
    this.timeZone = TimeZone.getTimeZone(timeZoneName);

    this.honorClientTZForTimestampNTZ =
        (boolean) ResultUtil.effectiveParamValue(
            this.parameters,
            "CLIENT_HONOR_CLIENT_TZ_FOR_TIMESTAMP_NTZ");

    logger.debug("Honoring client TZ for timestamp_ntz? {}",
                 this.honorClientTZForTimestampNTZ);

    String binaryFmt = (String) ResultUtil.effectiveParamValue(
        this.parameters, "BINARY_OUTPUT_FORMAT");
    this.binaryFormatter =
        SFBinaryFormat.getSafeOutputFormat(binaryFmt);
  }

  /**
   * Parse the chunk file nodes from the json result.
   *
   * @param rootNode    wrapper object over simple json result
   * @param sfStatement the snowflake statement
   */
  private void parseChunkFiles(JsonNode rootNode,
                               SFStatement sfStatement)
  {
    JsonNode chunksNode = rootNode.path("data").path("chunks");

    if (!chunksNode.isMissingNode())
    {
      this.chunkFileCount = chunksNode.size();

      // Try to get the Query Result Master Key
      JsonNode qrmkNode = rootNode.path("data").path("qrmk");
      this.qrmk = qrmkNode.isMissingNode() ?
                  null : qrmkNode.textValue();

      // Determine the prefetch thread count and memoryLimit
      if (this.chunkFileCount > 0)
      {
        logger.debug("#chunks={}, initialize chunk downloader",
                     this.chunkFileCount);

        this.resultPrefetchThreads = DEFAULT_CLIENT_PREFETCH_THREADS;
        if (this.statementType.isSelect()
            && this.parameters
                .containsKey(CLIENT_ENABLE_CONSERVATIVE_MEMORY_USAGE)
            && (boolean) this.parameters
            .get(CLIENT_ENABLE_CONSERVATIVE_MEMORY_USAGE))
        {
          // use conservative memory settings
          this.resultPrefetchThreads =
              sfStatement.getConservativePrefetchThreads();
          this.memoryLimit = sfStatement.getConservativeMemoryLimit();
          int chunkSize =
              (int) this.parameters.get(CLIENT_RESULT_CHUNK_SIZE);
          logger.debug(
              "enable conservative memory usage with prefetchThreads = {} and memoryLimit = {} and " +
              "resultChunkSize = {}",
              this.resultPrefetchThreads, this.memoryLimit,
              chunkSize);
        }
        else
        {
          // prefetch threads
          if (this.parameters.get(CLIENT_PREFETCH_THREADS) != null)
          {
            this.resultPrefetchThreads =
                (int) this.parameters.get(CLIENT_PREFETCH_THREADS);
          }
          this.memoryLimit = initMemoryLimit(this.parameters);
        }

        // Parse chunk header
        JsonNode chunkHeaders = rootNode.path("data").path("chunkHeaders");
        if (chunkHeaders != null && !chunkHeaders.isMissingNode())
        {
          Iterator<Map.Entry<String, JsonNode>> chunkHeadersIter =
              chunkHeaders.fields();

          while (chunkHeadersIter.hasNext())
          {
            Map.Entry<String, JsonNode> chunkHeader = chunkHeadersIter.next();

            logger.debug("add header key={}, value={}",
                         chunkHeader.getKey(),
                         chunkHeader.getValue().asText());
            this.chunkHeadersMap.put(chunkHeader.getKey(),
                                     chunkHeader.getValue().asText());
          }
        }

        // parse chunk files metadata e.g. url and row count
        for (int idx = 0; idx < this.chunkFileCount; idx++)
        {
          JsonNode chunkNode = chunksNode.get(idx);
          String url = chunkNode.path("url").asText();
          int rowCount = chunkNode.path("rowCount").asInt();
          int compressedSize = chunkNode.path("compressedSize").asInt();
          int uncompressedSize = chunkNode.path("uncompressedSize").asInt();

          this.chunkFileEntries.add(
              new ChunkFileEntry(url, rowCount, compressedSize,
                                 uncompressedSize));

          logger.debug("add chunk, url={} rowCount={} " +
                       "compressedSize={} uncompressedSize={}",
                       url, rowCount, compressedSize, uncompressedSize);
        }

        /*
         * Should JsonParser be used instead of the original Json deserializer.
         */
        this.useJsonParserV2 =
            this.parameters.get("JDBC_USE_JSON_PARSER") != null &&
            (boolean) this.parameters.get("JDBC_USE_JSON_PARSER");
      }
    }
  }

  /**
   * initialize memory limit in bytes
   *
   * @param parameters The parameters for json result
   * @return memory limit in bytes
   */
  private static long initMemoryLimit(Map<String, Object> parameters)
  {
    // default setting
    long memoryLimit = DEFAULT_CLIENT_MEMORY_LIMIT * 1024 * 1024;
    if (parameters.get(CLIENT_MEMORY_LIMIT) != null)
    {
      // use the settings from the customer
      memoryLimit =
          (int) parameters.get(CLIENT_MEMORY_LIMIT) * 1024L * 1024L;
    }

    long maxMemoryToUse = Runtime.getRuntime().maxMemory() * 8 / 10;
    if ((int) parameters.get(CLIENT_MEMORY_LIMIT)
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

  /**
   * Generate a standard ResultSet from this object.
   *
   * @return a standard ResultSet to access the data wrapped in this object.
   */
  public ResultSet getResultSet() throws SQLException
  {
    throw new SQLException("SnowflakeResultSetSerializableV1.getResultSet() " +
                           "is not implemented yet");
  }
}
