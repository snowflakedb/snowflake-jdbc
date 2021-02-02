/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import static net.snowflake.client.core.Constants.GB;
import static net.snowflake.client.core.Constants.MB;
import static net.snowflake.client.core.SessionUtil.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import net.snowflake.client.core.*;
import net.snowflake.client.jdbc.telemetry.NoOpTelemetryClient;
import net.snowflake.client.jdbc.telemetry.Telemetry;
import net.snowflake.client.log.ArgSupplier;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.common.core.SFBinaryFormat;
import net.snowflake.common.core.SnowflakeDateTimeFormat;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;

/**
 * This object is an intermediate object between result JSON from GS and ResultSet. Originally, it
 * is created from result JSON. And it can also be serializable. Logically, it stands for a part of
 * ResultSet.
 *
 * <p>A typical result JSON data section consists of the content of the first chunk file and file
 * metadata for the rest of chunk files e.g. URL, chunk size, etc. So this object consists of one
 * chunk data and a list of chunk file entries. In actual cases, it may only include chunk data or
 * chunk files entries.
 *
 * <p>This object is serializable, so it can be distributed to other threads or worker nodes for
 * distributed processing.
 */
public class SnowflakeResultSetSerializableV1
    implements SnowflakeResultSetSerializable, Serializable {
  private static final long serialVersionUID = 1L;

  static final SFLogger logger = SFLoggerFactory.getLogger(SnowflakeResultSetSerializableV1.class);

  static final ObjectMapper mapper = ObjectMapperFactory.getObjectMapper();
  private static final long LOW_MAX_MEMORY = GB;

  /** An Entity class to represent a chunk file metadata. */
  public static class ChunkFileMetadata implements Serializable {
    private static final long serialVersionUID = 1L;
    String fileURL;
    int rowCount;
    int compressedByteSize;
    int uncompressedByteSize;

    public ChunkFileMetadata(
        String fileURL, int rowCount, int compressedByteSize, int uncompressedByteSize) {
      this.fileURL = fileURL;
      this.rowCount = rowCount;
      this.compressedByteSize = compressedByteSize;
      this.uncompressedByteSize = uncompressedByteSize;
    }

    public void setFileURL(String fileURL) {
      this.fileURL = fileURL;
    }

    public String getFileURL() {
      return fileURL;
    }

    public int getRowCount() {
      return rowCount;
    }

    public int getCompressedByteSize() {
      return compressedByteSize;
    }

    public int getUncompressedByteSize() {
      return uncompressedByteSize;
    }

    public String toString() {
      StringBuilder builder = new StringBuilder(1024);

      builder.append("RowCount: ").append(rowCount).append(", ");
      builder.append("CompressedSize: ").append(compressedByteSize).append(", ");
      builder.append("UnCompressedSize: ").append(uncompressedByteSize);

      return builder.toString();
    }
  }

  // Below fields are for the data fields that this object wraps
  // For ARROW, firstChunkStringData is BASE64-encoded arrow file.
  // For JSON,  it's string data for the json.
  String firstChunkStringData;
  int firstChunkRowCount;
  int chunkFileCount;
  List<ChunkFileMetadata> chunkFileMetadatas = new ArrayList<>();

  // below fields are used for building a ChunkDownloader which
  // uses http client to download chunk files
  int resultPrefetchThreads;
  String qrmk;
  Map<String, String> chunkHeadersMap = new HashMap<>();
  // Below fields are from session or statement
  SnowflakeConnectString snowflakeConnectionString;
  OCSPMode ocspMode;
  int networkTimeoutInMilli;
  boolean isResultColumnCaseInsensitive;
  int resultSetType;
  int resultSetConcurrency;
  int resultSetHoldability;

  // Below are some metadata fields parsed from the result JSON node
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
  long resultVersion;
  int numberOfBinds;
  boolean arrayBindSupported;
  long sendResultTime;
  List<MetaDataOfBinds> metaDataOfBinds = new ArrayList<>();
  QueryResultFormat queryResultFormat;
  boolean treatNTZAsUTC;
  boolean formatDateWithTimezone;
  boolean useSessionTimezone;

  // Below fields are transient, they are generated from parameters
  transient TimeZone timeZone;
  transient Optional<SFBaseSession> possibleSession = Optional.empty();
  transient boolean honorClientTZForTimestampNTZ;
  transient SnowflakeDateTimeFormat timestampNTZFormatter;
  transient SnowflakeDateTimeFormat timestampLTZFormatter;
  transient SnowflakeDateTimeFormat timestampTZFormatter;
  transient SnowflakeDateTimeFormat dateFormatter;
  transient SnowflakeDateTimeFormat timeFormatter;
  transient SFBinaryFormat binaryFormatter;
  transient long memoryLimit;

  // Below fields are transient, they are generated on the fly.
  transient JsonNode firstChunkRowset = null; // only used for JSON result
  transient ChunkDownloader chunkDownloader = null;
  transient RootAllocator rootAllocator = null; // only used for ARROW result
  transient SFResultSetMetaData resultSetMetaData = null;

  /** Default constructor. */
  public SnowflakeResultSetSerializableV1() {}

  /**
   * This is copy constructor.
   *
   * <p>NOTE: The copy is NOT deep copy.
   *
   * @param toCopy the source object to be copied.
   */
  private SnowflakeResultSetSerializableV1(SnowflakeResultSetSerializableV1 toCopy) {
    // Below fields are for the data fields that this object wraps
    this.firstChunkStringData = toCopy.firstChunkStringData;
    this.firstChunkRowCount = toCopy.firstChunkRowCount;
    this.chunkFileCount = toCopy.chunkFileCount;
    this.chunkFileMetadatas = toCopy.chunkFileMetadatas;

    // below fields are used for building a ChunkDownloader
    this.resultPrefetchThreads = toCopy.resultPrefetchThreads;
    this.qrmk = toCopy.qrmk;
    this.chunkHeadersMap = toCopy.chunkHeadersMap;

    // Below fields are from session or statement
    this.snowflakeConnectionString = toCopy.snowflakeConnectionString;
    this.ocspMode = toCopy.ocspMode;
    this.networkTimeoutInMilli = toCopy.networkTimeoutInMilli;
    this.isResultColumnCaseInsensitive = toCopy.isResultColumnCaseInsensitive;
    this.resultSetType = toCopy.resultSetType;
    this.resultSetConcurrency = toCopy.resultSetConcurrency;
    this.resultSetHoldability = toCopy.resultSetHoldability;
    this.treatNTZAsUTC = toCopy.treatNTZAsUTC;
    this.formatDateWithTimezone = toCopy.formatDateWithTimezone;
    this.useSessionTimezone = toCopy.useSessionTimezone;

    // Below are some metadata fields parsed from the result JSON node
    this.queryId = toCopy.queryId;
    this.finalDatabaseName = toCopy.finalDatabaseName;
    this.finalSchemaName = toCopy.finalSchemaName;
    this.finalRoleName = toCopy.finalRoleName;
    this.finalWarehouseName = toCopy.finalWarehouseName;
    this.statementType = toCopy.statementType;
    this.totalRowCountTruncated = toCopy.totalRowCountTruncated;
    this.parameters = toCopy.parameters;
    this.columnCount = toCopy.columnCount;
    this.resultColumnMetadata = toCopy.resultColumnMetadata;
    this.resultVersion = toCopy.resultVersion;
    this.numberOfBinds = toCopy.numberOfBinds;
    this.arrayBindSupported = toCopy.arrayBindSupported;
    this.sendResultTime = toCopy.sendResultTime;
    this.metaDataOfBinds = toCopy.metaDataOfBinds;
    this.queryResultFormat = toCopy.queryResultFormat;
    this.possibleSession = toCopy.possibleSession;

    // Below fields are transient, they are generated from parameters
    this.timeZone = toCopy.timeZone;
    this.honorClientTZForTimestampNTZ = toCopy.honorClientTZForTimestampNTZ;
    this.timestampNTZFormatter = toCopy.timestampNTZFormatter;
    this.timestampLTZFormatter = toCopy.timestampLTZFormatter;
    this.timestampTZFormatter = toCopy.timestampTZFormatter;
    this.dateFormatter = toCopy.dateFormatter;
    this.timeFormatter = toCopy.timeFormatter;
    this.binaryFormatter = toCopy.binaryFormatter;
    this.memoryLimit = toCopy.memoryLimit;

    // Below fields are transient, they are generated on the fly.
    this.firstChunkRowset = toCopy.firstChunkRowset;
    this.chunkDownloader = toCopy.chunkDownloader;
    this.rootAllocator = toCopy.rootAllocator;
    this.resultSetMetaData = toCopy.resultSetMetaData;
  }

  public void setRootAllocator(RootAllocator rootAllocator) {
    this.rootAllocator = rootAllocator;
  }

  public void setQueryResultFormat(QueryResultFormat queryResultFormat) {
    this.queryResultFormat = queryResultFormat;
  }

  public void setChunkFileCount(int chunkFileCount) {
    this.chunkFileCount = chunkFileCount;
  }

  public void setFristChunkStringData(String firstChunkStringData) {
    this.firstChunkStringData = firstChunkStringData;
  }

  public void setChunkDownloader(ChunkDownloader chunkDownloader) {
    this.chunkDownloader = chunkDownloader;
  }

  public SFResultSetMetaData getSFResultSetMetaData() {
    return resultSetMetaData;
  }

  public int getResultSetType() {
    return resultSetType;
  }

  public int getResultSetConcurrency() {
    return resultSetConcurrency;
  }

  public int getResultSetHoldability() {
    return resultSetHoldability;
  }

  public SnowflakeConnectString getSnowflakeConnectString() {
    return snowflakeConnectionString;
  }

  public OCSPMode getOCSPMode() {
    return ocspMode;
  }

  public String getQrmk() {
    return qrmk;
  }

  public int getNetworkTimeoutInMilli() {
    return networkTimeoutInMilli;
  }

  public int getResultPrefetchThreads() {
    return resultPrefetchThreads;
  }

  public long getMemoryLimit() {
    return memoryLimit;
  }

  public Map<String, String> getChunkHeadersMap() {
    return chunkHeadersMap;
  }

  public List<ChunkFileMetadata> getChunkFileMetadatas() {
    return chunkFileMetadatas;
  }

  public RootAllocator getRootAllocator() {
    return rootAllocator;
  }

  public QueryResultFormat getQueryResultFormat() {
    return queryResultFormat;
  }

  public int getChunkFileCount() {
    return chunkFileCount;
  }

  public boolean isArrayBindSupported() {
    return arrayBindSupported;
  }

  public String getQueryId() {
    return queryId;
  }

  public String getFinalDatabaseName() {
    return finalDatabaseName;
  }

  public String getFinalSchemaName() {
    return finalSchemaName;
  }

  public String getFinalRoleName() {
    return finalRoleName;
  }

  public String getFinalWarehouseName() {
    return finalWarehouseName;
  }

  public SFStatementType getStatementType() {
    return statementType;
  }

  public boolean isTotalRowCountTruncated() {
    return totalRowCountTruncated;
  }

  public Map<String, Object> getParameters() {
    return parameters;
  }

  public int getColumnCount() {
    return columnCount;
  }

  public List<SnowflakeColumnMetadata> getResultColumnMetadata() {
    return resultColumnMetadata;
  }

  public JsonNode getAndClearFirstChunkRowset() {
    JsonNode firstChunkRowset = this.firstChunkRowset;
    this.firstChunkRowset = null;
    return firstChunkRowset;
  }

  public int getFirstChunkRowCount() {
    return firstChunkRowCount;
  }

  public long getResultVersion() {
    return resultVersion;
  }

  public int getNumberOfBinds() {
    return numberOfBinds;
  }

  public ChunkDownloader getChunkDownloader() {
    return chunkDownloader;
  }

  public SnowflakeDateTimeFormat getTimestampNTZFormatter() {
    return timestampNTZFormatter;
  }

  public SnowflakeDateTimeFormat getTimestampLTZFormatter() {
    return timestampLTZFormatter;
  }

  public SnowflakeDateTimeFormat getTimestampTZFormatter() {
    return timestampTZFormatter;
  }

  public SnowflakeDateTimeFormat getDateFormatter() {
    return dateFormatter;
  }

  public SnowflakeDateTimeFormat getTimeFormatter() {
    return timeFormatter;
  }

  public TimeZone getTimeZone() {
    return timeZone;
  }

  public boolean isHonorClientTZForTimestampNTZ() {
    return honorClientTZForTimestampNTZ;
  }

  public SFBinaryFormat getBinaryFormatter() {
    return binaryFormatter;
  }

  public long getSendResultTime() {
    return sendResultTime;
  }

  public List<MetaDataOfBinds> getMetaDataOfBinds() {
    return metaDataOfBinds;
  }

  public String getFirstChunkStringData() {
    return firstChunkStringData;
  }

  public boolean getTreatNTZAsUTC() {
    return treatNTZAsUTC;
  }

  public boolean getFormatDateWithTimeZone() {
    return formatDateWithTimezone;
  }

  public boolean getUseSessionTimezone() {
    return useSessionTimezone;
  }

  public Optional<SFBaseSession> getSession() {
    return possibleSession;
  }

  /**
   * A factory function to create SnowflakeResultSetSerializable object from result JSON node.
   *
   * @param rootNode result JSON node received from GS
   * @param sfSession the Snowflake session
   * @param sfStatement the Snowflake statement
   * @return processed ResultSetSerializable object
   * @throws SnowflakeSQLException if failed to parse the result JSON node
   */
  public static SnowflakeResultSetSerializableV1 create(
      JsonNode rootNode, SFSession sfSession, SFStatement sfStatement)
      throws SnowflakeSQLException {
    SnowflakeResultSetSerializableV1 resultSetSerializable = new SnowflakeResultSetSerializableV1();
    logger.debug("Entering create()");

    SnowflakeUtil.checkErrorAndThrowException(rootNode);

    // get the query id
    resultSetSerializable.queryId = rootNode.path("data").path("queryId").asText();

    JsonNode databaseNode = rootNode.path("data").path("finalDatabaseName");
    resultSetSerializable.finalDatabaseName = databaseNode.isNull() ? null : databaseNode.asText();

    JsonNode schemaNode = rootNode.path("data").path("finalSchemaName");
    resultSetSerializable.finalSchemaName = schemaNode.isNull() ? null : schemaNode.asText();

    JsonNode roleNode = rootNode.path("data").path("finalRoleName");
    resultSetSerializable.finalRoleName = roleNode.isNull() ? null : roleNode.asText();

    JsonNode warehouseNode = rootNode.path("data").path("finalWarehouseName");
    resultSetSerializable.finalWarehouseName =
        warehouseNode.isNull() ? null : warehouseNode.asText();

    resultSetSerializable.statementType =
        SFStatementType.lookUpTypeById(rootNode.path("data").path("statementTypeId").asLong());

    resultSetSerializable.totalRowCountTruncated =
        rootNode.path("data").path("totalTruncated").asBoolean();

    resultSetSerializable.possibleSession = Optional.ofNullable(sfSession);

    logger.debug("query id: {}", resultSetSerializable.queryId);

    Optional<QueryResultFormat> queryResultFormat =
        QueryResultFormat.lookupByName(rootNode.path("data").path("queryResultFormat").asText());
    resultSetSerializable.queryResultFormat = queryResultFormat.orElse(QueryResultFormat.JSON);

    // extract parameters
    resultSetSerializable.parameters =
        SessionUtil.getCommonParams(rootNode.path("data").path("parameters"));

    // initialize column metadata
    resultSetSerializable.columnCount = rootNode.path("data").path("rowtype").size();

    for (int i = 0; i < resultSetSerializable.columnCount; i++) {
      JsonNode colNode = rootNode.path("data").path("rowtype").path(i);

      SnowflakeColumnMetadata columnMetadata =
          SnowflakeUtil.extractColumnMetadata(
              colNode, sfSession.isJdbcTreatDecimalAsInt(), sfSession);

      resultSetSerializable.resultColumnMetadata.add(columnMetadata);

      logger.debug("Get column metadata: {}", (ArgSupplier) () -> columnMetadata.toString());
    }

    // process the content of first chunk.
    if (resultSetSerializable.queryResultFormat == QueryResultFormat.ARROW) {
      resultSetSerializable.firstChunkStringData =
          rootNode.path("data").path("rowsetBase64").asText();
      resultSetSerializable.rootAllocator = new RootAllocator(Long.MAX_VALUE);
      // Set first chunk row count from firstChunkStringData
      resultSetSerializable.setFirstChunkRowCountForArrow();
    } else {
      resultSetSerializable.firstChunkRowset = rootNode.path("data").path("rowset");

      if (resultSetSerializable.firstChunkRowset == null
          || resultSetSerializable.firstChunkRowset.isMissingNode()) {
        resultSetSerializable.firstChunkRowCount = 0;
        resultSetSerializable.firstChunkStringData = null;
      } else {
        resultSetSerializable.firstChunkRowCount = resultSetSerializable.firstChunkRowset.size();
        resultSetSerializable.firstChunkStringData =
            resultSetSerializable.firstChunkRowset.toString();
      }
    }
    logger.debug("First chunk row count: {}", resultSetSerializable.firstChunkRowCount);

    // parse file chunks
    resultSetSerializable.parseChunkFiles(rootNode, sfStatement);

    // result version
    JsonNode versionNode = rootNode.path("data").path("version");

    if (!versionNode.isMissingNode()) {
      resultSetSerializable.resultVersion = versionNode.longValue();
    }

    // number of binds
    JsonNode numberOfBindsNode = rootNode.path("data").path("numberOfBinds");

    if (!numberOfBindsNode.isMissingNode()) {
      resultSetSerializable.numberOfBinds = numberOfBindsNode.intValue();
    }

    JsonNode arrayBindSupported = rootNode.path("data").path("arrayBindSupported");
    resultSetSerializable.arrayBindSupported =
        !arrayBindSupported.isMissingNode() && arrayBindSupported.asBoolean();

    // time result sent by GS (epoch time in millis)
    JsonNode sendResultTimeNode = rootNode.path("data").path("sendResultTime");
    if (!sendResultTimeNode.isMissingNode()) {
      resultSetSerializable.sendResultTime = sendResultTimeNode.longValue();
    }

    logger.debug("result version={}", resultSetSerializable.resultVersion);

    // Bind parameter metadata
    JsonNode bindData = rootNode.path("data").path("metaDataOfBinds");
    if (!bindData.isMissingNode()) {
      List<MetaDataOfBinds> returnVal = new ArrayList<>();
      for (JsonNode child : bindData) {
        int precision = child.path("precision").asInt();
        boolean nullable = child.path("nullable").asBoolean();
        int scale = child.path("scale").asInt();
        int byteLength = child.path("byteLength").asInt();
        int length = child.path("length").asInt();
        String name = child.path("name").asText();
        String type = child.path("type").asText();
        MetaDataOfBinds param =
            new MetaDataOfBinds(precision, nullable, scale, byteLength, length, name, type);
        returnVal.add(param);
      }
      resultSetSerializable.metaDataOfBinds = returnVal;
    }

    // setup fields from sessions.
    resultSetSerializable.ocspMode = sfSession.getOCSPMode();
    resultSetSerializable.snowflakeConnectionString = sfSession.getSnowflakeConnectionString();
    resultSetSerializable.networkTimeoutInMilli = sfSession.getNetworkTimeoutInMilli();
    resultSetSerializable.isResultColumnCaseInsensitive = sfSession.isResultColumnCaseInsensitive();
    resultSetSerializable.treatNTZAsUTC = sfSession.getTreatNTZAsUTC();
    resultSetSerializable.formatDateWithTimezone = sfSession.getFormatDateWithTimezone();
    resultSetSerializable.useSessionTimezone = sfSession.getUseSessionTimezone();

    // setup transient fields from parameter
    resultSetSerializable.setupFieldsFromParameters();

    // The chunk downloader will start prefetching
    // first few chunk files in background thread(s)
    resultSetSerializable.chunkDownloader =
        (resultSetSerializable.chunkFileCount > 0)
            ? new SnowflakeChunkDownloader(resultSetSerializable)
            : new SnowflakeChunkDownloader.NoOpChunkDownloader();

    // Setup ResultSet metadata
    resultSetSerializable.resultSetMetaData =
        new SFResultSetMetaData(
            resultSetSerializable.getResultColumnMetadata(),
            resultSetSerializable.queryId,
            sfSession,
            resultSetSerializable.isResultColumnCaseInsensitive,
            resultSetSerializable.timestampNTZFormatter,
            resultSetSerializable.timestampLTZFormatter,
            resultSetSerializable.timestampTZFormatter,
            resultSetSerializable.dateFormatter,
            resultSetSerializable.timeFormatter);

    return resultSetSerializable;
  }

  /**
   * Some fields are generated from this.parameters, so generate them from this.parameters instead
   * of serializing them.
   */
  private void setupFieldsFromParameters() {
    String sqlTimestampFormat =
        (String) ResultUtil.effectiveParamValue(this.parameters, "TIMESTAMP_OUTPUT_FORMAT");

    // Special handling of specialized formatters, use a helper function
    this.timestampNTZFormatter =
        ResultUtil.specializedFormatter(
            this.parameters, "timestamp_ntz", "TIMESTAMP_NTZ_OUTPUT_FORMAT", sqlTimestampFormat);

    this.timestampLTZFormatter =
        ResultUtil.specializedFormatter(
            this.parameters, "timestamp_ltz", "TIMESTAMP_LTZ_OUTPUT_FORMAT", sqlTimestampFormat);

    this.timestampTZFormatter =
        ResultUtil.specializedFormatter(
            this.parameters, "timestamp_tz", "TIMESTAMP_TZ_OUTPUT_FORMAT", sqlTimestampFormat);

    String sqlDateFormat =
        (String) ResultUtil.effectiveParamValue(this.parameters, "DATE_OUTPUT_FORMAT");

    this.dateFormatter = SnowflakeDateTimeFormat.fromSqlFormat(sqlDateFormat);

    logger.debug(
        "sql date format: {}, java date format: {}",
        sqlDateFormat,
        (ArgSupplier) () -> this.dateFormatter.toSimpleDateTimePattern());

    String sqlTimeFormat =
        (String) ResultUtil.effectiveParamValue(this.parameters, "TIME_OUTPUT_FORMAT");

    this.timeFormatter = SnowflakeDateTimeFormat.fromSqlFormat(sqlTimeFormat);

    logger.debug(
        "sql time format: {}, java time format: {}",
        sqlTimeFormat,
        (ArgSupplier) () -> this.timeFormatter.toSimpleDateTimePattern());

    String timeZoneName = (String) ResultUtil.effectiveParamValue(this.parameters, "TIMEZONE");
    this.timeZone = TimeZone.getTimeZone(timeZoneName);

    this.honorClientTZForTimestampNTZ =
        (boolean)
            ResultUtil.effectiveParamValue(
                this.parameters, "CLIENT_HONOR_CLIENT_TZ_FOR_TIMESTAMP_NTZ");

    logger.debug("Honoring client TZ for timestamp_ntz? {}", this.honorClientTZForTimestampNTZ);

    String binaryFmt =
        (String) ResultUtil.effectiveParamValue(this.parameters, "BINARY_OUTPUT_FORMAT");
    this.binaryFormatter = SFBinaryFormat.getSafeOutputFormat(binaryFmt);
  }

  /**
   * Parse the chunk file nodes from result JSON node
   *
   * @param rootNode result JSON node received from GS
   * @param sfStatement the snowflake statement
   */
  private void parseChunkFiles(JsonNode rootNode, SFStatement sfStatement) {
    JsonNode chunksNode = rootNode.path("data").path("chunks");

    if (!chunksNode.isMissingNode()) {
      this.chunkFileCount = chunksNode.size();

      // Try to get the Query Result Master Key
      JsonNode qrmkNode = rootNode.path("data").path("qrmk");
      this.qrmk = qrmkNode.isMissingNode() ? null : qrmkNode.textValue();

      // Determine the prefetch thread count and memoryLimit
      if (this.chunkFileCount > 0) {
        logger.debug("#chunks={}, initialize chunk downloader", this.chunkFileCount);

        adjustMemorySettings(sfStatement);

        // Parse chunk header
        JsonNode chunkHeaders = rootNode.path("data").path("chunkHeaders");
        if (chunkHeaders != null && !chunkHeaders.isMissingNode()) {
          Iterator<Map.Entry<String, JsonNode>> chunkHeadersIter = chunkHeaders.fields();

          while (chunkHeadersIter.hasNext()) {
            Map.Entry<String, JsonNode> chunkHeader = chunkHeadersIter.next();

            logger.debug(
                "add header key={}, value={}",
                chunkHeader.getKey(),
                chunkHeader.getValue().asText());
            this.chunkHeadersMap.put(chunkHeader.getKey(), chunkHeader.getValue().asText());
          }
        }

        // parse chunk files metadata e.g. url and row count
        for (int idx = 0; idx < this.chunkFileCount; idx++) {
          JsonNode chunkNode = chunksNode.get(idx);
          String url = chunkNode.path("url").asText();
          int rowCount = chunkNode.path("rowCount").asInt();
          int compressedSize = chunkNode.path("compressedSize").asInt();
          int uncompressedSize = chunkNode.path("uncompressedSize").asInt();

          this.chunkFileMetadatas.add(
              new ChunkFileMetadata(url, rowCount, compressedSize, uncompressedSize));

          logger.debug(
              "add chunk, url={} rowCount={} " + "compressedSize={} uncompressedSize={}",
              url,
              rowCount,
              compressedSize,
              uncompressedSize);
        }
      }
    }
  }

  private void adjustMemorySettings(SFStatement sfStatement) {
    this.resultPrefetchThreads = DEFAULT_CLIENT_PREFETCH_THREADS;
    if (this.statementType.isSelect()
        && this.parameters.containsKey(CLIENT_ENABLE_CONSERVATIVE_MEMORY_USAGE)
        && (boolean) this.parameters.get(CLIENT_ENABLE_CONSERVATIVE_MEMORY_USAGE)) {
      // use conservative memory settings
      this.resultPrefetchThreads = sfStatement.getConservativePrefetchThreads();
      this.memoryLimit = sfStatement.getConservativeMemoryLimit();
      int chunkSize = (int) this.parameters.get(CLIENT_RESULT_CHUNK_SIZE);
      logger.debug(
          "enable conservative memory usage with prefetchThreads = {} and memoryLimit = {} and "
              + "resultChunkSize = {}",
          this.resultPrefetchThreads,
          this.memoryLimit,
          chunkSize);
    } else {
      // prefetch threads
      if (this.parameters.get(CLIENT_PREFETCH_THREADS) != null) {
        this.resultPrefetchThreads = (int) this.parameters.get(CLIENT_PREFETCH_THREADS);
      }
      this.memoryLimit = initMemoryLimit(this.parameters);
    }

    long maxChunkSize = (int) this.parameters.get(CLIENT_RESULT_CHUNK_SIZE) * MB;
    if (queryResultFormat == QueryResultFormat.ARROW
        && Runtime.getRuntime().maxMemory() < LOW_MAX_MEMORY
        && memoryLimit * 2 + maxChunkSize > Runtime.getRuntime().maxMemory()) {
      memoryLimit = Runtime.getRuntime().maxMemory() / 2 - maxChunkSize;
      logger.debug(
          "To avoid OOM for arrow buffer allocation, "
              + "memoryLimit {} should be less than half of the "
              + "maxMemory {} + maxChunkSize {}",
          memoryLimit,
          Runtime.getRuntime().maxMemory(),
          maxChunkSize);
    }
  }

  /**
   * Calculate memory limit in bytes
   *
   * @param parameters The parameters for result JSON node
   * @return memory limit in bytes
   */
  private static long initMemoryLimit(Map<String, Object> parameters) {
    // default setting
    long memoryLimit = DEFAULT_CLIENT_MEMORY_LIMIT * 1024 * 1024;
    if (parameters.get(CLIENT_MEMORY_LIMIT) != null) {
      // use the settings from the customer
      memoryLimit = (int) parameters.get(CLIENT_MEMORY_LIMIT) * 1024L * 1024L;
    }

    long maxMemoryToUse = Runtime.getRuntime().maxMemory() * 8 / 10;
    if ((int) parameters.get(CLIENT_MEMORY_LIMIT) == DEFAULT_CLIENT_MEMORY_LIMIT) {
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
   * Setup all transient fields based on serialized fields and System Runtime.
   *
   * @throws SQLException if fails to setup any transient fields
   */
  private void setupTransientFields() throws SQLException {
    // Setup transient fields from serialized fields
    setupFieldsFromParameters();

    // Setup memory limitation from parameters and System Runtime.
    this.memoryLimit = initMemoryLimit(this.parameters);

    // Create below transient fields on the fly.
    if (QueryResultFormat.ARROW.equals(this.queryResultFormat)) {
      this.rootAllocator = new RootAllocator(Long.MAX_VALUE);
      this.firstChunkRowset = null;
    } else {
      this.rootAllocator = null;
      try {
        this.firstChunkRowset =
            (this.firstChunkStringData != null) ? mapper.readTree(this.firstChunkStringData) : null;
      } catch (IOException ex) {
        throw new SnowflakeSQLLoggedException(
            possibleSession.orElse(/* session = */ null),
            "The JSON data is invalid. The error is: " + ex.getMessage());
      }
    }

    // Setup ResultSet metadata
    this.resultSetMetaData =
        new SFResultSetMetaData(
            this.getResultColumnMetadata(),
            this.queryId,
            null, // This is session less
            this.isResultColumnCaseInsensitive,
            this.timestampNTZFormatter,
            this.timestampLTZFormatter,
            this.timestampTZFormatter,
            this.dateFormatter,
            this.timeFormatter);

    // Allocate chunk downloader if necessary
    chunkDownloader =
        (this.chunkFileCount > 0)
            ? new SnowflakeChunkDownloader(this)
            : new SnowflakeChunkDownloader.NoOpChunkDownloader();
  }

  /**
   * Split this object into small pieces based on the user specified data size.
   *
   * @param maxSizeInBytes the expected max data size wrapped in the result ResultSetSerializables
   *     object. NOTE: if a result chunk size is greater than this value, the ResultSetSerializable
   *     object will include one result chunk.
   * @return a list of SnowflakeResultSetSerializable
   * @throws SQLException if fails to split objects.
   */
  public List<SnowflakeResultSetSerializable> splitBySize(long maxSizeInBytes) throws SQLException {
    List<SnowflakeResultSetSerializable> resultSetSerializables = new ArrayList<>();

    if (this.chunkFileMetadatas.isEmpty() && this.firstChunkStringData == null) {
      throw new SnowflakeSQLLoggedException(
          this.possibleSession.orElse(/* session = */ null),
          "The Result Set serializable is invalid.");
    }

    // In the beginning, only the first data chunk is included in the result
    // serializable, so the chunk files are removed from the copy.
    // NOTE: make sure to handle the case that the first data chunk doesn't
    // exist.
    SnowflakeResultSetSerializableV1 curResultSetSerializable =
        new SnowflakeResultSetSerializableV1(this);
    curResultSetSerializable.chunkFileMetadatas = new ArrayList<>();
    curResultSetSerializable.chunkFileCount = 0;

    for (int idx = 0; idx < this.chunkFileCount; idx++) {
      ChunkFileMetadata curChunkFileMetadata = this.getChunkFileMetadatas().get(idx);

      // If the serializable object has reach the max size,
      // save current one and create new one.
      if ((curResultSetSerializable.getUncompressedDataSizeInBytes() > 0)
          && (maxSizeInBytes
              < (curResultSetSerializable.getUncompressedDataSizeInBytes()
                  + curChunkFileMetadata.getUncompressedByteSize()))) {
        resultSetSerializables.add(curResultSetSerializable);

        // Create new result serializable and reset it as empty
        curResultSetSerializable = new SnowflakeResultSetSerializableV1(this);
        curResultSetSerializable.chunkFileMetadatas = new ArrayList<>();
        curResultSetSerializable.chunkFileCount = 0;
        curResultSetSerializable.firstChunkStringData = null;
        curResultSetSerializable.firstChunkRowCount = 0;
        curResultSetSerializable.firstChunkRowset = null;
      }

      // Append this chunk file to result serializable object
      curResultSetSerializable.getChunkFileMetadatas().add(curChunkFileMetadata);
      curResultSetSerializable.chunkFileCount++;
    }

    // Add the last result serializable object into result.
    resultSetSerializables.add(curResultSetSerializable);

    return resultSetSerializables;
  }

  /**
   * Get ResultSet from the ResultSet Serializable object so that the user can access the data.
   *
   * @param resultSetRetrieveConfig The extra info to retrieve the result set.
   * @return a ResultSet which represents for the data wrapped in the object
   */
  public ResultSet getResultSet(ResultSetRetrieveConfig resultSetRetrieveConfig)
      throws SQLException {
    // Adjust OCSP cache server if necessary.
    try {
      SessionUtil.resetOCSPUrlIfNecessary(resultSetRetrieveConfig.getSfFullURL());
    } catch (IOException e) {
      throw new SnowflakeSQLLoggedException(
          /*session = */ null, // There is no connection
          ErrorCode.INTERNAL_ERROR,
          "Hit exception when adjusting OCSP cache server. The original message is: "
              + e.getMessage());
    }

    return getResultSetInternal(resultSetRetrieveConfig.getProxyProperties());
  }

  /**
   * Get ResultSet from the ResultSet Serializable object so that the user can access the data.
   *
   * <p>This API is used by spark spark connector from 2.6.0 to 2.8.1. It is deprecated from
   * sc:2.8.2/jdbc:3.12.12 since Sept 2020. It is safe to remove it after Sept 2022.
   *
   * @return a ResultSet which represents for the data wrapped in the object
   * @deprecated Please use new interface function getResultSet(ResultSetRetrieveConfig)
   */
  @Deprecated
  public ResultSet getResultSet() throws SQLException {
    return getResultSetInternal(null);
  }

  /**
   * Get ResultSet from the ResultSet Serializable object so that the user can access the data.
   *
   * <p>This API is used by spark spark connector from 2.6.0 to 2.8.1. It is deprecated from
   * sc:2.8.2/jdbc:3.12.12 since Sept 2020. It is safe to remove it after Sept 2022.
   *
   * @param info The proxy sever information if proxy is necessary.
   * @return a ResultSet which represents for the data wrapped in the object
   * @deprecated Please use new interface function getResultSet(ResultSetRetrieveConfig)
   */
  @Deprecated
  public ResultSet getResultSet(Properties info) throws SQLException {
    return getResultSetInternal(info);
  }

  /**
   * Get ResultSet from the ResultSet Serializable object so that the user can access the data.
   *
   * @param info The proxy sever information if proxy is necessary.
   * @return a ResultSet which represents for the data wrapped in the object
   */
  private ResultSet getResultSetInternal(Properties info) throws SQLException {
    // Setup proxy info if necessary
    SnowflakeUtil.setupProxyPropertiesIfNecessary(info);

    // Setup transient fields
    setupTransientFields();

    // This result set is sessionless, so it doesn't support telemetry.
    Telemetry telemetryClient = new NoOpTelemetryClient();
    // The use case is distributed processing, so sortResult is not necessary.
    boolean sortResult = false;
    // Setup base result set.
    SFBaseResultSet sfBaseResultSet = null;
    switch (getQueryResultFormat()) {
      case ARROW:
        {
          sfBaseResultSet = new SFArrowResultSet(this, telemetryClient, sortResult);
          break;
        }
      case JSON:
        {
          sfBaseResultSet = new SFResultSet(this, telemetryClient, sortResult);
          break;
        }
      default:
        throw new SnowflakeSQLLoggedException(
            this.possibleSession.orElse(/*session = */ null),
            ErrorCode.INTERNAL_ERROR,
            "Unsupported query result format: " + getQueryResultFormat().name());
    }

    // Create result set
    SnowflakeResultSetV1 resultSetV1 = new SnowflakeResultSetV1(sfBaseResultSet, this);

    return resultSetV1;
  }

  // Set the row count for first result chunk by parsing the chunk data.
  private void setFirstChunkRowCountForArrow() throws SnowflakeSQLException {
    firstChunkRowCount = 0;

    // If the first chunk doesn't exist or empty, set it as 0
    if (firstChunkStringData == null || firstChunkStringData.isEmpty()) {
      firstChunkRowCount = 0;
    }
    // Parse the Arrow result chunk
    else if (getQueryResultFormat().equals(QueryResultFormat.ARROW)) {
      // Below code is developed based on SFArrowResultSet.buildFirstChunk
      // and ArrowResultChunk.readArrowStream()
      byte[] bytes = Base64.getDecoder().decode(firstChunkStringData);
      VectorSchemaRoot root = null;
      RootAllocator localRootAllocator =
          (rootAllocator != null) ? rootAllocator : new RootAllocator(Long.MAX_VALUE);
      try (ByteArrayInputStream is = new ByteArrayInputStream(bytes);
          ArrowStreamReader reader = new ArrowStreamReader(is, localRootAllocator)) {
        root = reader.getVectorSchemaRoot();
        while (reader.loadNextBatch()) {
          firstChunkRowCount += root.getRowCount();
          root.clear();
        }
      } catch (Exception ex) {
        throw new SnowflakeSQLLoggedException(
            possibleSession.orElse(/* session = */ null),
            ErrorCode.INTERNAL_ERROR,
            "Fail to retrieve row count for first arrow chunk: " + ex.getCause());
      } finally {
        if (root != null) {
          root.clear();
        }
      }
    } else {
      // This shouldn't happen
      throw new SnowflakeSQLLoggedException(
          this.possibleSession.orElse(/*session = */ null),
          ErrorCode.INTERNAL_ERROR,
          "setFirstChunkRowCountForArrow() should only be called for Arrow.");
    }
  }

  /**
   * Retrieve total row count included in the the ResultSet Serializable object.
   *
   * <p>GS sends the data of first chunk and metadata of the other chunk if exist to client, so this
   * function calculates the row count for all of them.
   *
   * @return the total row count from metadata
   */
  public long getRowCount() throws SQLException {
    // Get row count for first chunk if it exists.
    long totalRowCount = firstChunkRowCount;

    // Get row count from chunk file metadata
    for (ChunkFileMetadata chunkFileMetadata : chunkFileMetadatas) {
      totalRowCount += chunkFileMetadata.rowCount;
    }

    return totalRowCount;
  }

  /**
   * Retrieve compressed data size in the the ResultSet Serializable object.
   *
   * <p>GS sends the data of first chunk and metadata of the other chunks if exist to client, so
   * this function calculates the data size for all of them. NOTE: if first chunk exists, this
   * function uses its uncompressed data size as its compressed data size in this calculation though
   * it is not compressed.
   *
   * @return the total compressed data size in bytes from metadata
   */
  public long getCompressedDataSizeInBytes() throws SQLException {
    long totalCompressedDataSize = 0;

    // Count the data size for the first chunk if it exists.
    if (firstChunkStringData != null) {
      totalCompressedDataSize += firstChunkStringData.length();
    }

    for (ChunkFileMetadata chunkFileMetadata : chunkFileMetadatas) {
      totalCompressedDataSize += chunkFileMetadata.compressedByteSize;
    }

    return totalCompressedDataSize;
  }

  /**
   * Retrieve Uncompressed data size in the the ResultSet Serializable object.
   *
   * <p>GS sends the data of first chunk and metadata of the other chunk if exist to client, so this
   * function calculates the data size for all of them.
   *
   * @return the total uncompressed data size in bytes from metadata
   */
  public long getUncompressedDataSizeInBytes() throws SQLException {
    long totalUncompressedDataSize = 0;

    // Count the data size for the first chunk if it exists.
    if (firstChunkStringData != null) {
      totalUncompressedDataSize += firstChunkStringData.length();
    }

    for (ChunkFileMetadata chunkFileMetadata : chunkFileMetadatas) {
      totalUncompressedDataSize += chunkFileMetadata.uncompressedByteSize;
    }

    return totalUncompressedDataSize;
  }

  public String toString() {
    StringBuilder builder = new StringBuilder(16 * 1024);

    builder.append("hasFirstChunk: ").append(this.firstChunkStringData != null).append("\n");

    builder.append("RowCountInFirstChunk: ").append(this.firstChunkRowCount).append("\n");

    builder.append("queryResultFormat: ").append(this.queryResultFormat).append("\n");

    builder.append("chunkFileCount: ").append(this.chunkFileCount).append("\n");

    for (ChunkFileMetadata chunkFileMetadata : chunkFileMetadatas) {
      builder.append("\t").append(chunkFileMetadata.toString()).append("\n");
    }

    return builder.toString();
  }
}
