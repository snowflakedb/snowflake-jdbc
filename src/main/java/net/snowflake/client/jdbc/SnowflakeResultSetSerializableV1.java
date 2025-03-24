package net.snowflake.client.jdbc;

import static net.snowflake.client.core.Constants.GB;
import static net.snowflake.client.core.Constants.MB;
import static net.snowflake.client.core.SessionUtil.CLIENT_ENABLE_CONSERVATIVE_MEMORY_USAGE;
import static net.snowflake.client.core.SessionUtil.CLIENT_MEMORY_LIMIT;
import static net.snowflake.client.core.SessionUtil.CLIENT_PREFETCH_THREADS;
import static net.snowflake.client.core.SessionUtil.CLIENT_RESULT_CHUNK_SIZE;
import static net.snowflake.client.core.SessionUtil.DEFAULT_CLIENT_MEMORY_LIMIT;
import static net.snowflake.client.core.SessionUtil.DEFAULT_CLIENT_PREFETCH_THREADS;
import static net.snowflake.client.jdbc.SnowflakeChunkDownloader.NoOpChunkDownloader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.ClosedByInterruptException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.TimeZone;
import net.snowflake.client.core.ChunkDownloader;
import net.snowflake.client.core.HttpClientSettingsKey;
import net.snowflake.client.core.MetaDataOfBinds;
import net.snowflake.client.core.OCSPMode;
import net.snowflake.client.core.ObjectMapperFactory;
import net.snowflake.client.core.QueryResultFormat;
import net.snowflake.client.core.ResultUtil;
import net.snowflake.client.core.SFArrowResultSet;
import net.snowflake.client.core.SFBaseResultSet;
import net.snowflake.client.core.SFBaseSession;
import net.snowflake.client.core.SFBaseStatement;
import net.snowflake.client.core.SFResultSet;
import net.snowflake.client.core.SFResultSetMetaData;
import net.snowflake.client.core.SFSession;
import net.snowflake.client.core.SFStatementType;
import net.snowflake.client.core.SessionUtil;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
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

  private static final SFLogger logger =
      SFLoggerFactory.getLogger(SnowflakeResultSetSerializableV1.class);

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
  byte[] firstChunkByteData;

  // below fields are used for building a ChunkDownloader which
  // uses http client to download chunk files
  int resultPrefetchThreads;
  String qrmk;
  Map<String, String> chunkHeadersMap = new HashMap<>();
  // Below fields are from session or statement
  SnowflakeConnectString snowflakeConnectionString;
  OCSPMode ocspMode;
  HttpClientSettingsKey httpClientKey;
  int networkTimeoutInMilli;
  int authTimeout;
  int socketTimeout;
  int maxHttpRetries;
  boolean isResultColumnCaseInsensitive;
  int resultSetType;
  int resultSetConcurrency;
  int resultSetHoldability;
  boolean treatNTZAsUTC;
  boolean formatDateWithTimezone;
  boolean useSessionTimezone;
  boolean getDateUseNullTimezone;

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
  int sessionClientMemoryLimit;

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
  transient ResultStreamProvider resultStreamProvider = new DefaultResultStreamProvider();

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
    this.firstChunkByteData = toCopy.firstChunkByteData;

    // below fields are used for building a ChunkDownloader
    this.resultPrefetchThreads = toCopy.resultPrefetchThreads;
    this.qrmk = toCopy.qrmk;
    this.chunkHeadersMap = toCopy.chunkHeadersMap;

    // Below fields are from session or statement
    this.snowflakeConnectionString = toCopy.snowflakeConnectionString;
    this.ocspMode = toCopy.ocspMode;
    this.httpClientKey = toCopy.httpClientKey;
    this.networkTimeoutInMilli = toCopy.networkTimeoutInMilli;
    this.authTimeout = toCopy.authTimeout;
    this.socketTimeout = toCopy.socketTimeout;
    this.maxHttpRetries = toCopy.maxHttpRetries;
    this.isResultColumnCaseInsensitive = toCopy.isResultColumnCaseInsensitive;
    this.resultSetType = toCopy.resultSetType;
    this.resultSetConcurrency = toCopy.resultSetConcurrency;
    this.resultSetHoldability = toCopy.resultSetHoldability;
    this.treatNTZAsUTC = toCopy.treatNTZAsUTC;
    this.formatDateWithTimezone = toCopy.formatDateWithTimezone;
    this.useSessionTimezone = toCopy.useSessionTimezone;
    this.getDateUseNullTimezone = toCopy.getDateUseNullTimezone;

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
    this.resultStreamProvider = toCopy.resultStreamProvider;
  }

  /**
   * @param rootNode result JSON node received from GS
   * @param sfSession the Snowflake session
   * @param sfStatement the Snowflake statement
   * @param resultStreamProvider a ResultStreamProvider for computing a custom data source for
   *     result-file streams
   * @param disableChunksPrefetch is prefetch disabled
   * @throws SnowflakeSQLException if failed to parse the result JSON node
   */
  protected SnowflakeResultSetSerializableV1(
      JsonNode rootNode,
      SFBaseSession sfSession,
      SFBaseStatement sfStatement,
      ResultStreamProvider resultStreamProvider,
      boolean disableChunksPrefetch)
      throws SnowflakeSQLException {
    SnowflakeUtil.checkErrorAndThrowException(rootNode);

    // get the query id
    this.queryId = rootNode.path("data").path("queryId").asText();

    JsonNode databaseNode = rootNode.path("data").path("finalDatabaseName");
    this.finalDatabaseName =
        databaseNode.isNull()
            ? (sfSession != null ? sfSession.getDatabase() : null)
            : databaseNode.asText();

    JsonNode schemaNode = rootNode.path("data").path("finalSchemaName");
    this.finalSchemaName =
        schemaNode.isNull()
            ? (sfSession != null ? sfSession.getSchema() : null)
            : schemaNode.asText();

    JsonNode roleNode = rootNode.path("data").path("finalRoleName");
    this.finalRoleName =
        roleNode.isNull() ? (sfSession != null ? sfSession.getRole() : null) : roleNode.asText();

    JsonNode warehouseNode = rootNode.path("data").path("finalWarehouseName");
    this.finalWarehouseName =
        warehouseNode.isNull()
            ? (sfSession != null ? sfSession.getWarehouse() : null)
            : warehouseNode.asText();

    this.statementType =
        SFStatementType.lookUpTypeById(rootNode.path("data").path("statementTypeId").asLong());

    this.totalRowCountTruncated = rootNode.path("data").path("totalTruncated").asBoolean();

    this.possibleSession = Optional.ofNullable(sfSession);

    logger.debug("Query id: {}", this.queryId);

    Optional<QueryResultFormat> queryResultFormat =
        QueryResultFormat.lookupByName(rootNode.path("data").path("queryResultFormat").asText());
    this.queryResultFormat = queryResultFormat.orElse(QueryResultFormat.JSON);

    // extract query context and save it in current session
    JsonNode queryContextNode = rootNode.path("data").path("queryContext");
    String queryContext = queryContextNode.isNull() ? null : queryContextNode.toString();

    if (!sfSession.isAsyncSession()) {
      sfSession.setQueryContext(queryContext);
    }

    // extract parameters
    this.parameters = SessionUtil.getCommonParams(rootNode.path("data").path("parameters"));
    if (this.parameters.isEmpty()) {
      this.parameters = new HashMap<>(sfSession.getCommonParameters());
      this.setStatemementLevelParameters(sfStatement.getStatementParameters());
    }

    // initialize column metadata
    this.columnCount = rootNode.path("data").path("rowtype").size();

    for (int i = 0; i < this.columnCount; i++) {
      JsonNode colNode = rootNode.path("data").path("rowtype").path(i);

      SnowflakeColumnMetadata columnMetadata =
          new SnowflakeColumnMetadata(colNode, sfSession.isJdbcTreatDecimalAsInt(), sfSession);

      this.resultColumnMetadata.add(columnMetadata);

      logger.debug("Get column metadata: {}", (ArgSupplier) columnMetadata::toString);
    }

    this.resultStreamProvider = resultStreamProvider;

    // process the content of first chunk.
    if (this.queryResultFormat == QueryResultFormat.ARROW) {
      this.firstChunkStringData = rootNode.path("data").path("rowsetBase64").asText();
      this.rootAllocator = new RootAllocator(Long.MAX_VALUE);
      // Set first chunk row count from firstChunkStringData
      this.setFirstChunkRowCountForArrow();
    } else {
      this.firstChunkRowset = rootNode.path("data").path("rowset");

      if (this.firstChunkRowset == null || this.firstChunkRowset.isMissingNode()) {
        this.firstChunkRowCount = 0;
        this.firstChunkStringData = null;
        this.firstChunkByteData = new byte[0];
      } else {
        this.firstChunkRowCount = this.firstChunkRowset.size();
        this.firstChunkStringData = this.firstChunkRowset.toString();
      }
    }
    logger.debug("First chunk row count: {}", this.firstChunkRowCount);

    // parse file chunks
    this.parseChunkFiles(rootNode, sfStatement);

    // result version
    JsonNode versionNode = rootNode.path("data").path("version");

    if (!versionNode.isMissingNode()) {
      this.resultVersion = versionNode.longValue();
    }

    // number of binds
    JsonNode numberOfBindsNode = rootNode.path("data").path("numberOfBinds");

    if (!numberOfBindsNode.isMissingNode()) {
      this.numberOfBinds = numberOfBindsNode.intValue();
    }

    JsonNode arrayBindSupported = rootNode.path("data").path("arrayBindSupported");
    this.arrayBindSupported = !arrayBindSupported.isMissingNode() && arrayBindSupported.asBoolean();

    // time result sent by GS (epoch time in millis)
    JsonNode sendResultTimeNode = rootNode.path("data").path("sendResultTime");
    if (!sendResultTimeNode.isMissingNode()) {
      this.sendResultTime = sendResultTimeNode.longValue();
    }

    logger.debug("Result version: {}", this.resultVersion);

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
      this.metaDataOfBinds = returnVal;
    }

    // setup fields from sessions.
    this.ocspMode = sfSession.getOCSPMode();
    this.httpClientKey = sfSession.getHttpClientKey();
    this.snowflakeConnectionString = sfSession.getSnowflakeConnectionString();
    this.networkTimeoutInMilli = sfSession.getNetworkTimeoutInMilli();
    this.authTimeout = 0;
    this.maxHttpRetries = sfSession.getMaxHttpRetries();
    this.isResultColumnCaseInsensitive = sfSession.isResultColumnCaseInsensitive();
    this.treatNTZAsUTC = sfSession.getTreatNTZAsUTC();
    this.formatDateWithTimezone = sfSession.getFormatDateWithTimezone();
    this.useSessionTimezone = sfSession.getUseSessionTimezone();
    this.getDateUseNullTimezone = sfSession.getGetDateUseNullTimezone();

    // setup transient fields from parameter
    this.setupFieldsFromParameters();

    if (disableChunksPrefetch) {
      this.chunkDownloader = new NoOpChunkDownloader();
    } else {
      this.chunkDownloader =
          (this.chunkFileCount > 0)
              // The chunk downloader will start prefetching
              // first few chunk files in background thread(s)
              ? new SnowflakeChunkDownloader(this)
              : new NoOpChunkDownloader();
    }

    // Setup ResultSet metadata
    this.resultSetMetaData =
        new SFResultSetMetaData(
            this.getResultColumnMetadata(),
            this.queryId,
            sfSession,
            this.isResultColumnCaseInsensitive,
            this.timestampNTZFormatter,
            this.timestampLTZFormatter,
            this.timestampTZFormatter,
            this.dateFormatter,
            this.timeFormatter);
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

  public void setFirstChunkStringData(String firstChunkStringData) {
    this.firstChunkStringData = firstChunkStringData;
  }

  public void setFirstChunkByteData(byte[] firstChunkByteData) {
    this.firstChunkByteData = firstChunkByteData;
  }

  public void setChunkDownloader(ChunkDownloader chunkDownloader) {
    this.chunkDownloader = chunkDownloader;
  }

  public void setResultStreamProvider(ResultStreamProvider resultStreamProvider) {
    this.resultStreamProvider = resultStreamProvider;
  }

  public ResultStreamProvider getResultStreamProvider() {
    return resultStreamProvider;
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

  public HttpClientSettingsKey getHttpClientKey() {
    return httpClientKey;
  }

  public String getQrmk() {
    return qrmk;
  }

  public int getNetworkTimeoutInMilli() {
    return networkTimeoutInMilli;
  }

  public int getAuthTimeout() {
    return authTimeout;
  }

  public int getSocketTimeout() {
    return socketTimeout;
  }

  public int getMaxHttpRetries() {
    return maxHttpRetries;
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

  public byte[] getFirstChunkByteData() {
    return firstChunkByteData;
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

  public boolean getGetDateUseNullTimezone() {
    return getDateUseNullTimezone;
  }

  public Optional<SFBaseSession> getSession() {
    return possibleSession;
  }

  /**
   * A factory function to create SnowflakeResultSetSerializable object from result JSON node, using
   * the DefaultResultStreamProvider.
   *
   * @param rootNode result JSON node received from GS
   * @param sfSession the Snowflake session
   * @param sfStatement the Snowflake statement
   * @return processed ResultSetSerializable object
   * @throws SnowflakeSQLException if failed to parse the result JSON node
   */
  public static SnowflakeResultSetSerializableV1 create(
      JsonNode rootNode, SFBaseSession sfSession, SFBaseStatement sfStatement)
      throws SnowflakeSQLException {
    return create(rootNode, sfSession, sfStatement, new DefaultResultStreamProvider());
  }

  /**
   * A factory function to create SnowflakeResultSetSerializable object from result JSON node, with
   * an overridable ResultStreamProvider.
   *
   * @param rootNode result JSON node received from GS
   * @param sfSession the Snowflake session
   * @param sfStatement the Snowflake statement
   * @param resultStreamProvider a ResultStreamProvider for computing a custom data source for
   *     result-file streams
   * @return processed ResultSetSerializable object
   * @throws SnowflakeSQLException if failed to parse the result JSON node
   */
  public static SnowflakeResultSetSerializableV1 create(
      JsonNode rootNode,
      SFBaseSession sfSession,
      SFBaseStatement sfStatement,
      ResultStreamProvider resultStreamProvider)
      throws SnowflakeSQLException {
    logger.trace("Entering create()", false);
    return new SnowflakeResultSetSerializableV1(
        rootNode, sfSession, sfStatement, resultStreamProvider, false);
  }

  /**
   * A factory function for internal usage only. It creates SnowflakeResultSetSerializableV1 with
   * NoOpChunksDownloader which disables chunks prefetch.
   *
   * @param rootNode JSON root node
   * @param sfSession SFBaseSession
   * @param sfStatement SFBaseStatement
   * @return SnowflakeResultSetSerializableV1 with NoOpChunksDownloader
   * @throws SnowflakeSQLException if an error occurs
   */
  @SnowflakeJdbcInternalApi
  public static SnowflakeResultSetSerializableV1 createWithChunksPrefetchDisabled(
      JsonNode rootNode, SFBaseSession sfSession, SFBaseStatement sfStatement)
      throws SnowflakeSQLException {
    logger.trace("Entering create()", false);
    return new SnowflakeResultSetSerializableV1(
        rootNode, sfSession, sfStatement, new DefaultResultStreamProvider(), true);
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
        "Sql date format: {}, java date format: {}",
        sqlDateFormat,
        (ArgSupplier) () -> this.dateFormatter.toSimpleDateTimePattern());

    String sqlTimeFormat =
        (String) ResultUtil.effectiveParamValue(this.parameters, "TIME_OUTPUT_FORMAT");

    this.timeFormatter = SnowflakeDateTimeFormat.fromSqlFormat(sqlTimeFormat);

    logger.debug(
        "Sql time format: {}, java time format: {}",
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
  private void parseChunkFiles(JsonNode rootNode, SFBaseStatement sfStatement) {
    JsonNode chunksNode = rootNode.path("data").path("chunks");

    if (!chunksNode.isMissingNode()) {
      this.chunkFileCount = chunksNode.size();

      // Try to get the Query Result Master Key
      JsonNode qrmkNode = rootNode.path("data").path("qrmk");
      this.qrmk = qrmkNode.isMissingNode() ? null : qrmkNode.textValue();

      // Determine the prefetch thread count and memoryLimit
      if (this.chunkFileCount > 0) {
        logger.debug("#chunks: {}, initialize chunk downloader", this.chunkFileCount);

        adjustMemorySettings(sfStatement);

        // Parse chunk header
        JsonNode chunkHeaders = rootNode.path("data").path("chunkHeaders");
        if (chunkHeaders != null && !chunkHeaders.isMissingNode()) {
          Iterator<Map.Entry<String, JsonNode>> chunkHeadersIter = chunkHeaders.fields();

          while (chunkHeadersIter.hasNext()) {
            Map.Entry<String, JsonNode> chunkHeader = chunkHeadersIter.next();

            logger.debug(
                "Add header key: {}, value: {}",
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
              "Add chunk, url: {} rowCount: {} " + "compressedSize: {} uncompressedSize: {}",
              url,
              rowCount,
              compressedSize,
              uncompressedSize);
        }
      }
    }
  }

  private void adjustMemorySettings(SFBaseStatement sfStatement) {
    this.resultPrefetchThreads = DEFAULT_CLIENT_PREFETCH_THREADS;
    if (this.statementType.isSelect()
        && this.parameters.containsKey(CLIENT_ENABLE_CONSERVATIVE_MEMORY_USAGE)
        && (boolean) this.parameters.get(CLIENT_ENABLE_CONSERVATIVE_MEMORY_USAGE)) {
      // use conservative memory settings
      this.resultPrefetchThreads = sfStatement.getConservativePrefetchThreads();
      this.memoryLimit = sfStatement.getConservativeMemoryLimit();
      int chunkSize = (int) this.parameters.get(CLIENT_RESULT_CHUNK_SIZE);
      logger.debug(
          "Enable conservative memory usage with prefetchThreads: {} and memoryLimit: {} and "
              + "resultChunkSize: {}",
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
    if (sfStatement.getSFBaseSession().getMemoryLimitForTesting()
        != SFBaseSession.MEMORY_LIMIT_UNSET) {
      memoryLimit = sfStatement.getSFBaseSession().getMemoryLimitForTesting();
      logger.debug("memoryLimit changed for testing purposes to {}", memoryLimit);
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
    long maxMemoryToUse = Runtime.getRuntime().maxMemory() * 8 / 10;
    if (parameters.get(CLIENT_MEMORY_LIMIT) != null) {
      // use the settings from the customer
      memoryLimit = (int) parameters.get(CLIENT_MEMORY_LIMIT) * 1024L * 1024L;

      if (DEFAULT_CLIENT_MEMORY_LIMIT == (int) parameters.get(CLIENT_MEMORY_LIMIT)) {
        // if the memory limit is the default value and best effort memory is enabled
        // set the memory limit to 80% of the maximum as the best effort
        memoryLimit = Math.max(memoryLimit, maxMemoryToUse);
      }
    }

    // always make sure memoryLimit <= 80% of the maximum
    memoryLimit = Math.min(memoryLimit, maxMemoryToUse);

    logger.debug("Set allowed memory usage to {} bytes", memoryLimit);
    return memoryLimit;
  }

  /**
   * If statement parameter values are available, set those values in the resultset list of
   * parameters so they overwrite the session-level cached parameter values.
   *
   * @param stmtParamsMap
   */
  private void setStatemementLevelParameters(Map<String, Object> stmtParamsMap) {
    for (Map.Entry<String, Object> entry : stmtParamsMap.entrySet()) {
      this.parameters.put(entry.getKey(), entry.getValue());
    }
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

    this.resultStreamProvider = new DefaultResultStreamProvider();

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
            queryId,
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
        (this.chunkFileCount > 0) ? new SnowflakeChunkDownloader(this) : new NoOpChunkDownloader();

    this.possibleSession = Optional.empty(); // we don't have session object during deserializing
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
          queryId,
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
        curResultSetSerializable.firstChunkByteData = new byte[0];
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
          queryId,
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
   * <p>This API is used by spark connector from 2.6.0 to 2.8.1. It is deprecated from
   * sc:2.8.2/jdbc:3.12.12 since Sept 2020. It is safe to remove it after Sept 2022.
   *
   * @return a ResultSet which represents for the data wrapped in the object
   * @deprecated Use {@link #getResultSet(ResultSetRetrieveConfig)} instead
   */
  @Deprecated
  public ResultSet getResultSet() throws SQLException {
    return getResultSetInternal(null);
  }

  /**
   * Get ResultSet from the ResultSet Serializable object so that the user can access the data.
   *
   * <p>This API is used by spark connector from 2.6.0 to 2.8.1. It is deprecated from
   * sc:2.8.2/jdbc:3.12.12 since Sept 2020. It is safe to remove it after Sept 2022.
   *
   * @param info The proxy sever information if proxy is necessary.
   * @return a ResultSet which represents for the data wrapped in the object
   * @deprecated Use {@link #getResultSet(ResultSetRetrieveConfig)} instead
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
    this.httpClientKey = SnowflakeUtil.convertProxyPropertiesToHttpClientKey(ocspMode, info);

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
          sfBaseResultSet =
              new SFResultSet(
                  this, getSession().orElse(new SFSession()), telemetryClient, sortResult);
          break;
        }
      default:
        throw new SnowflakeSQLLoggedException(
            queryId,
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
    firstChunkByteData = new byte[0];
    // If the first chunk doesn't exist or empty, set it as 0
    if (firstChunkStringData == null || firstChunkStringData.isEmpty()) {
      firstChunkRowCount = 0;
      firstChunkByteData = new byte[0];
    }
    // Parse the Arrow result chunk
    else if (getQueryResultFormat().equals(QueryResultFormat.ARROW)) {
      // Below code is developed based on SFArrowResultSet.buildFirstChunk
      // and ArrowResultChunk.readArrowStream()
      byte[] bytes = Base64.getDecoder().decode(firstChunkStringData);
      firstChunkByteData = bytes;
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
      } catch (ClosedByInterruptException cbie) {
        // SNOW-755756: sometimes while reading from arrow stream, this exception can occur with
        // null message.
        // Log an interrupted message instead of throwing this exception.
        logger.debug("Interrupted when loading Arrow first chunk row count.", cbie);
      } catch (Exception ex) {
        throw new SnowflakeSQLLoggedException(
            queryId,
            possibleSession.orElse(/* session = */ null),
            ErrorCode.INTERNAL_ERROR,
            "Fail to retrieve row count for first arrow chunk: " + ex.getMessage());
      } finally {
        if (root != null) {
          root.clear();
        }
      }
    } else {
      // This shouldn't happen
      throw new SnowflakeSQLLoggedException(
          queryId,
          this.possibleSession.orElse(/*session = */ null),
          ErrorCode.INTERNAL_ERROR,
          "setFirstChunkRowCountForArrow() should only be called for Arrow.");
    }
  }

  /**
   * Retrieve total row count included in the ResultSet Serializable object.
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
   * Retrieve compressed data size in the ResultSet Serializable object.
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
   * Retrieve Uncompressed data size in the ResultSet Serializable object.
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
