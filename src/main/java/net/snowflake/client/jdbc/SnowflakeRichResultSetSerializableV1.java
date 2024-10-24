package net.snowflake.client.jdbc;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import net.snowflake.client.core.QueryResultFormat;
import net.snowflake.client.core.SFBaseSession;
import net.snowflake.client.core.SFBaseStatement;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.log.ArgSupplier;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

@SnowflakeJdbcInternalApi
public class SnowflakeRichResultSetSerializableV1 extends SnowflakeResultSetSerializableV1 {

  private static final SFLogger logger =
      SFLoggerFactory.getLogger(SnowflakeRichResultSetSerializableV1.class);

  private String richResultsFirstChunkStringData;
  private int richResultsFirstChunkRowCount;
  private int richResultsChunkFileCount;
  private int richResultsColumnCount;
  private final List<ChunkFileMetadata> richResultsChunkFilesMetadata = new ArrayList<>();
  private byte[] richResultsFirstChunkByteData;
  private String richResultsQrmk;
  private final Map<String, String> richResultsChunkHeadersMap = new HashMap<>();
  private final List<SnowflakeRichResultsColumnMetadata> richResultsColumnMetadata =
      new ArrayList<>();
  private QueryResultFormat richResultsQueryResultFormat;

  transient JsonNode richResultFirstChunkRowset = null;

  /**
   * A factory function for internal usage only. It creates SnowflakeRichResultSetSerializableV1
   * with NoOpChunksDownloader which disables chunks prefetch.
   *
   * @param rootNode JSON root node
   * @param sfSession SFBaseSession
   * @param sfStatement SFBaseStatement
   * @return SnowflakeRichResultSetSerializableV1 with NoOpChunksDownloader
   * @throws SnowflakeSQLException if an error occurs
   */
  public static SnowflakeRichResultSetSerializableV1 createWithChunksPrefetchDisabled(
      JsonNode rootNode, SFBaseSession sfSession, SFBaseStatement sfStatement)
      throws SnowflakeSQLException {
    return new SnowflakeRichResultSetSerializableV1(
        rootNode, sfSession, sfStatement, new DefaultResultStreamProvider(), true);
  }

  private SnowflakeRichResultSetSerializableV1(
      JsonNode rootNode,
      SFBaseSession sfSession,
      SFBaseStatement sfStatement,
      ResultStreamProvider resultStreamProvider,
      boolean disableChunksPrefetch)
      throws SnowflakeSQLException {
    super(rootNode, sfSession, sfStatement, resultStreamProvider, disableChunksPrefetch);
    if (!rootNode.at("/richResult").isMissingNode()) {
      JsonNode richResultsNode = rootNode.path("richResult");
      Optional<QueryResultFormat> queryResultFormat =
          QueryResultFormat.lookupByName(richResultsNode.path("queryResultFormat").asText());
      this.richResultsQueryResultFormat = queryResultFormat.orElse(QueryResultFormat.JSON);
      initializeColumnMetadata(richResultsNode, sfSession);
      initializeFirstChunkData(richResultsNode, this.queryResultFormat);
      initializeChunkFiles(richResultsNode);
    } else {
      logger.debug("Unable to initialize rich results metadata, no \"richResult\" node");
    }
  }

  private void initializeColumnMetadata(JsonNode richResultsNode, SFBaseSession sfSession)
      throws SnowflakeSQLException {
    this.richResultsColumnCount = richResultsNode.path("rowtype").size();
    for (int i = 0; i < this.richResultsColumnCount; i++) {
      JsonNode colNode = richResultsNode.path("rowtype").path(i);

      SnowflakeRichResultsColumnMetadata columnMetadata =
          new SnowflakeRichResultsColumnMetadata(
              colNode, sfSession.isJdbcTreatDecimalAsInt(), sfSession);

      this.richResultsColumnMetadata.add(columnMetadata);
      logger.debug("Get column metadata: {}", (ArgSupplier) columnMetadata::toString);
    }
  }

  private void initializeFirstChunkData(
      JsonNode richResultsNode, QueryResultFormat queryResultFormat) {
    if (queryResultFormat == QueryResultFormat.ARROW) {
      this.richResultsFirstChunkStringData = richResultsNode.path("rowsetBase64").asText();
    } else {
      this.richResultFirstChunkRowset = richResultsNode.path("rowset");

      if (this.richResultFirstChunkRowset == null
          || this.richResultFirstChunkRowset.isMissingNode()) {
        this.richResultsFirstChunkRowCount = 0;
        this.richResultsFirstChunkStringData = null;
        this.richResultsFirstChunkByteData = new byte[0];
      } else {
        this.richResultsFirstChunkRowCount = this.richResultFirstChunkRowset.size();
        this.richResultsFirstChunkStringData = this.richResultFirstChunkRowset.toString();
      }
      logger.debug("First rich results chunk row count: {}", this.richResultsFirstChunkRowCount);
    }
  }

  private void initializeChunkFiles(JsonNode richResultsNode) {
    JsonNode chunksNode = richResultsNode.path("chunks");
    if (!chunksNode.isMissingNode()) {
      this.richResultsChunkFileCount = chunksNode.size();
      JsonNode qrmkNode = richResultsNode.path("qrmk");
      this.richResultsQrmk = qrmkNode.isMissingNode() ? null : qrmkNode.textValue();
      if (this.richResultsChunkFileCount > 0) {
        logger.debug("Number of rich results metadata chunks: {}", this.richResultsChunkFileCount);
        initializeChunkHeaders(richResultsNode);
        initializeChunkFilesMetadata(chunksNode);
      }
    }
  }

  private void initializeChunkHeaders(JsonNode richResultsNode) {
    JsonNode chunkHeaders = richResultsNode.path("chunkHeaders");
    if (chunkHeaders != null && !chunkHeaders.isMissingNode()) {
      Iterator<Map.Entry<String, JsonNode>> chunkHeadersIter = chunkHeaders.fields();
      while (chunkHeadersIter.hasNext()) {
        Map.Entry<String, JsonNode> chunkHeader = chunkHeadersIter.next();
        logger.debug(
            "Add header key: {}, value: {}", chunkHeader.getKey(), chunkHeader.getValue().asText());
        this.richResultsChunkHeadersMap.put(chunkHeader.getKey(), chunkHeader.getValue().asText());
      }
    }
  }

  private void initializeChunkFilesMetadata(JsonNode chunksNode) {
    for (int idx = 0; idx < this.richResultsChunkFileCount; idx++) {
      JsonNode chunkNode = chunksNode.get(idx);
      String url = chunkNode.path("url").asText();
      int rowCount = chunkNode.path("rowCount").asInt();
      int compressedSize = chunkNode.path("compressedSize").asInt();
      int uncompressedSize = chunkNode.path("uncompressedSize").asInt();
      this.richResultsChunkFilesMetadata.add(
          new ChunkFileMetadata(url, rowCount, compressedSize, uncompressedSize));
      logger.debug(
          "Add rich results metadata chunk, url: {} rowCount: {} "
              + "compressedSize: {} uncompressedSize: {}",
          url,
          rowCount,
          compressedSize,
          uncompressedSize);
    }
  }

  public String getRichResultsFirstChunkStringData() {
    return richResultsFirstChunkStringData;
  }

  public int getRichResultsFirstChunkRowCount() {
    return richResultsFirstChunkRowCount;
  }

  public int getRichResultsChunkFileCount() {
    return richResultsChunkFileCount;
  }

  public int getRichResultsColumnCount() {
    return richResultsColumnCount;
  }

  public List<ChunkFileMetadata> getRichResultsChunkFilesMetadata() {
    return richResultsChunkFilesMetadata;
  }

  public byte[] getRichResultsFirstChunkByteData() {
    return richResultsFirstChunkByteData;
  }

  public String getRichResultsQrmk() {
    return richResultsQrmk;
  }

  public Map<String, String> getRichResultsChunkHeadersMap() {
    return richResultsChunkHeadersMap;
  }

  public List<SnowflakeRichResultsColumnMetadata> getRichResultsColumnMetadata() {
    return richResultsColumnMetadata;
  }

  public QueryResultFormat getRichResultsQueryResultFormat() {
    return richResultsQueryResultFormat;
  }

  public JsonNode getRichResultFirstChunkRowset() {
    return richResultFirstChunkRowset;
  }

  @SnowflakeJdbcInternalApi
  public static class SnowflakeRichResultsColumnMetadata extends SnowflakeColumnMetadata {

    private final int columnIndex;

    public SnowflakeRichResultsColumnMetadata(
        JsonNode colNode, boolean jdbcTreatDecimalAsInt, SFBaseSession session)
        throws SnowflakeSQLLoggedException {
      super(colNode, jdbcTreatDecimalAsInt, session);
      this.columnIndex = colNode.path("columnIndexing").asInt();
    }

    public int getColumnIndex() {
      return columnIndex;
    }

    @Override
    public String toString() {
      return super.toString() + ",columnIndex=" + columnIndex;
    }
  }
}
