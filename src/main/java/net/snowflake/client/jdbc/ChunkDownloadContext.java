package net.snowflake.client.jdbc;

import java.util.Map;
import net.snowflake.client.core.SFBaseSession;

/**
 * Simple struct to contain download context for a chunk. This is useful to organize the collection
 * of properties that may be used for containing download information, and allows for the
 * getInputStream() method to be overridden.
 */
public class ChunkDownloadContext {
  private final SnowflakeChunkDownloader chunkDownloader;

  public SnowflakeChunkDownloader getChunkDownloader() {
    return chunkDownloader;
  }

  public SnowflakeResultChunk getResultChunk() {
    return resultChunk;
  }

  public String getQrmk() {
    return qrmk;
  }

  public int getChunkIndex() {
    return chunkIndex;
  }

  public Map<String, String> getChunkHeadersMap() {
    return chunkHeadersMap;
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

  public SFBaseSession getSession() {
    return session;
  }

  private final SnowflakeResultChunk resultChunk;
  private final String qrmk;
  private final int chunkIndex;
  private final Map<String, String> chunkHeadersMap;
  private final int networkTimeoutInMilli;
  private final int authTimeout;
  private final int socketTimeout;
  private final int maxHttpRetries;
  private final SFBaseSession session;

  public ChunkDownloadContext(
      SnowflakeChunkDownloader chunkDownloader,
      SnowflakeResultChunk resultChunk,
      String qrmk,
      int chunkIndex,
      Map<String, String> chunkHeadersMap,
      int networkTimeoutInMilli,
      int authTimeout,
      int socketTimeout,
      int maxHttpRetries,
      SFBaseSession session) {
    this.chunkDownloader = chunkDownloader;
    this.resultChunk = resultChunk;
    this.qrmk = qrmk;
    this.chunkIndex = chunkIndex;
    this.chunkHeadersMap = chunkHeadersMap;
    this.networkTimeoutInMilli = networkTimeoutInMilli;
    this.authTimeout = authTimeout;
    this.socketTimeout = socketTimeout;
    this.maxHttpRetries = maxHttpRetries;
    this.session = session;
  }
}
