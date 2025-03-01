package net.snowflake.client.jdbc;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import net.snowflake.client.util.SecretDetector;

/** Class for result chunk */
public abstract class SnowflakeResultChunk {
  public boolean isReleased() {
    return released;
  }

  public void setReleased() {
    released = true;
  }

  public enum DownloadState {
    NOT_STARTED,
    IN_PROGRESS,
    SUCCESS,
    FAILURE
  }

  // url for result chunk
  private final String url;

  // url for result chunk, with any credentials present (e.g. SAS tokens)
  // masked
  private final String scrubbedUrl;

  // number of columns to expect
  final int colCount;

  // uncompressed size in bytes of this chunk
  int uncompressedSize;

  // row count
  final int rowCount;

  // download time for the chunk
  private long downloadTime;

  // parse time for the chunk
  private long parseTime;

  private DownloadState downloadState = DownloadState.NOT_STARTED;

  // lock for guarding shared chunk state between consumer and downloader
  private ReentrantLock lock = new ReentrantLock();

  // a condition to signal from downloader to consumer
  private Condition downloadCondition = lock.newCondition();

  // download error if any for the chunk
  private String downloadError;

  private boolean released = false;

  /**
   * Compute the memory necessary to store the data of this chunk
   *
   * @return necessary memory in bytes
   */
  abstract long computeNeededChunkMemory();

  /** Free the data stored in this chunk. Called when finish consuming the chunk */
  abstract void freeData();

  /** Reset all data structure used in this result chunk */
  abstract void reset();

  public SnowflakeResultChunk(String url, int rowCount, int colCount, int uncompressedSize) {
    this.url = url;
    this.scrubbedUrl = SecretDetector.maskSASToken(this.url);
    this.rowCount = rowCount;
    this.colCount = colCount;
    this.uncompressedSize = uncompressedSize;
  }

  public final String getUrl() {
    return url;
  }

  public final String getScrubbedUrl() {
    return this.scrubbedUrl;
  }

  public final int getRowCount() {
    return rowCount;
  }

  public final int getUncompressedSize() {
    return uncompressedSize;
  }

  public final int getColCount() {
    return this.colCount;
  }

  public long getDownloadTime() {
    return downloadTime;
  }

  public void setDownloadTime(long downloadTime) {
    this.downloadTime = downloadTime;
  }

  public long getParseTime() {
    return parseTime;
  }

  public void setParseTime(long parseTime) {
    this.parseTime = parseTime;
  }

  public ReentrantLock getLock() {
    return lock;
  }

  public Condition getDownloadCondition() {
    return downloadCondition;
  }

  public String getDownloadError() {
    return downloadError;
  }

  public void setDownloadError(String downloadError) {
    this.downloadError = downloadError;
  }

  public DownloadState getDownloadState() {
    return downloadState;
  }

  public void setDownloadState(DownloadState downloadState) {
    this.downloadState = downloadState;
  }

  long getTotalTime() {
    return downloadTime + parseTime;
  }
}
