package net.snowflake.client.core;

/** Metrics related to chunk downloader performance */
public class DownloaderMetrics {
  /** time in millis that main thread is blocked and waits for chunk is ready */
  private final long millisWaiting;

  /** time in millis that background thread is downloading the data */
  private final long millisDownloading;

  /** time in millis that background thread is parsing data */
  private final long millisParsing;

  public DownloaderMetrics(long millisWaiting, long millisDownloading, long millisParsing) {
    this.millisWaiting = millisWaiting;
    this.millisDownloading = millisDownloading;
    this.millisParsing = millisParsing;
  }

  long getMillisWaiting() {
    return millisWaiting;
  }

  long getMillisDownloading() {
    return millisDownloading;
  }

  long getMillisParsing() {
    return millisParsing;
  }
}
