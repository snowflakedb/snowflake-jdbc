package net.snowflake.client.jdbc.telemetry;

import java.util.concurrent.Future;

public interface Telemetry {
  /**
   * Attempt to add log to batch, and suppress exceptions thrown in case of failure
   *
   * @param log entry to add
   */
  void addLogToBatch(TelemetryData log);

  /** Close telemetry connector and send any unsubmitted logs */
  void close();

  /**
   * Send all cached logs to server
   *
   * @return future indicating whether the logs were sent successfully
   */
  Future<Boolean> sendBatchAsync();

  /**
   * A hook for post-processing after sending telemetry data. Can be used, for example, for
   * additional error handling.
   *
   * @param queryId The query id
   * @param sqlState The SQL state as defined in net.snowflake.common.core.SqlState
   * @param vendorCode The vendor code for localized messages
   * @param ex The throwable that caused this.
   */
  void postProcess(String queryId, String sqlState, int vendorCode, Throwable ex);
}
