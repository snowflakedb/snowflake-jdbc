/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
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
}
