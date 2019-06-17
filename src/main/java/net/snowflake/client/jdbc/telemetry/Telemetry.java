/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.jdbc.telemetry;

import java.io.IOException;

public interface Telemetry
{
  /**
   * Attempt to add log to batch, and suppress exceptions thrown in case of
   * failure
   *
   * @param log entry to add
   */
  void tryAddLogToBatch(TelemetryData log);

  /**
   * Attempt to add log to batch, and suppress exceptions thrown in case of
   * failure
   *
   * @param log entry to add
   */
  void addLogToBatch(TelemetryData log) throws IOException;

  /**
   * Close telemetry connector and send any unsubmitted logs
   *
   * @throws IOException if closed or uploading batch fails
   */
  void close() throws IOException;

  /**
   * Send all cached logs to server
   *
   * @return whether the logs were sent successfully
   * @throws IOException if closed or uploading batch fails
   */
  boolean sendBatch() throws IOException;
}
