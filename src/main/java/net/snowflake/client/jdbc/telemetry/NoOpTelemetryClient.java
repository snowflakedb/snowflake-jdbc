/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.jdbc.telemetry;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/** Telemetry client that is doing nothing. Mainly used in testing code */
public class NoOpTelemetryClient implements Telemetry {
  @Override
  public void addLogToBatch(TelemetryData log) {}

  @Override
  public void close() {}

  @Override
  public Future<Boolean> sendBatchAsync() {
    return CompletableFuture.completedFuture(true);
  }
}
