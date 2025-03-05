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

  @Override
  public void postProcess(String queryId, String sqlState, int vendorCode, Throwable ex) {}
}
