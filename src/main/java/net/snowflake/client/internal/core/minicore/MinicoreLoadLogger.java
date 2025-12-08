package net.snowflake.client.internal.core.minicore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;

@SnowflakeJdbcInternalApi
public class MinicoreLoadLogger {

  private final long startTimeNanos;
  private final List<String> logs;

  public MinicoreLoadLogger() {
    this.startTimeNanos = System.nanoTime();
    this.logs = new ArrayList<>();
  }

  public void log(String message) {
    long elapsedNanos = System.nanoTime() - startTimeNanos;
    double elapsedMs = elapsedNanos / 1_000_000.0;
    String timestampedMessage = String.format("[%.6fms] %s", elapsedMs, message);
    logs.add(timestampedMessage);
  }

  public List<String> getLogs() {
    return Collections.unmodifiableList(logs);
  }
}
