package net.snowflake.client.jdbc.telemetry;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

/**
 * A telemetry client that buffers telemetry data until a real telemetry client becomes available.
 * Used for scenario where telemetry needs to be collected before a session is established, such as
 * during SSL/TLS setup and certificate validation.
 */
@SnowflakeJdbcInternalApi
public class PreSessionTelemetryClient implements Telemetry {
  private static final SFLogger logger = SFLoggerFactory.getLogger(PreSessionTelemetryClient.class);

  private final List<TelemetryData> bufferedData = new ArrayList<>();
  private final Lock lock = new ReentrantLock();
  private Telemetry realTelemetryClient = null;
  private boolean closed = false;

  // Prevent potential memory issues by limiting buffer size
  private static final int MAX_BUFFER_SIZE = 1000;

  @Override
  public void addLogToBatch(TelemetryData log) {
    if (closed || log == null) {
      return;
    }

    lock.lock();
    try {
      if (realTelemetryClient != null) {
        // Real client available, use it directly
        realTelemetryClient.addLogToBatch(log);
      } else {
        if (bufferedData.size() < MAX_BUFFER_SIZE) {
          bufferedData.add(log);
          logger.debug("Buffered telemetry data, buffer size: {}", bufferedData.size());
        } else {
          logger.debug(
              "Telemetry buffer full (size: {}), dropping telemetry data to prevent memory issues",
              MAX_BUFFER_SIZE);
        }
      }
    } finally {
      lock.unlock();
    }
  }

  public void setRealTelemetryClient(Telemetry realClient) {
    lock.lock();
    try {
      if (closed) {
        logger.debug("PreSessionTelemetryClient is closed, ignoring real client");
        return;
      }
      this.realTelemetryClient = realClient;
      flushBufferedData(realClient);
    } finally {
      lock.unlock();
    }
  }

  private void flushBufferedData(Telemetry realClient) {
    for (TelemetryData data : bufferedData) {
      try {
        realClient.addLogToBatch(data);
      } catch (Exception e) {
        logger.debug("Failed to flush buffered telemetry data: {}", e.getMessage());
      }
    }
    bufferedData.clear();
  }

  @Override
  public Future<Boolean> sendBatchAsync() {
    lock.lock();
    try {
      if (realTelemetryClient != null) {
        return realTelemetryClient.sendBatchAsync();
      }
      return CompletableFuture.completedFuture(true);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void close() {
    lock.lock();
    try {
      if (closed) {
        return;
      }

      closed = true;

      if (realTelemetryClient != null) {
        try {
          realTelemetryClient.close();
        } catch (Exception e) {
          logger.debug("Error closing telemetry client: {}", e.getMessage());
        }
      }

      if (!bufferedData.isEmpty()) {
        logger.debug(
            "Closing PreSessionTelemetryClient with {} unflushed entries", bufferedData.size());
      }

      bufferedData.clear();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void postProcess(String queryId, String sqlState, int vendorCode, Throwable ex) {
    lock.lock();
    try {
      if (realTelemetryClient != null) {
        realTelemetryClient.postProcess(queryId, sqlState, vendorCode, ex);
      }
    } finally {
      lock.unlock();
    }
  }

  public boolean hasRealTelemetryClient() {
    return realTelemetryClient != null;
  }
}
