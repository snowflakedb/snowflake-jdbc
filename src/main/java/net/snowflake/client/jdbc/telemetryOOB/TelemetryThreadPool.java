package net.snowflake.client.jdbc.telemetryOOB;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * A singleton class which wrapped the ExecutorService, which is used to submit telemetry data
 * asynchronously to server
 */
public class TelemetryThreadPool {
  private ExecutorService uploader;

  private static TelemetryThreadPool instance;

  public static TelemetryThreadPool getInstance() {
    if (instance == null) {
      synchronized (TelemetryThreadPool.class) {
        if (instance == null) {
          instance = new TelemetryThreadPool();
        }
      }
    }
    return instance;
  }

  private TelemetryThreadPool() {
    uploader =
        new ThreadPoolExecutor(
            0, // core size
            10, // max size
            1, // keep alive time
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>() // work queue
            );
  }

  public void execute(Runnable task) {
    uploader.execute(task);
  }

  public <T> Future<T> submit(Callable<T> task) {
    return uploader.submit(task);
  }
}
