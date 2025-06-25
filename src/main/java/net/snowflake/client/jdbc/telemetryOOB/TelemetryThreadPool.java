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

  /**
   * Private constructor to initialize the singleton instance.
   *
   * <p>Configures a thread pool that scales dynamically based on workload. The pool starts with
   * zero threads and will create new threads on demand up to a maximum of 10. If all 10 threads are
   * active, new tasks are placed in an unbounded queue to await execution.
   *
   * <p>To conserve resources, threads that are idle for more than 30 seconds are terminated,
   * allowing the pool to shrink back to zero during periods of inactivity.
   */
  private TelemetryThreadPool() {
    uploader =
        new ThreadPoolExecutor(
            10, // core size
            10, // max size
            30L, // keep alive time
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>() // work queue
            );
    // Allow core threads to time out and be terminated when idle.
    ((ThreadPoolExecutor) uploader).allowCoreThreadTimeOut(true);
  }

  public void execute(Runnable task) {
    uploader.execute(task);
  }

  public <T> Future<T> submit(Callable<T> task) {
    return uploader.submit(task);
  }
}
