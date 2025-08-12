package net.snowflake.client.jdbc.telemetryOOB;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.OTHERS)
class TelemetryThreadPoolTest {

  /**
   * This test confirms fixing the issue where the TelemetryThreadPool did not scale up to its
   * maximum pool size due to its configuration with a core pool size of 0 and an unbounded work
   * queue (LinkedBlockingQueue). https://github.com/snowflakedb/snowflake-jdbc/issues/2219
   *
   * <p>This test submits 100 tasks that each sleep for a short duration. If the pool were scaling,
   * it would create up to 10 threads to handle the load concurrently. Previously only a single
   * thread was ever created to process all tasks sequentially from the queue.
   *
   * @throws InterruptedException if the waiting thread is interrupted.
   */
  @Test
  void testThreadPoolScaling() throws InterruptedException {
    TelemetryThreadPool telemetryPool = TelemetryThreadPool.getInstance();
    int taskCount = 100; // Submit more tasks than the max pool size (10)
    ThreadPoolExecutor executor = getThreadPoolExecutor(telemetryPool);

    final Set<Long> uniqueThreadIds = Collections.newSetFromMap(new ConcurrentHashMap<>());
    final CountDownLatch completionLatch = new CountDownLatch(taskCount);

    // Submit a burst of tasks. These tasks will be queued up and the pool should scale up to handle
    // them
    for (int i = 0; i < taskCount; i++) {
      telemetryPool.execute(
          () -> {
            uniqueThreadIds.add(Thread.currentThread().getId());
            try {
              Thread.sleep(10);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            } finally {
              completionLatch.countDown();
            }
          });
    }

    // Wait for all tasks to finish processing.
    completionLatch.await(5, TimeUnit.SECONDS);

    assertEquals(
        10,
        uniqueThreadIds.size(),
        "The thread pool should have used 10 threads, but it used " + uniqueThreadIds.size());
    assertEquals(10, executor.getPoolSize());

    executor.setKeepAliveTime(
        1L, TimeUnit.MILLISECONDS); // Reset keep-alive time to 0 to allow threads to terminate.

    await()
        .atMost(5, TimeUnit.SECONDS) // Max time to wait for the condition
        .pollInterval(100, TimeUnit.MILLISECONDS) // Check every 100ms
        .untilAsserted(
            () -> {
              assertEquals(
                  0,
                  executor.getPoolSize(),
                  "The thread pool should have scaled down to 0 idle threads.");
            });
  }

  @Test
  void testTelemetryThreadsAreDaemonThreads() throws InterruptedException {
    TelemetryThreadPool telemetryPool = TelemetryThreadPool.getInstance();
    final AtomicBoolean isDaemonThread = new AtomicBoolean(false);
    final CountDownLatch latch = new CountDownLatch(1);

    telemetryPool.execute(
        () -> {
          isDaemonThread.set(Thread.currentThread().isDaemon());
          latch.countDown();
        });

    // Wait for the task to complete
    latch.await(1, TimeUnit.SECONDS);

    assertTrue(isDaemonThread.get(), "TelemetryThreadPool threads should be daemon threads");
  }

  private ThreadPoolExecutor getThreadPoolExecutor(TelemetryThreadPool telemetryThreadPool) {
    try {
      Field uploaderField = TelemetryThreadPool.class.getDeclaredField("uploader");
      uploaderField.setAccessible(true);
      return (ThreadPoolExecutor) uploaderField.get(telemetryThreadPool);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException("Failed to access the uploader field in TelemetryThreadPool", e);
    }
  }
}
