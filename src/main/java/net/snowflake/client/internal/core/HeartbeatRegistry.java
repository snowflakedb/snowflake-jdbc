package net.snowflake.client.internal.core;

import static net.snowflake.client.internal.jdbc.telemetry.InternalApiTelemetryTracker.internalCallMarker;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.time.Clock;
import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import net.snowflake.client.internal.jdbc.telemetry.Telemetry;
import net.snowflake.client.internal.jdbc.telemetry.TelemetryClient;
import net.snowflake.client.internal.jdbc.telemetry.TelemetryField;
import net.snowflake.client.internal.log.SFLogger;
import net.snowflake.client.internal.log.SFLoggerFactory;

/**
 * Registry that manages multiple HeartbeatThread instances.
 *
 * <p>Each unique heartbeat interval gets its own thread. This solves the problem where a session
 * with short interval could expire when a session with long interval is added.
 *
 * <p>Replaces the singleton HeartbeatBackground.
 *
 * <p>Thread-safe: Uses concurrent data structures and synchronization where needed.
 */
public class HeartbeatRegistry {
  private static final SFLogger logger = SFLoggerFactory.getLogger(HeartbeatRegistry.class);

  /** Maximum number of different heartbeat intervals (prevents thread explosion) */
  private static final int MAX_HEARTBEAT_THREADS = 10;

  /** Singleton instance */
  private static volatile HeartbeatRegistry instance;

  /** Map: heartbeat interval (seconds) -> thread managing that interval */
  private final ConcurrentHashMap<Long, HeartbeatThread> threads = new ConcurrentHashMap<>();

  /**
   * Map: session -> its required interval (for removal). Uses WeakHashMap for automatic cleanup if
   * session is garbage collected.
   */
  private final Map<SFSession, Long> sessionIntervals =
      Collections.synchronizedMap(new WeakHashMap<>());

  private final ScheduledExecutorService executor;
  private final Clock clock;
  private volatile boolean isShutdown = false;

  /** Private constructor for singleton. */
  private HeartbeatRegistry() {
    this(
        Executors.newScheduledThreadPool(
            MAX_HEARTBEAT_THREADS,
            new ThreadFactoryBuilder().setNameFormat("heartbeat-pool-%d").setDaemon(true).build()),
        Clock.systemUTC());

    // Register shutdown hook to clean up resources on JVM exit
    try {
      Runtime.getRuntime()
          .addShutdownHook(
              new Thread(
                  () -> {
                    logger.debug("JVM shutdown detected - cleaning up HeartbeatRegistry");
                    shutdown();
                  },
                  "heartbeat-registry-shutdown"));
    } catch (SecurityException | IllegalStateException ex) {
      logger.debug("Unable to register HeartbeatRegistry shutdown hook; continuing", ex);
    }
  }

  /**
   * Package-private constructor for testing.
   *
   * @param executor Scheduler for heartbeat tasks
   * @param clock Time source (injectable for testing)
   */
  HeartbeatRegistry(ScheduledExecutorService executor, Clock clock) {
    if (executor == null) {
      throw new IllegalArgumentException("Executor cannot be null");
    }
    if (clock == null) {
      throw new IllegalArgumentException("Clock cannot be null");
    }

    this.executor = executor;
    this.clock = clock;

    logger.debug("HeartbeatRegistry initialized");
  }

  /**
   * Get singleton instance.
   *
   * @return The singleton HeartbeatRegistry instance
   */
  public static HeartbeatRegistry getInstance() {
    if (instance == null) {
      synchronized (HeartbeatRegistry.class) {
        if (instance == null) {
          instance = new HeartbeatRegistry();
        }
      }
    }
    return instance;
  }

  /**
   * Add session to heartbeat management.
   *
   * <p>Calculates the required heartbeat interval and adds the session to the appropriate
   * HeartbeatThread. If no thread exists for this interval, creates one. If a thread already
   * exists, the session is simply added (no rescheduling).
   *
   * @param session The session to heartbeat
   * @param masterTokenValidityInSecs Master token validity in seconds
   * @param heartbeatFrequencyInSecs Desired heartbeat frequency in seconds
   */
  public void addSession(
      SFSession session, long masterTokenValidityInSecs, int heartbeatFrequencyInSecs) {
    if (session == null) {
      throw new IllegalArgumentException("Session cannot be null");
    }
    if (masterTokenValidityInSecs <= 0) {
      throw new IllegalArgumentException(
          "Master token validity must be positive: " + masterTokenValidityInSecs);
    }
    if (heartbeatFrequencyInSecs <= 0) {
      throw new IllegalArgumentException(
          "Heartbeat frequency must be positive: " + heartbeatFrequencyInSecs);
    }
    if (isShutdown) {
      throw new IllegalStateException("Cannot add session to shutdown HeartbeatRegistry");
    }

    // Calculate required interval: min of requested frequency and 1/4 of token validity
    long requiredInterval = Math.min(heartbeatFrequencyInSecs, masterTokenValidityInSecs / 4);

    // Enforce minimum interval of 1 second
    requiredInterval = Math.max(requiredInterval, 1);

    logger.debug(
        "Adding session {} with interval {}s (requested: {}s, validity: {}s)",
        session.getSessionId(),
        requiredInterval,
        heartbeatFrequencyInSecs,
        masterTokenValidityInSecs);

    // Synchronize thread creation to prevent race conditions with limit check
    synchronized (this) {
      if (isShutdown) {
        throw new IllegalStateException("Cannot add session to shutdown HeartbeatRegistry");
      }
      // Check thread count limit
      if (threads.size() >= MAX_HEARTBEAT_THREADS && !threads.containsKey(requiredInterval)) {
        logger.debug("Maximum threads reached - attempting to prune empty threads before fallback");
        pruneEmptyThreads();

        // Check again after pruning
        if (threads.size() >= MAX_HEARTBEAT_THREADS && !threads.containsKey(requiredInterval)) {
          logger.warn(
              "Maximum heartbeat threads ({}) reached. Session {} will use closest existing interval.",
              MAX_HEARTBEAT_THREADS,
              session.getSessionId());

          // Send telemetry event
          sendMaxThreadsExceededTelemetry(session, requiredInterval);

          final int requestedInterval = (int) requiredInterval;

          // Find best existing interval (prefers shorter intervals for safety)
          requiredInterval =
              HeartbeatIntervalSelector.selectBestInterval(requiredInterval, threads.keySet());

          logger.debug(
              "Mapped requested heartbeat interval {}s to existing interval {}s as best-effort fallback after reaching maximum heartbeat threads.",
              requestedInterval,
              requiredInterval);
        }
      }

      // Get or create a live thread for this interval, retrying if a stale shutdown thread is
      // found.
      final int maxAddAttempts = 2;
      boolean added = false;
      for (int attempt = 0; attempt < maxAddAttempts; attempt++) {
        HeartbeatThread thread =
            threads.computeIfAbsent(
                requiredInterval, interval -> new HeartbeatThread(interval, executor, clock));

        if (thread.addSession(session)) {
          sessionIntervals.put(session, requiredInterval);
          logger.debug(
              "Session {} added to interval {}s. Active threads: {}",
              session.getSessionId(),
              requiredInterval,
              threads.size());
          added = true;
          break;
        }

        logger.debug(
            "Heartbeat thread for interval {}s was shutdown before session {} could be added. Retrying.",
            requiredInterval,
            session.getSessionId());
        threads.remove(requiredInterval, thread);
      }

      if (!added) {
        throw new IllegalStateException(
            "Unable to add session " + session.getSessionId() + " to a live heartbeat thread");
      }
    }
  }

  /**
   * Send telemetry event when max heartbeat threads limit is exceeded.
   *
   * @param session The session that triggered the limit
   * @param requestedInterval The interval that was requested but couldn't be created
   */
  private void sendMaxThreadsExceededTelemetry(SFSession session, long requestedInterval) {
    try {
      Telemetry telemetry = session.getTelemetryClient(internalCallMarker());
      if (!(telemetry instanceof TelemetryClient)) {
        logger.trace("Telemetry client not available, skipping max threads telemetry");
        return;
      }
      TelemetryClient telemetryClient = (TelemetryClient) telemetry;

      ObjectNode telemetryData = ObjectMapperFactory.getObjectMapper().createObjectNode();
      telemetryData.put(
          TelemetryField.TYPE.toString(), TelemetryField.HEARTBEAT_MAX_THREADS_EXCEEDED.toString());
      telemetryData.put("max_heartbeat_threads", MAX_HEARTBEAT_THREADS);
      telemetryData.put("active_threads", threads.size());
      telemetryData.put("requested_interval", requestedInterval);
      telemetryData.put("session_id", session.getSessionId());

      telemetryClient.addLogToBatch(telemetryData, System.currentTimeMillis());
      logger.trace("Queued max threads exceeded telemetry for sending");

    } catch (Exception e) {
      // Never fail the session due to telemetry
      logger.trace("Failed to send max threads exceeded telemetry: {}", e.getMessage());
    }
  }

  /**
   * Remove empty threads from the registry.
   *
   * <p>Sessions in HeartbeatThread are stored in a WeakHashMap. When sessions are garbage collected
   * without explicit removal, the thread becomes empty but remains in the registry. This method
   * cleans up such threads to free up thread slots.
   *
   * <p>Called when approaching thread limit before falling back to existing intervals. Uses atomic
   * operations to prevent races with concurrent addSession calls.
   */
  private void pruneEmptyThreads() {
    int beforeCount = threads.size();

    // Use computeIfPresent to atomically check and remove empty threads
    threads.forEach(
        (interval, thread) -> {
          threads.computeIfPresent(
              interval,
              (key, t) -> {
                // Double-check it's the same thread and it's empty
                if (t == thread && t.isEmpty()) {
                  logger.debug(
                      "Pruning empty heartbeat thread for interval {}s (sessions were GC'd)",
                      interval);
                  t.shutdown();
                  return null; // Remove from map
                }
                return t; // Keep in map
              });
        });

    int prunedCount = beforeCount - threads.size();
    if (prunedCount > 0) {
      logger.debug(
          "Pruned {} empty heartbeat threads. Active threads: {} -> {}",
          prunedCount,
          beforeCount,
          threads.size());
    }
  }

  /**
   * Remove session from heartbeat management.
   *
   * <p>Session's interval is looked up from internal tracking. If the HeartbeatThread becomes
   * empty, it is shutdown and removed atomically to prevent races with concurrent addSession calls.
   *
   * @param session The session to remove
   */
  public void removeSession(SFSession session) {
    if (session == null) {
      logger.debug("Attempted to remove null session");
      return;
    }
    if (isShutdown) {
      logger.debug("Registry is shutdown, ignoring removeSession for {}", session.getSessionId());
      return;
    }

    synchronized (this) {
      Long interval = sessionIntervals.remove(session);

      if (interval == null) {
        logger.debug("Session {} not found in registry", session.getSessionId());
        return;
      }

      logger.debug("Removing session {} with interval {}s", session.getSessionId(), interval);

      // Remove session from thread and cleanup if empty - atomic operation
      threads.computeIfPresent(
          interval,
          (key, thread) -> {
            thread.removeSession(session);

            // If thread is now empty, shut it down and return null to remove from map
            if (thread.isEmpty()) {
              logger.debug(
                  "Removed empty heartbeat thread for interval {}s. Remaining threads: {}",
                  interval,
                  threads.size() - 1); // -1 because we're about to remove this one
              thread.shutdown();
              return null; // Remove from map
            }
            return thread; // Keep in map
          });
    }
  }

  // === Testing Support ===

  /**
   * Trigger heartbeat immediately for a specific interval (for testing).
   *
   * @param intervalSeconds The interval to trigger
   */
  @VisibleForTesting
  public void triggerHeartbeatForInterval(long intervalSeconds) {
    HeartbeatThread thread = threads.get(intervalSeconds);
    if (thread != null) {
      thread.triggerHeartbeatNow();
    } else {
      logger.debug("No heartbeat thread found for interval {}s", intervalSeconds);
    }
  }

  /**
   * Get active thread count (for testing/monitoring).
   *
   * @return Number of active heartbeat threads
   */
  @VisibleForTesting
  public int getActiveThreadCount() {
    return threads.size();
  }

  /**
   * Get session count for a specific interval (for testing).
   *
   * @param intervalSeconds The interval to query
   * @return Number of sessions, or 0 if no thread exists
   */
  @VisibleForTesting
  public int getSessionCountForInterval(long intervalSeconds) {
    HeartbeatThread thread = threads.get(intervalSeconds);
    return thread != null ? thread.getSessionCount() : 0;
  }

  /**
   * Shutdown all threads and cleanup.
   *
   * <p>Should be called during application shutdown to cleanly release resources. Also used in test
   * cleanup. Idempotent - safe to call multiple times.
   */
  public synchronized void shutdown() {
    if (isShutdown) {
      logger.debug("HeartbeatRegistry already shutdown");
      return;
    }

    logger.debug("Shutting down HeartbeatRegistry");
    isShutdown = true;

    threads.values().forEach(HeartbeatThread::shutdown);
    threads.clear();
    sessionIntervals.clear();

    executor.shutdown();
  }
}
