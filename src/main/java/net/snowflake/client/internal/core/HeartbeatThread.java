package net.snowflake.client.internal.core;

import com.google.common.annotations.VisibleForTesting;
import java.time.Clock;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import net.snowflake.client.internal.log.SFLogger;
import net.snowflake.client.internal.log.SFLoggerFactory;

/**
 * Manages heartbeat for all sessions that share the same heartbeat interval.
 *
 * <p>Each unique interval gets its own HeartbeatThread instance. Uses WeakHashMap to automatically
 * clean up sessions that are garbage collected.
 *
 * <p>Thread-safe: Session management methods are synchronized. The scheduled {@code run()} method
 * coordinates with shared state using the class's existing concurrency controls.
 */
class HeartbeatThread implements Runnable {
  private static final SFLogger logger = SFLoggerFactory.getLogger(HeartbeatThread.class);

  private final long intervalSeconds;
  private final ScheduledExecutorService executor;
  private final Clock clock;

  /**
   * Sessions managed by this thread. Uses a Set backed by WeakHashMap to automatically remove
   * sessions when garbage collected, preventing memory leaks if cleanup fails.
   */
  private final Set<SFSession> sessions = Collections.newSetFromMap(new WeakHashMap<>());

  private volatile ScheduledFuture<?> scheduledTask;
  private volatile long lastHeartbeatStartTimeInSecs;
  private volatile boolean isShutdown = false;

  /**
   * Creates a new HeartbeatThread for a specific interval.
   *
   * @param intervalSeconds How often to heartbeat (in seconds)
   * @param executor Scheduler for running heartbeat tasks
   * @param clock Time source (injectable for testing)
   */
  HeartbeatThread(long intervalSeconds, ScheduledExecutorService executor, Clock clock) {
    if (intervalSeconds <= 0) {
      throw new IllegalArgumentException("Interval must be positive: " + intervalSeconds);
    }
    if (executor == null) {
      throw new IllegalArgumentException("Executor cannot be null");
    }
    if (clock == null) {
      throw new IllegalArgumentException("Clock cannot be null");
    }

    this.intervalSeconds = intervalSeconds;
    this.executor = executor;
    this.clock = clock;
    this.lastHeartbeatStartTimeInSecs = 0;

    logger.debug("Created HeartbeatThread for interval {}s", intervalSeconds);
  }

  /**
   * Add session to this heartbeat thread.
   *
   * <p>If this is the first session, starts the heartbeat task. If thread is already running, the
   * new session will be included in the next scheduled heartbeat.
   *
   * @param session The session to add
   * @return true if session was added, false if thread is shutdown
   * @throws IllegalArgumentException if session is null
   */
  synchronized boolean addSession(SFSession session) {
    if (session == null) {
      throw new IllegalArgumentException("Session cannot be null");
    }

    if (isShutdown) {
      logger.warn(
          "Attempted to add session {} to shutdown HeartbeatThread (interval={}s)",
          session.getSessionId(),
          intervalSeconds);
      return false;
    }

    boolean wasEmpty = sessions.isEmpty();
    sessions.add(session);

    logger.debug(
        "Added session {} to HeartbeatThread (interval={}s), total sessions: {}",
        session.getSessionId(),
        intervalSeconds,
        sessions.size());

    // Start heartbeat if this is the first session
    if (wasEmpty) {
      start();
    }

    return true;
  }

  /**
   * Remove session from this heartbeat thread.
   *
   * <p>WeakHashMap ensures sessions are cleaned up even if this method isn't called.
   *
   * @param session The session to remove
   */
  synchronized void removeSession(SFSession session) {
    sessions.remove(session);

    logger.debug(
        "Removed session {} from HeartbeatThread (interval={}s), remaining sessions: {}",
        session.getSessionId(),
        intervalSeconds,
        sessions.size());

    // Don't auto-shutdown here - let the registry decide when to cleanup
  }

  /**
   * Check if no sessions remain.
   *
   * @return true if this thread has no sessions
   */
  synchronized boolean isEmpty() {
    return sessions.isEmpty();
  }

  /**
   * Get the current number of sessions.
   *
   * @return Number of sessions managed by this thread
   */
  @VisibleForTesting
  synchronized int getSessionCount() {
    return sessions.size();
  }

  /**
   * Get the heartbeat interval in seconds.
   *
   * @return Interval in seconds
   */
  long getIntervalSeconds() {
    return intervalSeconds;
  }

  /** Start scheduled heartbeat task. Called automatically when first session is added. */
  private synchronized void start() {
    if (isShutdown) {
      logger.warn("Cannot start shutdown HeartbeatThread (interval={}s)", intervalSeconds);
      return;
    }

    if (scheduledTask != null) {
      logger.debug("HeartbeatThread (interval={}s) already started", intervalSeconds);
      return;
    }

    logger.debug("Starting HeartbeatThread (interval={}s)", intervalSeconds);
    scheduleHeartbeat();
  }

  /** Schedule the next heartbeat task. */
  private void scheduleHeartbeat() {
    // Calculate elapsed time since last heartbeat
    long currentTimeInSecs = clock.millis() / 1000;
    long elapsedSecsSinceLastHeartbeat = currentTimeInSecs - lastHeartbeatStartTimeInSecs;

    /*
     * The initial delay is 0 if enough time has elapsed,
     * otherwise it's the remaining time until the next interval.
     */
    long initialDelay = Math.max(intervalSeconds - elapsedSecsSinceLastHeartbeat, 0);

    logger.debug(
        "Scheduling heartbeat task (interval={}s) with initial delay of {}s",
        intervalSeconds,
        initialDelay);

    // Use schedule() for single execution, not scheduleAtFixedRate()
    // We manually reschedule after each run to:
    // 1. Stop scheduling when sessions list becomes empty
    // 2. Handle exceptions without stopping future executions
    try {
      scheduledTask = executor.schedule(this, initialDelay, TimeUnit.SECONDS);
    } catch (RejectedExecutionException e) {
      logger.error(
          "Failed to schedule heartbeat task (interval={}s): executor rejected task. Marking thread as shutdown.",
          intervalSeconds,
          e);
      isShutdown = true;
    }
  }

  /**
   * Stop scheduled heartbeat task and prevent new sessions from being added.
   *
   * <p>Called when no sessions remain or during cleanup.
   */
  synchronized void shutdown() {
    if (isShutdown) {
      logger.debug(
          "HeartbeatThread (interval={}s) already shutdown; performing cleanup", intervalSeconds);
    } else {
      logger.debug("Shutting down HeartbeatThread (interval={}s)", intervalSeconds);
    }
    isShutdown = true;

    if (scheduledTask != null) {
      scheduledTask.cancel(false);
      scheduledTask = null;
    }

    sessions.clear();
  }

  /**
   * Run heartbeat for all sessions in this thread.
   *
   * <p>Called by scheduler at fixed interval. Synchronization is only around accessing the sessions
   * map, not around the actual heartbeat calls, to minimize blocking.
   */
  @Override
  public void run() {
    if (isShutdown) {
      logger.debug("Skipping heartbeat run for shutdown thread (interval={}s)", intervalSeconds);
      return;
    }

    // Record current time as heartbeat start time
    lastHeartbeatStartTimeInSecs = clock.millis() / 1000;

    // Get a copy of sessions to heartbeat (outside synchronized block)
    Set<SFSession> sessionsToHeartbeat = new HashSet<>();
    synchronized (this) {
      sessionsToHeartbeat.addAll(sessions);
    }

    logger.debug(
        "Running heartbeat for {} sessions (interval={}s)",
        sessionsToHeartbeat.size(),
        intervalSeconds);

    // Heartbeat each session (outside synchronized block to avoid blocking)
    for (SFSession session : sessionsToHeartbeat) {
      try {
        session.heartbeat();
      } catch (Throwable ex) {
        logger.error(
            "Heartbeat error for session {} - message={}",
            session.getSessionId(),
            ex.getMessage(),
            ex);
      }
    }

    // Schedule next heartbeat if there are still sessions
    synchronized (this) {
      if (!isShutdown && !sessions.isEmpty()) {
        logger.debug("Scheduling next heartbeat run (interval={}s)", intervalSeconds);
        scheduleHeartbeat();
      } else {
        logger.debug(
            "Not scheduling next heartbeat (shutdown={}, sessions={})",
            isShutdown,
            sessions.size());
        scheduledTask = null;
      }
    }
  }

  /**
   * Trigger heartbeat immediately without waiting for scheduled time.
   *
   * <p>For testing purposes only.
   */
  @VisibleForTesting
  synchronized void triggerHeartbeatNow() {
    if (!isShutdown) {
      run();
    }
  }
}
