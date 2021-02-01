/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import java.util.HashSet;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

/**
 * This class is a singleton which is running inside driver to heartbeat snowflake server for each
 * connection
 */
public class HeartbeatBackground implements Runnable {
  private static HeartbeatBackground singleton = new HeartbeatBackground();

  /** The logger. */
  private static final SFLogger LOGGER = SFLoggerFactory.getLogger(HeartbeatBackground.class);

  // default master token validity (in seconds) is 4 hours
  private long masterTokenValidityInSecs = 4 * 3600;

  /** How often heartbeat executes is calculated based on master token validity and the headroom */
  private long heartBeatIntervalInSecs = masterTokenValidityInSecs / 4;

  // Scheduler handling the main heartbeat background
  private ScheduledExecutorService scheduler = null;

  // future for a scheduled heartbeat task
  ScheduledFuture<?> heartbeatFuture;

  /**
   * List of sessions to heartbeat. Use weak hash map so that if a session object is deleted and
   * garbaged collected, it will be removed from the list so that we will not keep doing heartbeat
   * for it. This is to take care of the case when some application does not close session before it
   * goes out of scope.
   */
  WeakHashMap<SFSession, Boolean> sessions = new WeakHashMap<>();

  // When is the last time heartbeat started
  private long lastHeartbeatStartTimeInSecs = 0;

  // Method to get the heartbeat instance
  public static HeartbeatBackground getInstance() {
    return singleton;
  }

  /** private constructor so that no one can try to create one */
  private HeartbeatBackground() {}

  /**
   * Method to add a session
   *
   * <p>It will compare the master token validity and stored master token validity and if it is
   * less, we will update the stored one and the heartbeat interval and reschedule heartbeat.
   *
   * <p>This method is called when a session is created.
   *
   * @param session the session will be added
   * @param masterTokenValidityInSecs time interval for which client need to check validity of
   *     master token with server
   */
  protected synchronized void addSession(
      SFSession session, long masterTokenValidityInSecs, int heartbeatFrequencyInSecs) {
    boolean requireReschedule = false;

    long oldHeartBeatIntervalInSecs = this.heartBeatIntervalInSecs;
    this.heartBeatIntervalInSecs = (long) heartbeatFrequencyInSecs;

    if (this.heartBeatIntervalInSecs > masterTokenValidityInSecs / 4) {
      this.heartBeatIntervalInSecs = masterTokenValidityInSecs / 4;
    }

    LOGGER.debug(
        "update heartbeat interval" + " from {} to {}",
        oldHeartBeatIntervalInSecs,
        this.heartBeatIntervalInSecs);

    // heartbeat rescheduling required
    requireReschedule = true;

    // add session to the list to be heartbeated
    sessions.put(session, Boolean.TRUE);

    /*
     * Create scheduler if it is the first time. It uses a custom thread
     * factory that will create daemon thread so that it will not block
     * JVM from exiting.
     */
    if (this.scheduler == null) {
      LOGGER.debug("create heartbeat thread pool");
      this.scheduler =
          Executors.newScheduledThreadPool(
              1,
              new ThreadFactory() {
                @Override
                public Thread newThread(Runnable runnable) {
                  Thread thread = Executors.defaultThreadFactory().newThread(runnable);
                  thread.setName("heartbeat (" + thread.getId() + ")");
                  thread.setDaemon(true);
                  return thread;
                }
              });
    }

    // schedule a heartbeat task if none exists
    if (heartbeatFuture == null) {
      LOGGER.debug("schedule heartbeat task");
      this.scheduleHeartbeat();
    }
    // or reschedule if the master token validity has been reduced (rare event)
    else if (requireReschedule) {
      LOGGER.debug("Cancel existing heartbeat task");

      // Cancel existing task if not started yet and reschedule
      if (heartbeatFuture.cancel(false)) {
        LOGGER.debug("Canceled existing heartbeat task, reschedule");
        this.scheduleHeartbeat();
      } else {
        LOGGER.debug("Failed to cancel existing heartbeat task");
      }
    }
  }

  /**
   * Method to remove a session. This is called when a session is closed. Notice that if a session
   * is not closed but the session object goes out of scope, then this method will not be called.
   * And then the session will be kept in the list of sessions to be heartbeated until the object
   * gets garbage collected since we use weak reference to the object.
   *
   * @param session the session will be removed
   */
  protected synchronized void removeSession(SFBaseSession session) {
    sessions.remove(session);
  }

  /** Schedule the next heartbeat */
  private void scheduleHeartbeat() {
    // elapsed time in seconds since the last heartbeat
    long elapsedSecsSinceLastHeartBeat =
        System.currentTimeMillis() / 1000 - lastHeartbeatStartTimeInSecs;

    /*
     * The initial delay for the new scheduling is 0 if the elapsed
     * time is more than the heartbeat time interval, otherwise it is the
     * difference between the heartbeat time interval and the elapsed time.
     */
    long initialDelay = Math.max(heartBeatIntervalInSecs - elapsedSecsSinceLastHeartBeat, 0);

    LOGGER.debug("schedule heartbeat task with initial delay of {} seconds", initialDelay);

    // Creates and executes a periodic action to send heartbeats
    this.heartbeatFuture = this.scheduler.schedule(this, initialDelay, TimeUnit.SECONDS);
  }

  /**
   * Run heartbeat: for each session send a heartbeat request and schedule next heartbeat as long as
   * there are sessions left.
   *
   * <p>Notice that the synchronization is only around the code that visits the global sessions map
   * and the code that schedules next heartbeat, but not around the heartbeat calls for each session
   * in order to minimize the chance of blocking the adding of a session by performing the
   * heartbeats. This is because adding a session is called from JDBC connection creation call which
   * directly affects application performance.
   */
  @Override
  public void run() {
    /*
     * Remember current time as the heartbeat start time. This is used for
     * calculating the delay for the next heartbeat.
     */
    this.lastHeartbeatStartTimeInSecs = System.currentTimeMillis() / 1000;

    Set<SFSession> sessionsToHeartbeat = new HashSet<SFSession>();

    // synchronously get a copy of the sessions from the global list
    synchronized (this) {
      sessionsToHeartbeat.addAll(sessions.keySet());
    }

    // heartbeat every session.
    for (SFSession session : sessionsToHeartbeat) {
      try {
        session.heartbeat();
      } catch (Throwable ex) {
        LOGGER.error("heartbeat error - message=" + ex.getMessage(), ex);
      }
    }

    /*
     * The following is synchronized with the methods to add or remove a
     * session so that we always make sure we have one heartbeat task scheduled
     * when there is any session left.
     */
    synchronized (this) {
      // schedule next heartbeat
      if (sessions.size() > 0) {
        LOGGER.debug("schedule next heartbeat run");

        scheduleHeartbeat();
      } else {
        LOGGER.debug("no need for heartbeat since no more sessions");

        // no need to heartbeat if no more session
        this.heartbeatFuture = null;
      }
    }
  }
}
