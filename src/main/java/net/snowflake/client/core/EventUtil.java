/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Utility class to encapsulate support information pertaining to the EventHandler and events.
 *
 * @author jrosen
 */
public class EventUtil {
  public static final String DUMP_PATH_PROP = "snowflake.dump_path";
  public static final String DUMP_SIZE_PROP = "snowflake.max_dump_size";
  public static final String DUMP_SUBDIR = "snowflake_dumps";

  private static final String DUMP_FILE_ID = UUID.randomUUID().toString();
  private static final String DUMP_PATH_PREFIX =
      systemGetProperty(DUMP_PATH_PROP) == null ? "/tmp" : systemGetProperty(DUMP_PATH_PROP);
  private static final long MAX_DUMP_FILE_SIZE_BYTES =
      systemGetProperty(DUMP_SIZE_PROP) == null
          ? (10 << 20)
          : Long.valueOf(systemGetProperty(DUMP_SIZE_PROP));

  private static AtomicReference<EventHandler> eventHandler = new AtomicReference<>(null);

  private static int MAX_ENTRIES = 1000;

  private static int FLUSH_PERIOD_MS = 10000;

  /**
   * Initializes the common eventHandler instance for all sessions/threads
   *
   * @param maxEntries - maximum number of buffered events before flush
   * @param flushPeriodMs - period of time between asynchronous buffer flushes
   */
  public static synchronized void initEventHandlerInstance(int maxEntries, int flushPeriodMs) {
    if (eventHandler.get() == null) {
      eventHandler.set(new EventHandler(maxEntries, flushPeriodMs));
    }
    // eventHandler.startFlusher();
  }

  /** @return the shared EventHandler instance */
  public static EventHandler getEventHandlerInstance() {
    if (eventHandler.get() == null) {
      initEventHandlerInstance(MAX_ENTRIES, FLUSH_PERIOD_MS);
    }

    return eventHandler.get();
  }

  public static void triggerBasicEvent(Event.EventType type, String message, boolean flushBuffer) {
    EventHandler eh = eventHandler.get();
    if (eh != null) {
      eh.triggerBasicEvent(type, message, flushBuffer);
    }
  }

  public static void triggerStateTransition(BasicEvent.QueryState newState, String identifier) {
    EventHandler eh = eventHandler.get();
    if (eh != null) {
      eh.triggerStateTransition(newState, identifier);
    }
  }

  public static String getDumpPathPrefix() {
    return DUMP_PATH_PREFIX + "/" + DUMP_SUBDIR;
  }

  public static String getDumpFileId() {
    return DUMP_FILE_ID;
  }

  public static long getmaxDumpFileSizeBytes() {
    return MAX_DUMP_FILE_SIZE_BYTES;
  }
}
