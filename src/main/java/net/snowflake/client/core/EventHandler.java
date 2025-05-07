package net.snowflake.client.core;

import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.zip.GZIPOutputStream;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

public class EventHandler extends Handler {
  private static final SFLogger logger = SFLoggerFactory.getLogger(EventHandler.class);

  // Number of entries in the log buffer in memory
  protected static final long LOG_BUFFER_SIZE = (1L << 14);

  // Maximum amount of time a Snowflake dump file can exist before being
  // delete upon the next attempt to dump (1 week)
  protected static final long FILE_EXPN_TIME_MS = 7L * 24L * 3600L * 1000L;

  // Dump file properties
  protected static final String LOG_DUMP_FILE_NAME = "sf_log_";
  protected static final String LOG_DUMP_FILE_EXT = ".dmp";
  protected static final String LOG_DUMP_COMP_EXT = ".gz";

  // Property to control time (in hours) an incident signature will be throttled for
  protected static final String THROTTLE_DURATION_PROP = "snowflake.throttle_duration";

  // Property to control number of times an incident signature can be seen
  // before being throttled
  protected static final String THROTTLE_LIMIT_PROP = "snowflake.throttle_limit";

  // Property to disable dumps completely
  protected static final String DISABLE_DUMPS_PROP = "snowflake.disable_debug_dumps";

  // Property to set the number of allowable snowflake dump files
  protected static final String MAX_NUM_DUMP_FILES_PROP = "snowflake.max_dumpfiles";

  // Property to set the number of allowable snowflake dump files
  protected static final String MAX_SIZE_DUMPS_MB_PROP = "snowflake.max_dumpdir_size_mb";

  // Property to disable GZIP compression of log dump files
  protected static final String DISABLE_DUMP_COMPR_PROP = "snowflake.disable_log_compression";

  private static final int THROTTLE_DURATION_HRS =
      systemGetProperty(THROTTLE_DURATION_PROP) == null
          ? 1
          : Integer.valueOf(systemGetProperty(THROTTLE_DURATION_PROP));

  private static final int INCIDENT_THROTTLE_LIMIT_PER_HR =
      systemGetProperty(THROTTLE_LIMIT_PROP) == null
          ? 1
          : Integer.valueOf(systemGetProperty(THROTTLE_LIMIT_PROP));

  // Default values
  private static final int DEFAULT_MAX_DUMP_FILES = 100;
  private static final int DEFAULT_MAX_DUMPDIR_SIZE_MB = 128;

  /** Runnable to handle periodic flushing of the event buffer */
  private class QueueFlusher implements Runnable {
    @Override
    public void run() {
      flushEventBuffer();
    }
  }

  // Location to dump log entries to
  private final String logDumpPathPrefix;

  // Max size the event buffer can reach before being forcibly flushed
  private final int maxEntries;

  // Period of time (in ms) to wait before waking the QueueFlusher
  private final int flushPeriodMs;

  // Queue to buffer events while they are waiting to be flushed
  private final ArrayList<Event> eventBuffer;

  // Queue to buffer log messages
  private final ArrayList<LogRecord> logBuffer;

  // Executor to periodically flush the eventBuffer
  private ScheduledExecutorService flusher;

  public EventHandler(int maxEntries, int flushPeriodMs) {
    this.maxEntries = maxEntries;
    this.flushPeriodMs = flushPeriodMs;

    eventBuffer = new ArrayList<>();
    logBuffer = new ArrayList<>();

    logDumpPathPrefix = EventUtil.getDumpPathPrefix();
  }

  /**
   * Returns current size of the event buffer
   *
   * @return size of eventBuffer
   */
  public synchronized int getBufferSize() {
    return eventBuffer.size();
  }

  /**
   * Returns the current size of the log buffer
   *
   * @return size of log buffer
   */
  public synchronized long getLogBufferSize() {
    return logBuffer.size();
  }

  /** Creates and runs a new QueueFlusher thread */
  synchronized void startFlusher() {
    // Create a new scheduled executor service with a thread factory that
    // creates daemonized threads; this way if the user doesn't exit nicely
    // the JVM Runtime won't hang
    flusher =
        Executors.newScheduledThreadPool(
            1,
            new ThreadFactory() {
              @Override
              public Thread newThread(Runnable r) {
                Thread t = Executors.defaultThreadFactory().newThread(r);
                t.setDaemon(true);
                return t;
              }
            });

    flusher.scheduleWithFixedDelay(new QueueFlusher(), 0, flushPeriodMs, TimeUnit.MILLISECONDS);
  }

  /** Stops the running QueueFlusher thread, if any */
  synchronized void stopFlusher() {
    if (flusher != null) {
      flusher.shutdown();
    }
  }

  /*
   * Pushes an event onto the event buffer and flushes if specified or if
   * the buffer has reached maximum capacity.
   */
  private synchronized void pushEvent(Event event, boolean flushBuffer) {
    eventBuffer.add(event);

    if (flushBuffer || eventBuffer.size() >= maxEntries) {
      this.flushEventBuffer();
    }
  }

  /**
   * Triggers a new event of type @type with message @message and flushes the eventBuffer if full
   *
   * @param type event type
   * @param message triggering message
   */
  void triggerBasicEvent(Event.EventType type, String message) {
    triggerBasicEvent(type, message, false);
  }

  /**
   * Triggers a new BaseEvent of type @type with message @message and flushes the eventBuffer if
   * full or @flushBuffer is true
   *
   * @param type event type
   * @param message trigger message
   * @param flushBuffer true if push the event to flush buffer
   */
  void triggerBasicEvent(Event.EventType type, String message, boolean flushBuffer) {
    Event triggeredEvent = new BasicEvent(type, message);

    pushEvent(triggeredEvent, flushBuffer);
  }

  /**
   * Triggers a state transition event to @newState with an identifier (eg, requestId, jobUUID, etc)
   *
   * @param newState new state
   * @param identifier event id
   */
  void triggerStateTransition(BasicEvent.QueryState newState, String identifier) {
    String msg =
        "{newState: "
            + newState.getDescription()
            + ", "
            + "info: "
            + identifier
            + ", "
            + "timestamp: "
            + getCurrentTimestamp()
            + "}";

    Event triggeredEvent = new BasicEvent(Event.EventType.STATE_TRANSITION, msg);

    pushEvent(triggeredEvent, false);
  }

  static String getCurrentTimestamp() {
    DateFormat fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
    fmt.setTimeZone(TimeZone.getTimeZone("UTC"));
    return fmt.format(new Date());
  }

  /**
   * Dumps the contents of the in-memory log buffer to disk and clears the buffer.
   *
   * @param identifier event id
   */
  public void dumpLogBuffer(String identifier) {
    final ArrayList<LogRecord> logBufferCopy;
    final PrintWriter logDumper;
    final OutputStream outStream;
    Formatter formatter = this.getFormatter();

    // Check if compression of dump file is enabled
    boolean disableCompression = systemGetProperty(DISABLE_DUMP_COMPR_PROP) != null;

    // If no identifying factor (eg, an incident id) was provided, get one
    if (identifier == null) {
      identifier = EventUtil.getDumpFileId();
    }

    // Do some sanity checking to make sure we're not flooding the user's
    // disk with dump files
    cleanupSfDumps(true);

    String logDumpPath =
        logDumpPathPrefix + File.separator + LOG_DUMP_FILE_NAME + identifier + LOG_DUMP_FILE_EXT;

    if (!disableCompression) {
      logDumpPath += LOG_DUMP_COMP_EXT;
    }

    logger.debug("EventHandler dumping log buffer to {}", logDumpPath);

    // Copy logBuffer because this is potentially long running.
    synchronized (this) {
      logBufferCopy = new ArrayList<>(logBuffer);
      logBuffer.clear();
    }

    File outputFile = new File(logDumpPath);

    /*
     * Because log files could potentially be very large, we should never open
     * them in append mode. It's rare that this should happen anyways...
     */
    try {
      // If the dump path doesn't already exist, create it.
      if (outputFile.getParentFile() != null) {
        outputFile.getParentFile().mkdirs();
      }

      outStream =
          disableCompression
              ? new FileOutputStream(logDumpPath, false)
              : new GZIPOutputStream(new FileOutputStream(logDumpPath, false));

      logDumper = new PrintWriter(outStream, true);
    } catch (IOException exc) {
      // Not much to do here, can't dump logs so exit out.
      logger.debug("Log dump failed, exception: {}", exc.getMessage());

      return;
    }

    // Iterate over log entries, format them, then dump them.
    for (LogRecord entry : logBufferCopy) {
      logDumper.write(formatter != null ? formatter.format(entry) : entry.getMessage());
    }

    // Clean up
    logDumper.flush();
    logDumper.close();
  }

  /**
   * Function to remove old Snowflake Dump files to make room for new ones.
   *
   * @param deleteOldest if true, always deletes the oldest file found if max number of dump files
   *     has been reached
   */
  protected void cleanupSfDumps(boolean deleteOldest) {
    // Check what the maximum number of dumpfiles and the max allowable
    // aggregate dump file size is.
    int maxDumpFiles =
        systemGetProperty(MAX_NUM_DUMP_FILES_PROP) != null
            ? Integer.valueOf(systemGetProperty(MAX_NUM_DUMP_FILES_PROP))
            : DEFAULT_MAX_DUMP_FILES;

    int maxDumpDirSizeMB =
        systemGetProperty(MAX_SIZE_DUMPS_MB_PROP) != null
            ? Integer.valueOf(systemGetProperty(MAX_SIZE_DUMPS_MB_PROP))
            : DEFAULT_MAX_DUMPDIR_SIZE_MB;

    File dumpDir = new File(logDumpPathPrefix);
    long dirSizeBytes = 0;

    if (dumpDir.listFiles() == null) {
      return;
    }

    // Keep a sorted list of files by size as we go in case we need to
    // delete some
    TreeSet<File> fileList =
        new TreeSet<>(
            new Comparator<File>() {
              @Override
              public int compare(File a, File b) {
                return a.length() < b.length() ? -1 : 1;
              }
            });

    // Loop over files in this directory and get rid of old ones
    // while accumulating the total size
    for (File file : dumpDir.listFiles()) {
      if ((!file.getName().startsWith(LOG_DUMP_FILE_NAME)
              && !file.getName().startsWith(IncidentUtil.INC_DUMP_FILE_NAME))
          || (System.currentTimeMillis() - file.lastModified() > FILE_EXPN_TIME_MS
              && file.delete())) {
        continue;
      }

      dirSizeBytes += file.length();
      fileList.add(file);
    }

    // If we're exceeding our max allotted disk usage, cut some stuff out;
    // else if we need to make space for a new dump delete the oldest.
    if (dirSizeBytes >= ((long) maxDumpDirSizeMB << 20)) {
      // While we take up more than half the allotted disk usage, keep deleting.
      for (File file : fileList) {
        if (dirSizeBytes < ((long) maxDumpDirSizeMB << 19)) {
          break;
        }

        long victimSize = file.length();
        if (file.delete()) {
          dirSizeBytes -= victimSize;
        }
      }
    } else if (deleteOldest && fileList.size() >= maxDumpFiles) {
      fileList.first().delete();
    }
  } // cleanupSfDumps(...)

  /**
   * Function to copy the event buffer, clear it, and iterate of the copy, calling each event's
   * flush() method one by one.
   *
   * <p>NOTE: This function is subject to a race condition; while the buffer copy is being iterated
   * over, the next round of buffer entries could be flushed creating a flush order that is not
   * "strictly consistent". While this could hypothetically also cause the system to run out of
   * memory due to an unbounded number of eventBuffer copies, that scenario is unlikely.
   */
  private void flushEventBuffer() {
    ArrayList<Event> eventBufferCopy;

    logger.debug("Flushing eventBuffer", false);

    // Copy event buffer because this may be long running
    synchronized (this) {
      eventBufferCopy = new ArrayList<>(eventBuffer);
      eventBuffer.clear();
    }

    for (Event event : eventBufferCopy) {
      event.flush();
    }
  }

  /* Overridden methods for Handler interface */

  /** Flushes all eventBuffer entries. */
  @Override
  public synchronized void flush() {
    logger.debug("EventHandler flushing logger buffer", false);

    dumpLogBuffer("");
  }

  /**
   * Overridden Logger.Handler publish(...) method. Buffers unformatted log records in memory in a
   * circular buffer-like fashion.
   *
   * @param record log record
   */
  @Override
  public synchronized void publish(LogRecord record) {
    if (!super.isLoggable(record)
        || this.getLevel() != null && record.getLevel().intValue() < this.getLevel().intValue()) {
      return;
    }

    synchronized (logBuffer) {
      if (logBuffer.size() == LOG_BUFFER_SIZE) {
        logBuffer.remove(0);
      }

      logBuffer.add(record);
    }
  }

  @Override
  public void close() {
    this.flushEventBuffer();
    this.stopFlusher();
  }
}
