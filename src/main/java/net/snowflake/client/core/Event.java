package net.snowflake.client.core;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.zip.GZIPOutputStream;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

/** Abstract class to encapsulate a Client-side Event and any methods associated with it. */
public abstract class Event {
  private static final SFLogger logger = SFLoggerFactory.getLogger(Event.class);

  private static final String EVENT_DUMP_FILE_NAME = "sf_event_";
  private static final String EVENT_DUMP_FILE_EXT = ".dmp.gz";

  // need to check if directory exists, if not try to create it
  // need to check file size, see if it exceeds maximum (need parameter for this)

  public static enum EventType {
    NETWORK_ERROR(1, "NETWORK ERROR", BasicEvent.class),
    STATE_TRANSITION(2, "STATE TRANSITION", BasicEvent.class),
    NONE(100, "NONE", BasicEvent.class);

    public int getId() {
      return id;
    }

    public String getDescription() {
      return description;
    }

    public Class<? extends Event> getEventClass() {
      return eventClass;
    }

    EventType(int id, String description, Class<? extends Event> eventClass) {
      this.id = id;
      this.description = description;
      this.eventClass = eventClass;
    }

    private final int id;
    private final String description;
    private final Class<? extends Event> eventClass;
  }

  private EventType type;
  private String message;

  public Event(EventType type, String message) {
    Preconditions.checkArgument(type.getEventClass() == this.getClass());

    this.type = type;
    this.message = message;
  }

  public EventType getType() {
    return this.type;
  }

  public void setType(EventType type) {
    this.type = type;
  }

  public String getMessage() {
    return this.message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  protected void writeEventDumpLine(String message) {
    final String eventDumpPath =
        EventUtil.getDumpPathPrefix()
            + "/"
            + EVENT_DUMP_FILE_NAME
            + EventUtil.getDumpFileId()
            + EVENT_DUMP_FILE_EXT;

    // If the event dump file is too large, truncate
    if (new File(eventDumpPath).length() < EventUtil.getmaxDumpFileSizeBytes()) {
      try {
        final OutputStream outStream =
            new GZIPOutputStream(new FileOutputStream(eventDumpPath, true));
        PrintWriter eventDumper = new PrintWriter(outStream, true);
        eventDumper.println(message);
        eventDumper.flush();
        eventDumper.close();
      } catch (IOException ex) {
        logger.error(
            "Could not open Event dump file {}, exception:{}", eventDumpPath, ex.getMessage());
      }
    } else {
      logger.error(
          "Failed to dump Event because dump file is "
              + "too large. Delete dump file or increase maximum dump file size.",
          false);
    }
  }

  /*
   * An event is "flushed" when it is leaving the system, hence the behavior
   * defined by flush dictates what to do with an Event when the Handler is
   * finished buffering it.
   */
  public abstract void flush();
}
