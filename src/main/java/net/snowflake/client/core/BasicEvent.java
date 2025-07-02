package net.snowflake.client.core;

import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;

/** Base Event class for events that don't need to deviate from the default flush behavior. */
public class BasicEvent extends Event {
  // Format strings for query state transitions
  private static final String requestId = "requestId: %s";
  private static final String numPings = "numberPings: %d";
  private static final String jobId = "jobId: %s";
  private static final String chunkIdx = "chunkIndex: %d";

  private static final String EVENT_DUMP_PROP = "snowflake.dump_events";
  private static final Boolean doDump = systemGetProperty(EVENT_DUMP_PROP) != null;

  public enum QueryState {
    QUERY_STARTED(1, "Query Started", "{" + requestId + "}"),
    SENDING_QUERY(2, "Sending Query", "{" + requestId + "}"),
    WAITING_FOR_RESULT(3, "Waiting for Result", "{" + requestId + "," + numPings + "}"),
    PROCESSING_RESULT(4, "Processing Result", "{" + requestId + "}"),
    CONSUMING_RESULT(5, "Consuming Result", "{" + jobId + "," + chunkIdx + "}"),
    QUERY_ENDED(6, "Query ended", "{" + requestId + "}"),
    GETTING_FILES(8, "Getting Files", "{" + requestId + "}"),
    PUTTING_FILES(9, "Putting Files", "{" + requestId + "}"),
    ;

    QueryState(int id, String description, String argString) {
      this.id = id;
      this.description = description;
      this.argString = argString;
    }

    public int getId() {
      return id;
    }

    public String getDescription() {
      return description;
    }

    public String getArgString() {
      return argString;
    }

    private final int id;
    private final String description;
    private final String argString;
  }

  public BasicEvent(Event.EventType type, String message) {
    super(type, message);
  }

  @Override
  public void flush() {
    if (doDump) {
      // this.writeEventDumpLine("Event: " + getType() + "; Message: " + getMessage());
    }
  }
}
