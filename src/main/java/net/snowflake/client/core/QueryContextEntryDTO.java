package net.snowflake.client.core;

/**
 * An entry in the set of query context exchanged with Cloud Services. This includes a domain
 * identifier(id), a timestamp that is monodically increasing, a priority for eviction and the
 * opaque information sent from the Cloud service.
 */
public class QueryContextEntryDTO {
  private long id;
  private long timestamp;
  private long priority;
  private OpaqueContextDTO context;

  public QueryContextEntryDTO() {
    // empty constructor
  }

  public QueryContextEntryDTO(long id, long timestamp, long priority, OpaqueContextDTO context) {
    this.id = id;
    this.timestamp = timestamp;
    this.priority = priority;
    this.context = context;
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public long getPriority() {
    return priority;
  }

  public void setPriority(long priority) {
    this.priority = priority;
  }

  public OpaqueContextDTO getContext() {
    return context;
  }

  public void setContext(OpaqueContextDTO context) {
    this.context = context;
  }
}
