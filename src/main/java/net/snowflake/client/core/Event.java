/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import com.google.common.base.Preconditions;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

/**
 * Abstract class to encapsulate a Client-side Event and any methods associated with it.
 *
 * @author jrosen
 */
public abstract class Event {
  private static final SFLogger logger = SFLoggerFactory.getLogger(Event.class);

  private static final String EVENT_DUMP_FILE_NAME = "sf_event_";
  private static final String EVENT_DUMP_FILE_EXT = ".dmp.gz";

  // need to check if directory exists, if not try to create it
  // need to check file size, see if it exceeds maximum (need parameter for this)

  public static enum EventType {
    INCIDENT(1, "INCIDENT", Incident.class),
    NETWORK_ERROR(2, "NETWORK ERROR", BasicEvent.class),
    STATE_TRANSITION(3, "STATE TRANSITION", BasicEvent.class),
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

  public void setMessage(String message) {
    this.message = message;
  }

  /*
   * An event is "flushed" when it is leaving the system, hence the behavior
   * defined by flush dictates what to do with an Event when the Handler is
   * finished buffering it.
   */
  public abstract void flush();
}
