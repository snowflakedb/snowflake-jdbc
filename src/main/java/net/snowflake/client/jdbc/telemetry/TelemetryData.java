package net.snowflake.client.jdbc.telemetry;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Copyright (c) 2018 Snowflake Computing Inc. All rights reserved.
 */
public class TelemetryData
{
  //message is a json node
  private final ObjectNode message;
  private final long timeStamp;
  private final static ObjectMapper mapper = new ObjectMapper();

  public TelemetryData(ObjectNode message, long timeStamp)
  {
    this.message = message;
    this.timeStamp = timeStamp;
  }

  public long getTimeStamp()
  {
    return timeStamp;
  }

  public ObjectNode getMessage()
  {
    return message;
  }

  public ObjectNode toJson()
  {
    ObjectNode node = mapper.createObjectNode();
    node.put("timestamp",this.timeStamp+"");
    node.set("message",this.message);
    return node;
  }

  @Override
  public String toString()
  {

    return toJson().toString();

  }
}
