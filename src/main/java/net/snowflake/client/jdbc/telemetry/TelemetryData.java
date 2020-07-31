/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.jdbc.telemetry;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import net.snowflake.client.core.ObjectMapperFactory;
import net.snowflake.client.util.SecretDetector;

/** Copyright (c) 2018-2019 Snowflake Computing Inc. All rights reserved. */
public class TelemetryData {
  // message is a json node
  private final ObjectNode message;
  private final long timeStamp;
  private static final ObjectMapper mapper = ObjectMapperFactory.getObjectMapper();

  // Only allow code in same package to construct TelemetryData
  TelemetryData(ObjectNode message, long timeStamp) {
    this.message = (ObjectNode) SecretDetector.maskJacksonNode(message);
    this.timeStamp = timeStamp;
  }

  public long getTimeStamp() {
    return timeStamp;
  }

  public ObjectNode getMessage() {
    return message;
  }

  public ObjectNode toJson() {
    ObjectNode node = mapper.createObjectNode();
    node.put("timestamp", this.timeStamp + "");
    node.set("message", this.message);
    return node;
  }

  @Override
  public String toString() {

    return toJson().toString();
  }
}
