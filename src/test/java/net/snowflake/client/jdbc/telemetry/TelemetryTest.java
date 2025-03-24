package net.snowflake.client.jdbc.telemetry;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.LinkedList;
import org.junit.jupiter.api.Test;

/** Telemetry unit tests */
public class TelemetryTest {
  private ObjectMapper mapper = new ObjectMapper();

  @Test
  public void testJsonConversion() {

    ObjectNode log1 = mapper.createObjectNode();
    log1.put("type", "query");
    log1.put("query_id", "sdasdasdasdasds");
    long timestamp1 = 12345678;
    ObjectNode log2 = mapper.createObjectNode();
    log2.put("type", "query");
    log2.put("query_id", "eqweqweqweqwe");
    long timestamp2 = 22345678;

    LinkedList<TelemetryData> list = new LinkedList<>();
    list.add(new TelemetryData(log1, timestamp1));
    list.add(new TelemetryData(log2, timestamp2));

    String result = TelemetryClient.logsToString(list);

    ObjectNode expect = mapper.createObjectNode();
    ArrayNode logs = mapper.createArrayNode();
    ObjectNode message1 = mapper.createObjectNode();
    message1.put("timestamp", timestamp1 + "");
    message1.set("message", log1);
    logs.add(message1);
    ObjectNode message2 = mapper.createObjectNode();
    message2.put("timestamp", timestamp2 + "");
    message2.set("message", log2);
    logs.add(message2);
    expect.set("logs", logs);

    assertEquals(expect.toString(), result);
  }
}
