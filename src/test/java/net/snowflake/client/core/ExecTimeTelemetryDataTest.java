package net.snowflake.client.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;
import net.snowflake.client.jdbc.telemetryOOB.TelemetryService;
import org.junit.jupiter.api.Test;

public class ExecTimeTelemetryDataTest {

  @Test
  public void testExecTimeTelemetryData() throws ParseException {
    ExecTimeTelemetryData execTimeTelemetryData = new ExecTimeTelemetryData();
    execTimeTelemetryData.sendData = true;
    execTimeTelemetryData.setBindStart();
    execTimeTelemetryData.setOCSPStatus(true);
    execTimeTelemetryData.setBindEnd();
    execTimeTelemetryData.setHttpClientStart();
    execTimeTelemetryData.setHttpClientEnd();
    execTimeTelemetryData.setGzipStart();
    execTimeTelemetryData.setGzipEnd();
    execTimeTelemetryData.setQueryEnd();
    execTimeTelemetryData.setQueryId("queryid");
    execTimeTelemetryData.setProcessResultChunkStart();
    execTimeTelemetryData.setProcessResultChunkEnd();
    execTimeTelemetryData.setResponseIOStreamStart();
    execTimeTelemetryData.setResponseIOStreamEnd();
    execTimeTelemetryData.setCreateResultSetStart();
    execTimeTelemetryData.setCreateResultSetEnd();
    execTimeTelemetryData.incrementRetryCount();
    execTimeTelemetryData.setRequestId("mockId");
    execTimeTelemetryData.addRetryLocation("retry");

    String telemetry = execTimeTelemetryData.generateTelemetry();
    JSONParser parser = new JSONParser(JSONParser.MODE_JSON_SIMPLE);
    JSONObject json = (JSONObject) parser.parse(telemetry);
    assertNotNull(json.get("BindStart"));
    assertNotNull(json.get("BindEnd"));
    assertEquals(json.get("ocspEnabled"), true);
    assertNotNull(json.get("HttpClientStart"));
    assertNotNull(json.get("HttpClientEnd"));
    assertNotNull(json.get("GzipStart"));
    assertNotNull(json.get("GzipEnd"));
    assertNotNull(json.get("QueryEnd"));
    assertEquals(json.get("QueryID"), "queryid");
    assertNotNull(json.get("ProcessResultChunkStart"));
    assertNotNull(json.get("ProcessResultChunkEnd"));
    assertNotNull(json.get("ResponseIOStreamStart"));
    assertNotNull(json.get("CreateResultSetStart"));
    assertNotNull(json.get("CreateResultSetEnd"));
    assertNotNull(json.get("ElapsedQueryTime"));
    assertNotNull(json.get("ElapsedResultProcessTime"));
    assertNull(json.get("QueryFunction"));
    assertNull(json.get("BatchID"));
    assertEquals(((Long) json.get("RetryCount")).intValue(), 1);
    assertEquals(json.get("RequestID"), "mockId");
    assertEquals(json.get("RetryLocations"), "retry");
    assertEquals(json.get("Urgent"), true);
    assertEquals(json.get("eventType"), "ExecutionTimeRecord");
  }

  @Test
  public void testRetryLocation() throws ParseException {
    TelemetryService.enableHTAP();
    ExecTimeTelemetryData execTimeTelemetryData =
        new ExecTimeTelemetryData("queryFunction", "batchId");
    execTimeTelemetryData.addRetryLocation("hello");
    execTimeTelemetryData.addRetryLocation("world");
    execTimeTelemetryData.sendData = true;
    String telemetry = execTimeTelemetryData.generateTelemetry();

    JSONParser parser = new JSONParser(JSONParser.MODE_JSON_SIMPLE);
    JSONObject json = (JSONObject) parser.parse(telemetry);
    assertEquals(json.get("QueryFunction"), "queryFunction");
    assertEquals(json.get("BatchID"), "batchId");
    assertNotNull(json.get("QueryStart"));
    assertEquals(json.get("RetryLocations"), "hello, world");
    TelemetryService.disableHTAP();
  }
}
