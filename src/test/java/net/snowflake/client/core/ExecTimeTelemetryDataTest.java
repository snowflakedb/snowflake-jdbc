package net.snowflake.client.core;

import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;
import net.snowflake.client.jdbc.telemetryOOB.TelemetryService;
import org.junit.jupiter.api.Assertions;
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
    Assertions.assertNotNull(json.get("BindStart"));
    Assertions.assertNotNull(json.get("BindEnd"));
    Assertions.assertEquals(json.get("ocspEnabled"), true);
    Assertions.assertNotNull(json.get("HttpClientStart"));
    Assertions.assertNotNull(json.get("HttpClientEnd"));
    Assertions.assertNotNull(json.get("GzipStart"));
    Assertions.assertNotNull(json.get("GzipEnd"));
    Assertions.assertNotNull(json.get("QueryEnd"));
    Assertions.assertEquals(json.get("QueryID"), "queryid");
    Assertions.assertNotNull(json.get("ProcessResultChunkStart"));
    Assertions.assertNotNull(json.get("ProcessResultChunkEnd"));
    Assertions.assertNotNull(json.get("ResponseIOStreamStart"));
    Assertions.assertNotNull(json.get("CreateResultSetStart"));
    Assertions.assertNotNull(json.get("CreateResultSetEnd"));
    Assertions.assertNotNull(json.get("ElapsedQueryTime"));
    Assertions.assertNotNull(json.get("ElapsedResultProcessTime"));
    Assertions.assertNull(json.get("QueryFunction"));
    Assertions.assertNull(json.get("BatchID"));
    Assertions.assertEquals(((Long) json.get("RetryCount")).intValue(), 1);
    Assertions.assertEquals(json.get("RequestID"), "mockId");
    Assertions.assertEquals(json.get("RetryLocations"), "retry");
    Assertions.assertEquals(json.get("Urgent"), true);
    Assertions.assertEquals(json.get("eventType"), "ExecutionTimeRecord");
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
    Assertions.assertEquals(json.get("QueryFunction"), "queryFunction");
    Assertions.assertEquals(json.get("BatchID"), "batchId");
    Assertions.assertNotNull(json.get("QueryStart"));
    Assertions.assertEquals(json.get("RetryLocations"), "hello, world");
    TelemetryService.disableHTAP();
  }
}
