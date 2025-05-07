package net.snowflake.client.core;

import static net.snowflake.client.jdbc.SnowflakeUtil.isNullOrEmpty;

import net.minidev.json.JSONObject;
import net.snowflake.client.jdbc.telemetryOOB.TelemetryService;
import net.snowflake.client.util.TimeMeasurement;

public class ExecTimeTelemetryData {
  private final TimeMeasurement query = new TimeMeasurement();
  private final TimeMeasurement bind = new TimeMeasurement();
  private final TimeMeasurement gzip = new TimeMeasurement();
  private final TimeMeasurement httpClient = new TimeMeasurement();
  private final TimeMeasurement responseIOStream = new TimeMeasurement();
  private final TimeMeasurement processResultChunk = new TimeMeasurement();
  private final TimeMeasurement createResultSet = new TimeMeasurement();

  private String batchId;
  private String queryId;
  private String queryFunction;
  private int retryCount = 0;
  private String retryLocations = "";
  private Boolean ocspEnabled = false;
  boolean sendData = true;

  private String requestId;

  public ExecTimeTelemetryData(String queryFunction, String batchId) {
    this.query.setStart();
    this.queryFunction = queryFunction;
    this.batchId = batchId;
    if (!TelemetryService.getInstance().isHTAPEnabled()) {
      this.sendData = false;
    }
  }

  public ExecTimeTelemetryData() {
    this.sendData = false;
  }

  public void setBindStart() {
    bind.setStart();
  }

  public void setOCSPStatus(Boolean ocspEnabled) {
    this.ocspEnabled = ocspEnabled;
  }

  public void setBindEnd() {
    this.bind.setEnd();
  }

  public void setHttpClientStart() {
    httpClient.setStart();
  }

  public void setHttpClientEnd() {
    httpClient.setEnd();
  }

  public void setGzipStart() {
    gzip.setStart();
  }

  public void setGzipEnd() {
    gzip.setEnd();
  }

  public void setQueryEnd() {
    query.setEnd();
  }

  public void setQueryId(String queryId) {
    this.queryId = queryId;
  }

  public void setProcessResultChunkStart() {
    processResultChunk.setStart();
  }

  public void setProcessResultChunkEnd() {
    processResultChunk.setEnd();
  }

  public void setResponseIOStreamStart() {
    responseIOStream.setStart();
  }

  public void setResponseIOStreamEnd() {
    responseIOStream.setEnd();
  }

  public void setCreateResultSetStart() {
    createResultSet.setStart();
  }

  public void setCreateResultSetEnd() {
    createResultSet.setEnd();
  }

  public void incrementRetryCount() {
    this.retryCount++;
  }

  public void setRequestId(String requestId) {
    this.requestId = requestId;
  }

  public void addRetryLocation(String location) {
    if (isNullOrEmpty(this.retryLocations)) {
      this.retryLocations = location;
    } else {
      this.retryLocations = this.retryLocations.concat(", ").concat(location);
    }
  }

  long getTotalQueryTime() {
    return query.getTime();
  }

  long getResultProcessingTime() {
    if (createResultSet.getEnd() == 0 || processResultChunk.getStart() == 0) {
      return -1;
    }

    return createResultSet.getEnd() - processResultChunk.getStart();
  }

  long getHttpRequestTime() {
    return httpClient.getTime();
  }

  long getResultSetCreationTime() {
    return createResultSet.getTime();
  }

  public String generateTelemetry() {
    if (this.sendData) {
      String eventType = "ExecutionTimeRecord";
      JSONObject value = new JSONObject();
      String valueStr;
      value.put("eventType", eventType);
      value.put("QueryStart", this.query.getStart());
      value.put("BindStart", this.bind.getStart());
      value.put("BindEnd", this.bind.getEnd());
      value.put("GzipStart", this.gzip.getStart());
      value.put("GzipEnd", this.gzip.getEnd());
      value.put("HttpClientStart", this.httpClient.getStart());
      value.put("HttpClientEnd", this.httpClient.getEnd());
      value.put("ResponseIOStreamStart", this.responseIOStream.getStart());
      value.put("ResponseIOStreamEnd", this.responseIOStream.getEnd());
      value.put("ProcessResultChunkStart", this.processResultChunk.getStart());
      value.put("ProcessResultChunkEnd", this.processResultChunk.getEnd());
      value.put("CreateResultSetStart", this.createResultSet.getStart());
      value.put("CreateResultSetEnd", this.createResultSet.getEnd());
      value.put("QueryEnd", this.query.getEnd());
      value.put("BatchID", this.batchId);
      value.put("QueryID", this.queryId);
      value.put("RequestID", this.requestId);
      value.put("QueryFunction", this.queryFunction);
      value.put("RetryCount", this.retryCount);
      value.put("RetryLocations", this.retryLocations);
      value.put("ocspEnabled", this.ocspEnabled);
      value.put("ElapsedQueryTime", getTotalQueryTime());
      value.put("ElapsedResultProcessTime", getResultProcessingTime());
      value.put("Urgent", true);
      valueStr = value.toString(); // Avoid adding exception stacktrace to user logs.
      TelemetryService.getInstance().logExecutionTimeTelemetryEvent(value, eventType);
      return valueStr;
    }
    return "";
  }

  @SnowflakeJdbcInternalApi
  public String getLogString() {
    return "Query id: "
        + this.queryId
        + ", query function: "
        + this.queryFunction
        + ", batch id: "
        + this.batchId
        + ", request id: "
        + this.requestId
        + ", total query time: "
        + getTotalQueryTime() / 1000
        + " ms"
        + ", result processing time: "
        + getResultProcessingTime() / 1000
        + " ms"
        + ", result set creation time: "
        + getResultSetCreationTime() / 1000
        + " ms"
        + ", http request time: "
        + getHttpRequestTime() / 1000
        + " ms"
        + ", retry count: "
        + this.retryCount;
  }
}
