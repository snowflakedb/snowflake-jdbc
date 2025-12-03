package net.snowflake.client.core;

import static net.snowflake.client.jdbc.SnowflakeUtil.isNullOrEmpty;

import net.minidev.json.JSONObject;
import net.snowflake.client.jdbc.telemetry.CSVMetricsExporter;
import net.snowflake.client.jdbc.telemetryOOB.TelemetryService;
import net.snowflake.client.util.TimeMeasurement;

public class ExecTimeTelemetryData {
  // Measures time from when the client initiated a query (with executeQuery) until it is sent via
  // HTTP.
  private final TimeMeasurement executeToSend = new TimeMeasurement();
  // Measures time from when the client initiated a query (with executeQuery) until the control is
  // returned to the user.
  private final TimeMeasurement query = new TimeMeasurement();
  // Measures time from when binding preparation is started (including pushing to stage, if needed)
  // until it is ended.
  private final TimeMeasurement bind = new TimeMeasurement();
  // Measures time spent on compressing the request.
  private final TimeMeasurement gzip = new TimeMeasurement();
  // Measures time spent on HTTP roundtrip, except for downloading a response body.
  private final TimeMeasurement httpClient = new TimeMeasurement();
  // Measures time spent on response body download.
  private final TimeMeasurement responseIOStream = new TimeMeasurement();
  // Measures time spent on parsing result chunk.
  private final TimeMeasurement processResultChunk = new TimeMeasurement();
  // Measures time spent on creating a result set from parsed data.
  private final TimeMeasurement createResultSet = new TimeMeasurement();

  private String batchId;
  private String queryId;
  private String queryFunction;
  private int retryCount = 0;
  private String retryLocations = "";
  private Boolean ocspEnabled = false;
  boolean sendData = true;

  private String requestId;
  private String sessionId;
  private String queryText;

  public ExecTimeTelemetryData(String queryFunction, String batchId) {
    this.query.setStart();
    this.executeToSend.setStart();
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

  public void setExecuteToSendQueryEnd() {
    executeToSend.setEnd();
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

  public void setSessionId(String sessionId) {
    this.sessionId = sessionId;
  }

  public void setQueryText(String sql) {
    this.queryText = sql;
  }

  public String getSessionId() {
    return sessionId;
  }

  public String getQueryText() {
    return queryText;
  }

  public String getRequestId() {
    return requestId;
  }

  public String getQueryId() {
    return queryId;
  }

  public TimeMeasurement getExecuteToSend() {
    return executeToSend;
  }

  public TimeMeasurement getBind() {
    return bind;
  }

  public TimeMeasurement getGzip() {
    return gzip;
  }

  public TimeMeasurement getHttpClient() {
    return httpClient;
  }

  public TimeMeasurement getResponseIOStream() {
    return responseIOStream;
  }

  public TimeMeasurement getProcessResultChunk() {
    return processResultChunk;
  }

  public TimeMeasurement getCreateResultSet() {
    return createResultSet;
  }

  public TimeMeasurement getQuery() {
    return query;
  }

  public String generateTelemetry() {
    CSVMetricsExporter.getDefaultInstance().save(this);
    if (this.sendData) {
      String eventType = "ExecutionTimeRecord";
      JSONObject value = new JSONObject();
      String valueStr;
      value.put("eventType", eventType);
      value.put("QueryStart", this.query.getStart());
      value.put("ExecuteToSendStart", this.executeToSend.getStart());
      value.put("ExecuteToSendEnd", this.executeToSend.getEnd());
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
  public String toString() {
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
        + ", prepare to sent time: "
        + this.executeToSend.getTime() / 1000
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
