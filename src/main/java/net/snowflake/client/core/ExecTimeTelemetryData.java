/*
 * Copyright (c) 2018-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core;

import com.google.common.base.Strings;
import net.minidev.json.JSONObject;
import net.snowflake.client.jdbc.SnowflakeUtil;
import net.snowflake.client.jdbc.telemetryOOB.TelemetryService;

public class ExecTimeTelemetryData {
  private long queryStart;
  private long bindStart;
  private long bindEnd;
  private long gzipStart;
  private long gzipEnd;
  private long httpClientStart;
  private long httpClientEnd;
  private long responseIOStreamStart;
  private long responseIOStreamEnd;
  private long processResultChunkStart;
  private long processResultChunkEnd;
  private long createResultSetStart;
  private long createResultSetEnd;
  private long queryEnd;
  private String batchId;
  private String queryId;
  private String queryFunction;
  private int retryCount = 0;
  private String retryLocations = "";
  private Boolean ocspEnabled = false;
  boolean sendData = true;

  private String requestId;

  public ExecTimeTelemetryData(String queryFunction, String batchId) {
    this.queryStart = SnowflakeUtil.getEpochTimeInMicroSeconds();
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
    this.bindStart = SnowflakeUtil.getEpochTimeInMicroSeconds();
  }

  public void setOCSPStatus(Boolean ocspEnabled) {
    this.ocspEnabled = ocspEnabled;
  }

  public void setBindEnd() {
    this.bindEnd = SnowflakeUtil.getEpochTimeInMicroSeconds();
  }

  public void setHttpClientStart() {
    this.httpClientStart = SnowflakeUtil.getEpochTimeInMicroSeconds();
  }

  public void setHttpClientEnd() {
    this.httpClientEnd = SnowflakeUtil.getEpochTimeInMicroSeconds();
  }

  public void setGzipStart() {
    this.gzipStart = SnowflakeUtil.getEpochTimeInMicroSeconds();
  }

  public void setGzipEnd() {
    this.gzipEnd = SnowflakeUtil.getEpochTimeInMicroSeconds();
  }

  public void setQueryEnd() {
    this.queryEnd = SnowflakeUtil.getEpochTimeInMicroSeconds();
  }

  public void setQueryId(String queryId) {
    this.queryId = queryId;
  }

  public void setProcessResultChunkStart() {
    this.processResultChunkStart = SnowflakeUtil.getEpochTimeInMicroSeconds();
  }

  public void setProcessResultChunkEnd() {
    this.processResultChunkEnd = SnowflakeUtil.getEpochTimeInMicroSeconds();
  }

  public void setResponseIOStreamStart() {
    this.responseIOStreamStart = SnowflakeUtil.getEpochTimeInMicroSeconds();
  }

  public void setResponseIOStreamEnd() {
    this.responseIOStreamEnd = SnowflakeUtil.getEpochTimeInMicroSeconds();
  }

  public void setCreateResultSetStart() {
    this.createResultSetStart = SnowflakeUtil.getEpochTimeInMicroSeconds();
  }

  public void setCreateResultSetEnd() {
    this.createResultSetEnd = SnowflakeUtil.getEpochTimeInMicroSeconds();
  }

  public void incrementRetryCount() {
    this.retryCount++;
  }

  public void setRequestId(String requestId) {
    this.requestId = requestId;
  }

  public void addRetryLocation(String location) {
    if (Strings.isNullOrEmpty(this.retryLocations)) {
      this.retryLocations = location;
    } else {
      this.retryLocations = this.retryLocations.concat(", ").concat(location);
    }
  }

  public long getTotalQueryTime() {
    if (queryStart == 0 || queryEnd == 0) {
      return -1;
    }

    return queryEnd - queryStart;
  }

  public long getResultProcessingTime() {
    if (createResultSetEnd == 0 || processResultChunkStart == 0) {
      return -1;
    }

    return createResultSetEnd - processResultChunkStart;
  }

  public long getHttpRequestTime() {
    if (httpClientStart == 0 || httpClientEnd == 0) {
      return -1;
    }

    return httpClientEnd - httpClientStart;
  }

  public long getResultSetCreationTime() {
    if (createResultSetStart == 0 || createResultSetEnd == 0) {
      return -1;
    }

    return createResultSetEnd - createResultSetStart;
  }

  public String generateTelemetry() {
    if (this.sendData) {
      String eventType = "ExecutionTimeRecord";
      JSONObject value = new JSONObject();
      String valueStr;
      value.put("eventType", eventType);
      value.put("QueryStart", this.queryStart);
      value.put("BindStart", this.bindStart);
      value.put("BindEnd", this.bindEnd);
      value.put("GzipStart", this.gzipStart);
      value.put("GzipEnd", this.gzipEnd);
      value.put("HttpClientStart", this.httpClientStart);
      value.put("HttpClientEnd", this.httpClientEnd);
      value.put("ResponseIOStreamStart", this.responseIOStreamStart);
      value.put("ResponseIOStreamEnd", this.responseIOStreamEnd);
      value.put("ProcessResultChunkStart", this.processResultChunkStart);
      value.put("ProcessResultChunkEnd", this.processResultChunkEnd);
      value.put("CreateResultSetStart", this.createResultSetStart);
      value.put("CreatResultSetEnd", this.createResultSetEnd);
      value.put("QueryEnd", this.queryEnd);
      value.put("BatchID", this.batchId);
      value.put("QueryID", this.queryId);
      value.put("RequestID", this.requestId);
      value.put("QueryFunction", this.queryFunction);
      value.put("RetryCount", this.retryCount);
      value.put("RetryLocations", this.retryLocations);
      value.put("ocspEnabled", this.ocspEnabled);
      value.put("ElapsedQueryTime", getTotalQueryTime());
      value.put(
          "ElapsedResultProcessTime", getResultProcessingTime());
      value.put("Urgent", true);
      valueStr = value.toString(); // Avoid adding exception stacktrace to user logs.
      TelemetryService.getInstance().logExecutionTimeTelemetryEvent(value, eventType);
      return valueStr;
    }
    return "";
  }

  public String getLogString() {
    return "Query id: " + this.queryId
            + ", query function: " + this.queryFunction
            + ", batch id: " + this.batchId
            + ", request id: " + this.requestId
            + ", total query time: " + getTotalQueryTime() / 1000 + " ms"
            + ", result processing time: " + getResultProcessingTime() / 1000 + " ms"
            + ", result set creation time: " + getResultSetCreationTime() / 1000 + " ms"
            + ", http request time: " + getHttpRequestTime() / 1000 + " ms"
            + ", retry count: " + this.retryCount;
  }
}
