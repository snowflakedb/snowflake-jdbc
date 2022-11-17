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
    if (TelemetryService.getInstance().isHTAPEnabled()) {
      this.queryStart = SnowflakeUtil.getEpochTimeInMicroSeconds();
      this.queryFunction = queryFunction;
      this.batchId = batchId;
    } else {
      this.sendData = false;
    }
  }

  public ExecTimeTelemetryData() {
    this.sendData = false;
  }

  public void setBindStart() {
    if (!this.sendData) return;
    this.bindStart = SnowflakeUtil.getEpochTimeInMicroSeconds();
  }

  public void setOCSPStatus(Boolean ocspEnabled) {
    if (!this.sendData) return;
    this.ocspEnabled = ocspEnabled;
  }

  public void setBindEnd() {
    if (!this.sendData) return;
    this.bindEnd = SnowflakeUtil.getEpochTimeInMicroSeconds();
  }

  public void setHttpClientStart() {
    if (!this.sendData) return;
    this.httpClientStart = SnowflakeUtil.getEpochTimeInMicroSeconds();
  }

  public void setHttpClientEnd() {
    if (!this.sendData) return;
    this.httpClientEnd = SnowflakeUtil.getEpochTimeInMicroSeconds();
  }

  public void setGzipStart() {
    if (!this.sendData) return;
    this.gzipStart = SnowflakeUtil.getEpochTimeInMicroSeconds();
  }

  public void setGzipEnd() {
    if (!this.sendData) return;
    this.gzipEnd = SnowflakeUtil.getEpochTimeInMicroSeconds();
  }

  public void setQueryEnd() {
    if (!this.sendData) return;
    this.queryEnd = SnowflakeUtil.getEpochTimeInMicroSeconds();
  }

  public void setQueryId(String queryId) {
    if (!this.sendData) return;
    this.queryId = queryId;
  }

  public void setProcessResultChunkStart() {
    if (!this.sendData) return;
    this.processResultChunkStart = SnowflakeUtil.getEpochTimeInMicroSeconds();
  }

  public void setProcessResultChunkEnd() {
    if (!this.sendData) return;
    this.processResultChunkEnd = SnowflakeUtil.getEpochTimeInMicroSeconds();
  }

  public void setResponseIOStreamStart() {
    if (!this.sendData) return;
    this.responseIOStreamStart = SnowflakeUtil.getEpochTimeInMicroSeconds();
  }

  public void setResponseIOStreamEnd() {
    if (!this.sendData) return;
    this.responseIOStreamEnd = SnowflakeUtil.getEpochTimeInMicroSeconds();
  }

  public void setCreateResultSetStart() {
    if (!this.sendData) return;
    this.createResultSetStart = SnowflakeUtil.getEpochTimeInMicroSeconds();
  }

  public void setCreateResultSetEnd() {
    if (!this.sendData) return;
    this.createResultSetEnd = SnowflakeUtil.getEpochTimeInMicroSeconds();
  }

  public void incrementRetryCount() {
    if (!this.sendData) return;
    this.retryCount++;
  }

  public void setRequestId(String requestId) {
    if (!this.sendData) return;
    this.requestId = requestId;
  }

  public void addRetryLocation(String location) {
    if (!this.sendData) return;
    if (Strings.isNullOrEmpty(this.retryLocations)) {
      this.retryLocations = location;
    } else {
      this.retryLocations = this.retryLocations.concat(", ").concat(location);
    }
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
      value.put("ElapsedQueryTime", (this.queryEnd - this.queryStart));
      value.put(
          "ElapsedResultProcessTime", (this.createResultSetEnd - this.processResultChunkStart));
      value.put("Urgent", true);
      valueStr = value.toString(); // Avoid adding exception stacktrace to user logs.
      TelemetryService.getInstance().logExecutionTimeTelemetryEvent(value, eventType);
      return valueStr;
    }
    return "";
  }
}
