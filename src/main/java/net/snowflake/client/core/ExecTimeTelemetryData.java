/*
 * Copyright (c) 2018-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core;

import com.google.common.base.Strings;
import net.minidev.json.JSONObject;
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

  public ExecTimeTelemetryData(long queryStart, String queryFunction, String batchId) {
    this.queryStart = queryStart;
    this.queryFunction = queryFunction;
    this.batchId = batchId;
  }

  public ExecTimeTelemetryData() {
    this.sendData = false;
  }

  public void setBindStart(long bindStart) {
    this.bindStart = bindStart;
  }

  public void setOCSPStatus(Boolean ocspEnabled) {
    this.ocspEnabled = ocspEnabled;
  }

  public void setBindEnd(long bindEnd) {
    this.bindEnd = bindEnd;
  }

  public void setHttpClientStart(long httpClientStart) {
    this.httpClientStart = httpClientStart;
  }

  public void setHttpClientEnd(long httpClientEnd) {
    this.httpClientEnd = httpClientEnd;
  }

  public void setGzipStart(long gzipStart) {
    this.gzipStart = gzipStart;
  }

  public void setGzipEnd(long gzipEnd) {
    this.gzipEnd = gzipEnd;
  }

  public void setQueryEnd(long queryEnd) {
    this.queryEnd = queryEnd;
  }

  public void setQueryId(String queryId) {
    this.queryId = queryId;
  }

  public void setProcessResultChunkStart(long processResultChunkStart) {
    this.processResultChunkStart = processResultChunkStart;
  }

  public void setProcessResultChunkEnd(long processResultChunkEnd) {
    this.processResultChunkEnd = processResultChunkEnd;
  }

  public void setResponseIOStreamStart(long ioStreamStart) {
    this.responseIOStreamStart = ioStreamStart;
  }

  public void setResponseIOStreamEnd(long ioStreamEnd) {
    this.responseIOStreamEnd = ioStreamEnd;
  }

  public void setCreateResultSetStart(long createResultSetStart) {
    this.createResultSetStart = createResultSetStart;
  }

  public void setCreateResultSetEnd(long createResultSetEnd) {
    this.createResultSetEnd = createResultSetEnd;
  }

  public void incrementRetryCount() {
    this.retryCount++;
  }

  public void addRetryLocation(String location) {
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
