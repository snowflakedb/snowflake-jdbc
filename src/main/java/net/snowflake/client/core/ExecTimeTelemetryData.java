/*
 * Copyright (c) 2018-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core;

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
    private long processResultChunkStart;
    private long processResultChunkEnd;
    private long queryEnd;
    private String batchId;
    private String queryId;
    private String queryFunction;
    private Boolean didRetry = false;
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

    public void setRetry(Boolean didRetry) {
        this.didRetry = didRetry;
    }

    public void setOCSPStatus(Boolean ocspEnabled) {
        this.ocspEnabled = ocspEnabled;
    }

    public String generateTelemetry() {
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
        value.put("ProcessResultChunkStart", this.processResultChunkStart);
        value.put("ProcessResultChunkEnd", this.processResultChunkEnd);
        value.put("QueryEnd", this.queryEnd);
        value.put("BatchID", this.batchId);
        value.put("QueryID", this.queryId);
        value.put("QueryFunction", this.queryFunction);
        value.put("DidRetry", this.didRetry);
        value.put("ocspEnabled", this.ocspEnabled);
        valueStr = value.toString(); // Avoid adding exception stacktrace to user logs.
        TelemetryService.getInstance().logExecutionTimeTelemetryEvent(value, eventType);
        return valueStr;
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

    public void setBatchId(String batchId) {
        this.batchId = batchId;
    }

    public void setQueryFunction(String queryFunction) {
        this.queryFunction = queryFunction;
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
}
