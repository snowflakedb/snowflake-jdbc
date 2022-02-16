/*
 * Copyright (c) 2018-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core;

import net.minidev.json.JSONObject;
import net.snowflake.client.jdbc.telemetryOOB.TelemetryService;

import java.security.cert.CertificateException;

public class ExecTimeTelemetryData {
    private long queryStart;
    private long bindStart;
    private long bindEnd;
    private long httpClientStart;
    private long httpClientEnd;
    private long gzipStart;
    private long gzipEnd;
    private long queryEnd;
    private String batchId;
    private String queryFunction;
    private Boolean didRetry;
    private Boolean ocspEnabled;

    public ExecTimeTelemetryData(long queryStart, String queryFunction) {
        this.queryStart = queryStart;
        this.queryFunction = queryFunction;
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

    public String generateTelemetry(String eventType, CertificateException ex) {
        JSONObject value = new JSONObject();
        String valueStr;
        value.put("eventType", eventType);
        //value.put("sfcPeerHost", this.sfcPeerHost);
        value.put("bindTime", this.bindTime);
        value.put("encodingTime", this.encodingTime);
        value.put("executeQuery", this.executeQuery);
        value.put("httpRequest", this.httpRequest);
        value.put("beforeHttpLib", this.beforeHttpLib);
        value.put("afterHttpLib", this.afterHttpLib);
        value.put("end2end", this.end2end);
        value.put("didRetry", this.didRetry);
        value.put("ocspEnabled", this.ocspEnabled);
        valueStr = value.toString(); // Avoid adding exception stacktrace to user logs.
        TelemetryService.getInstance().logExecutionTimeTelemetryEvent(eventType, value);
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
}
