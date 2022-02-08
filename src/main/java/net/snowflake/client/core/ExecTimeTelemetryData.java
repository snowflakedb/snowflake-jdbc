/*
 * Copyright (c) 2018-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core;

import java.math.BigDecimal;
import java.security.cert.CertificateException;
import net.minidev.json.JSONObject;
import net.snowflake.client.jdbc.telemetryOOB.TelemetryService;

public class ExecTimeTelemetryData {
    private long bindTime;
    private long encodingTime;
    private long executeQuery;
    private long httpRequest;
    private long beforeHttpLib;
    private long afterHttpLib;
    private long end2end;
    private Boolean didRetry;
    private Boolean ocspEnabled;

    public ExecTimeTelemetryData(
            long bindTime,
            long encodingTime,
            long executeQuery,
            long httpRequest,
            long beforeHttpLib,
            long afterHttpLib,
            long end2end,
            Boolean didRetry,
            Boolean ocspEnabled) {
        this.bindTime = bindTime;
        this.encodingTime = encodingTime;
        this.executeQuery = executeQuery;
        this.httpRequest = httpRequest;
        this.beforeHttpLib = beforeHttpLib;
        this.afterHttpLib = afterHttpLib;
        this.end2end = end2end;
        this.didRetry = didRetry;
        this.ocspEnabled = ocspEnabled;
    }

    public void setBindTime(long bindTime) {
        this.bindTime = bindTime;
    }

    public void setEncodingTime(long encodingTime) {
        this.encodingTime = encodingTime;
    }

    public void setExecuteQuery(long executeQuery) {
        this.executeQuery = executeQuery;
    }

    public void setHttpRequest(long httpRequest) {
        this.httpRequest = httpRequest;
    }

    public void setBeforeHttpLib(long beforeHttpLib) {
        this.beforeHttpLib = beforeHttpLib;
    }

    public void setAfterHttpLib(long afterHttpLib) {
        this.afterHttpLib = afterHttpLib;
    }

    public void setEnd2end(long end2end) {
        this.end2end = end2end;
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
        value.put("sfcPeerHost", this.sfcPeerHost);
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
}
