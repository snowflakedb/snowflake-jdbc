/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import net.minidev.json.JSONObject;
import net.snowflake.client.core.SFException;
import net.snowflake.client.jdbc.telemetryOOB.TelemetryEvent;
import net.snowflake.client.jdbc.telemetryOOB.TelemetryService;
import net.snowflake.client.util.SecretDetector;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * @author mknister
 *
 * This SnowflakeSQLLoggedException class extends the SnowflakeSQLException class to add OOB telemetry data for sql
 * exceptions. Not all sql exceptions require OOB telemetry logging so the exceptions in this class should only be
 * thrown if there is a need for logging the exception with OOB telemetry.
 */

public class SnowflakeSQLLoggedException extends SnowflakeSQLException {

    public TelemetryService telemetryInstance = TelemetryService.getInstance();

    /**
     *
     * @param value JSONnode containing relevant information specific to the exception constructor that
     *              should be included in the telemetry data, such as sqlState or vendorCode
     * @param ex The exception being thrown
     */
    private void buildExceptionTelemetryLog(JSONObject value, SnowflakeSQLLoggedException ex)
    {
        TelemetryEvent.LogBuilder logBuilder = new TelemetryEvent.LogBuilder();
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        ex.printStackTrace(pw);
        String stackTrace = sw.toString();
        value.put("Stacktrace", stackTrace);
        TelemetryEvent log = logBuilder
                .withName("Exception: " + ex.getMessage())
                .withValue(value)
                .build();
        telemetryInstance.report(log);
    }

    public SnowflakeSQLLoggedException(String queryId, String reason, String SQLState, int vendorCode)
    {
        super(queryId, reason, SQLState, vendorCode);
        // add telemetry
        JSONObject value = new JSONObject();
        value.put("SQLState", SQLState);
        value.put("Query ID", queryId);
        value.put("Vendor Code", vendorCode);
        value.put("reason", SecretDetector.maskSecrets(reason));
        buildExceptionTelemetryLog(value, this);
    }

    public SnowflakeSQLLoggedException(String SQLState, int vendorCode)
    {
        super(SQLState, vendorCode);
        // add telemetry
        JSONObject value = new JSONObject();
        value.put("SQLState", SQLState);
        value.put("Vendor Code", vendorCode);
        buildExceptionTelemetryLog(value, this);
    }

    public SnowflakeSQLLoggedException(String reason, String SQLState)
    {
        super(reason, SQLState);
        // add telemetry
        JSONObject value = new JSONObject();
        value.put("SQLState", SQLState);
        value.put("reason", SecretDetector.maskSecrets(reason));
        buildExceptionTelemetryLog(value, this);
    }

    public SnowflakeSQLLoggedException(String SQLState, int vendorCode, Object... params)
    {
        super(SQLState, vendorCode, params);
        // add telemetry
        String errorMessage = errorResourceBundleManager.getLocalizedMessage(String.valueOf(vendorCode), params);
        JSONObject value = new JSONObject();
        value.put("SQLState", SQLState);
        value.put("error message", errorMessage);
        value.put("vendorCode", vendorCode);
        buildExceptionTelemetryLog(value, this);
    }

    public SnowflakeSQLLoggedException(Throwable ex, ErrorCode errorCode, Object... params)
    {
        super(ex, errorCode, params);
        // add telemetry
        String errorMessage = errorResourceBundleManager.getLocalizedMessage(String.valueOf(errorCode.getMessageCode()), params);
        JSONObject value = new JSONObject();
        value.put("SQLState", errorCode.getSqlState());
        value.put("error message", errorMessage);
        value.put("VendorCode", errorCode.getMessageCode());
        value.put("Error code", errorCode);
        buildExceptionTelemetryLog(value, this);
    }

    public SnowflakeSQLLoggedException(Throwable ex,
                                       String SQLState,
                                       int vendorCode,
                                       Object... params)
    {
        super(ex, SQLState, vendorCode, params);
        // add telemetry
        String errorMessage = errorResourceBundleManager.getLocalizedMessage(String.valueOf(vendorCode), params);
        JSONObject value = new JSONObject();
        value.put("error message", errorMessage);
        value.put("VendorCode", vendorCode);
        buildExceptionTelemetryLog(value, this);
    }

    public SnowflakeSQLLoggedException(ErrorCode errorCode, Object... params)
    {
        super (errorCode, params);
        // add telemetry
        String errorMessage = errorResourceBundleManager.getLocalizedMessage(String.valueOf(errorCode.getMessageCode()), params);
        JSONObject value = new JSONObject();
        value.put("error message", errorMessage);
        value.put("errorCode", errorCode);
        buildExceptionTelemetryLog(value, this);
    }

    public SnowflakeSQLLoggedException(SFException e)
    {
        super (e);
        // add telemetry
        buildExceptionTelemetryLog(null, this);
    }

    public SnowflakeSQLLoggedException(String reason) {
        super(reason);
        // add telemetry
        JSONObject value = new JSONObject();
        value.put("reason", SecretDetector.maskSecrets(reason));
        buildExceptionTelemetryLog(value, this);
    }
}
