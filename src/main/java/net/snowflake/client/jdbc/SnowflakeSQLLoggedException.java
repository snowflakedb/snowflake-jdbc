package net.snowflake.client.jdbc;

import net.minidev.json.JSONObject;
import net.snowflake.client.core.SFException;
import net.snowflake.client.jdbc.telemetryOOB.TelemetryEvent;
import net.snowflake.client.jdbc.telemetryOOB.TelemetryService;

import java.io.PrintWriter;
import java.io.StringWriter;

public class SnowflakeSQLLoggedException extends SnowflakeSQLException {

    public TelemetryService telemetryInstance = TelemetryService.getInstance();

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

    public SnowflakeSQLLoggedException(String queryId, String reason, String sqlState, int vendorCode)
    {
        super(queryId, reason, sqlState, vendorCode);
        // add telemetry
        JSONObject value = new JSONObject();
        value.put("SQLState", sqlState);
        value.put("Query ID", queryId);
        value.put("Vendor Code", vendorCode);
        buildExceptionTelemetryLog(value, this);

    }

    public SnowflakeSQLLoggedException(String sqlState, int vendorCode)
    {
        super(sqlState, vendorCode);
        // add telemetry
        JSONObject value = new JSONObject();
        value.put("SQLState", sqlState);
        value.put("Vendor Code", vendorCode);
        buildExceptionTelemetryLog(value, this);
    }

    public SnowflakeSQLLoggedException(String reason, String SQLState)
    {
        super(reason, SQLState);
        // add telemetry
        JSONObject value = new JSONObject();
        value.put("SQLState", SQLState);
        buildExceptionTelemetryLog(value, this);
    }

    public SnowflakeSQLLoggedException(String sqlState, int vendorCode, Object... params)
    {
        super(sqlState, vendorCode, params);
        // add telemetry
        String errorMessage = errorResourceBundleManager.getLocalizedMessage(String.valueOf(vendorCode), params);
        JSONObject value = new JSONObject();
        value.put("SQLState", sqlState);
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
                                 String sqlState,
                                 int vendorCode,
                                 Object... params)
    {
        super(ex, sqlState, vendorCode, params);
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
        value.put("reason", reason);
        buildExceptionTelemetryLog(value, this);
    }
}
