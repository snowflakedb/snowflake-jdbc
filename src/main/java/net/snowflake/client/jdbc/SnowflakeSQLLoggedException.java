package net.snowflake.client.jdbc;

import net.minidev.json.JSONObject;
import net.snowflake.client.core.SFException;
import net.snowflake.client.jdbc.telemetryOOB.TelemetryEvent;
import net.snowflake.client.jdbc.telemetryOOB.TelemetryService;
import net.snowflake.client.util.SecretDetector;

public class SnowflakeSQLLoggedException extends SnowflakeSQLException {

    public TelemetryService telemetryInstance = TelemetryService.getInstance();


    public SnowflakeSQLLoggedException(String queryId, String reason, String sqlState, int vendorCode)
    {
        super(queryId, reason, sqlState, vendorCode);
        TelemetryEvent.LogBuilder logBuilder = new TelemetryEvent.LogBuilder();
        JSONObject value = new JSONObject();
        value.put("queryID", queryId);
        value.put("reason", SecretDetector.maskSecrets(reason));
        value.put("SQLState", sqlState);
        value.put("vendorCode", vendorCode);
        TelemetryEvent log = logBuilder
                .withException(this)
                .withValue(value)
                .build();
        telemetryInstance.report(log);
    }

    public SnowflakeSQLLoggedException(String sqlState, int vendorCode)
    {
        super(sqlState, vendorCode);
        TelemetryEvent.LogBuilder logBuilder = new TelemetryEvent.LogBuilder();
        JSONObject value = new JSONObject();
        value.put("SQLState", sqlState);
        value.put("vendorCode", vendorCode);
        TelemetryEvent log = logBuilder
                .withException(this)
                .withValue(value)
                .build();
        telemetryInstance.report(log);
    }

    public SnowflakeSQLLoggedException(String reason, String SQLState)
    {
        super(reason, SQLState);
        //telemetry
        TelemetryEvent.LogBuilder logBuilder = new TelemetryEvent.LogBuilder();
        JSONObject value = new JSONObject();
        value.put("reason", SecretDetector.maskSecrets(reason));
        value.put("SQLState", SQLState);
        TelemetryEvent log = logBuilder
                .withException(this)
                .withValue(value)
                .withTag("Meg", 25)
                .build();
        telemetryInstance.report(log);
    }

    public SnowflakeSQLLoggedException(String sqlState, int vendorCode, Object... params)
    {
        super(sqlState, vendorCode, params);
        //telemetry
        TelemetryEvent.LogBuilder logBuilder = new TelemetryEvent.LogBuilder();
        JSONObject value = new JSONObject();
        value.put("SQLState", sqlState);
        value.put("vendorCode", vendorCode);
        TelemetryEvent log = logBuilder
                .withException(this)
                .withValue(value)
                .withTag("Meg", 25)
                .build();
        telemetryInstance.report(log);
    }

    public SnowflakeSQLLoggedException(Throwable ex, ErrorCode errorCode, Object... params)
    {
        super(ex, errorCode, params);
        //telemetry

    }

    public SnowflakeSQLLoggedException(Throwable ex,
                                 String sqlState,
                                 int vendorCode,
                                 Object... params)
    {
        super(ex, sqlState, vendorCode, params);
        // telemetry

    }

    public SnowflakeSQLLoggedException(ErrorCode errorCode, Object... params)
    {
        super (errorCode, params);
        // telemetry
    }

    public SnowflakeSQLLoggedException(SFException e)
    {
        super (e);
        // add telemetry
    }

    public SnowflakeSQLLoggedException(String reason) {
        super(reason);
        // add telemetry
    }
}
