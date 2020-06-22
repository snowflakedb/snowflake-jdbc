package net.snowflake.client.jdbc;

import net.minidev.json.JSONObject;
import net.snowflake.client.jdbc.telemetryOOB.TelemetryEvent;
import net.snowflake.client.jdbc.telemetryOOB.TelemetryService;
import net.snowflake.client.util.SecretDetector;

public class SnowflakeSQLLoggedException extends SnowflakeSQLException {

    public TelemetryService telemetryInstance = TelemetryService.getInstance();

    String eventName = "SQLException";

    public SnowflakeSQLLoggedException(String queryId, String reason, String sqlState, int vendorCode)
    {
        super(queryId, reason, sqlState, vendorCode);
        TelemetryEvent.LogBuilder logBuilder = new TelemetryEvent.LogBuilder();
        JSONObject value = new JSONObject();
        value.put("queryID", queryId);
        value.put("reason", SecretDetector.maskSecrets(reason));
        value.put("SQLState", sqlState);
        value.put("vendorCode", vendorCode);
        TelemetryEvent log = logBuilder.withName(eventName)
                .build();
        telemetryInstance.report(log);
        //telemetry
    }

    public SnowflakeSQLLoggedException(String sqlState, int vendorCode)
    {
        super(sqlState, vendorCode);
        //telemetry
        TelemetryEvent.LogBuilder logBuilder = new TelemetryEvent.LogBuilder();
        TelemetryEvent log = logBuilder.build();
        telemetryInstance.report(log);
    }

    public SnowflakeSQLLoggedException(String reason, String SQLState)
    {
        super(reason, SQLState);
        //telemetry
        TelemetryEvent.LogBuilder logBuilder = new TelemetryEvent.LogBuilder();
        TelemetryEvent log = logBuilder.build();
        telemetryInstance.report(log);
    }

}
