package net.snowflake.client.jdbc;

import net.snowflake.client.core.SFException;

public class SnowflakeSQLLoggedException extends SnowflakeSQLException {

    public SnowflakeSQLLoggedException(String queryId, String reason, String sqlState, int vendorCode)
    {
        super(queryId, reason, sqlState, vendorCode);
        // add telemetry
    }

    public SnowflakeSQLLoggedException(String sqlState, int vendorCode)
    {
        super(sqlState, vendorCode);
        // add telemetry
    }

    public SnowflakeSQLLoggedException(String reason, String SQLState)
    {
        super(reason, SQLState);
        // add telemetry
    }

    public SnowflakeSQLLoggedException(String sqlState, int vendorCode, Object... params)
    {
        super(sqlState, vendorCode, params);
        // add telemetry
    }

    public SnowflakeSQLLoggedException(Throwable ex, ErrorCode errorCode, Object... params)
    {
        super(ex, errorCode, params);
        // add telemetry

    }

    public SnowflakeSQLLoggedException(Throwable ex,
                                 String sqlState,
                                 int vendorCode,
                                 Object... params)
    {
        super(ex, sqlState, vendorCode, params);
        // add telemetry

    }

    public SnowflakeSQLLoggedException(ErrorCode errorCode, Object... params)
    {
        super (errorCode, params);
        // add telemetry
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
