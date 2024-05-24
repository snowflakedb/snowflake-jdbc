package net.snowflake.client.jdbc.diagnostic;

import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

abstract class DiagnosticCheck {
    protected String name;
    protected boolean success;

    private static final SFLogger logger =
            SFLoggerFactory.getLogger(DiagnosticCheck.class);
    public String name() {
        return this.name;
    }
    public void run(SnowflakeEndpoint snowflakeEndpoint){
        logger.debug("{}: {}", snowflakeEndpoint.getHost(), this.name);
    }
    public boolean isSuccess() {
        return this.success;
    }

    protected DiagnosticCheck(String name){
        this.name = name;
    }
}
