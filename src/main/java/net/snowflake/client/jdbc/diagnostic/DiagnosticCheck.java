package net.snowflake.client.jdbc.diagnostic;

abstract class DiagnosticCheck {
    protected String name;
    protected boolean success;
    public String name() {
        return this.name;
    }
    public void run(SnowflakeEndpoint snowflakeEndpoint){ }
    public boolean isSuccess() {
        return this.success;
    }

    protected DiagnosticCheck(String name){
        this.name = name;
    }
}
