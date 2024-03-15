package net.snowflake.client.jdbc.diagnostic;

/*
The SnowflakeEndpoint class represents an endpoint as returned by the System$allowlist() SQL
function. Example:

[{"type":"SNOWFLAKE_DEPLOYMENT","host":"snowhouse.snowflakecomputing.com","port":443},{"type":"SNOWFLAKE_DEPLOYMENT_REGIONLESS","host":"sfcogsops-snowhouse_aws_us_west_2.snowflakecomputing.com","port":443},{"type":"STAGE","host":"sfc-ds2-customer-stage.s3.amazonaws.com","port":443},{"type":"STAGE","host":"sfc-ds2-customer-stage.s3.us-west-2.amazonaws.com","port":443},{"type":"STAGE","host":"sfc-ds2-customer-stage.s3-us-west-2.amazonaws.com","port":443},{"type":"SNOWSQL_REPO","host":"sfc-repo.snowflakecomputing.com","port":443},{"type":"OUT_OF_BAND_TELEMETRY","host":"client-telemetry.snowflakecomputing.com","port":443},{"type":"OCSP_CACHE","host":"ocsp.snowflakecomputing.com","port":80},{"type":"DUO_SECURITY","host":"api-35a58de5.duosecurity.com","port":443},{"type":"CLIENT_FAILOVER","host":"sfcogsops-snowhouseprimary.snowflakecomputing.com","port":443},{"type":"OCSP_RESPONDER","host":"o.ss2.us","port":80},{"type":"OCSP_RESPONDER","host":"ocsp.r2m02.amazontrust.com","port":80},{"type":"OCSP_RESPONDER","host":"ocsp.sca1b.amazontrust.com","port":80},{"type":"OCSP_RESPONDER","host":"ocsp.rootg2.amazontrust.com","port":80},{"type":"OCSP_RESPONDER","host":"ocsp.rootca1.amazontrust.com","port":80},{"type":"SNOWSIGHT_DEPLOYMENT","host":"app.snowflake.com","port":443},{"type":"SNOWSIGHT_DEPLOYMENT","host":"apps-api.c1.us-west-2.aws.app.snowflake.com","port":443}]

 */
public class SnowflakeEndpoint {
    private String type;
    private String host;
    private int port;
    private boolean isSecure;

    public SnowflakeEndpoint(String type, String host, int port) {
        this.type = type;
        this.host = host;
        this.port = port;
        this.isSecure = (this.port == 443) ? true : false;
    }

    public SnowflakeEndpoint() {
        this(null, null, -1);
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getType() {
        return this.type;
    }

    public String getHost() {
        return this.host;
    }

    public boolean isSslEnabled() { return this.isSecure; }

    public int getPort() {
        return this.port;
    }

    //We can only tell if private link is enabled for certain hosts when the hostname contains
    //the word 'privatelink' but we don't have a good way of telling if a private link connection
    //is expected for internal stages for example.
    public boolean isPrivateLink() {
        return (host.contains("privatelink.snowflakecomputing.com"));
    }

    public String toString() {
        return this.host + ":" + this.port;
    }
}
