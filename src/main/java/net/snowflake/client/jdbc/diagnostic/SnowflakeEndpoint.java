package net.snowflake.client.jdbc.diagnostic;

import net.snowflake.client.core.PrivateLinkDetector;

/*
The SnowflakeEndpoint class represents an endpoint as returned by the System$allowlist() SQL
function. Example:

[{"type":"SNOWFLAKE_DEPLOYMENT","host":"snowhouse.snowflakecomputing.com","port":443},{"type":"SNOWFLAKE_DEPLOYMENT_REGIONLESS","host":"sfcogsops-snowhouse_aws_us_west_2.snowflakecomputing.com","port":443},{"type":"STAGE","host":"sfc-ds2-customer-stage.s3.amazonaws.com","port":443},{"type":"STAGE","host":"sfc-ds2-customer-stage.s3.us-west-2.amazonaws.com","port":443},{"type":"STAGE","host":"sfc-ds2-customer-stage.s3-us-west-2.amazonaws.com","port":443},{"type":"SNOWSQL_REPO","host":"sfc-repo.snowflakecomputing.com","port":443},{"type":"OUT_OF_BAND_TELEMETRY","host":"client-telemetry.snowflakecomputing.com","port":443},{"type":"OCSP_CACHE","host":"ocsp.snowflakecomputing.com","port":80},{"type":"DUO_SECURITY","host":"api-35a58de5.duosecurity.com","port":443},{"type":"CLIENT_FAILOVER","host":"sfcogsops-snowhouseprimary.snowflakecomputing.com","port":443},{"type":"OCSP_RESPONDER","host":"o.ss2.us","port":80},{"type":"OCSP_RESPONDER","host":"ocsp.r2m02.amazontrust.com","port":80},{"type":"OCSP_RESPONDER","host":"ocsp.sca1b.amazontrust.com","port":80},{"type":"OCSP_RESPONDER","host":"ocsp.rootg2.amazontrust.com","port":80},{"type":"OCSP_RESPONDER","host":"ocsp.rootca1.amazontrust.com","port":80},{"type":"SNOWSIGHT_DEPLOYMENT","host":"app.snowflake.com","port":443},{"type":"SNOWSIGHT_DEPLOYMENT","host":"apps-api.c1.us-west-2.aws.app.snowflake.com","port":443}]

 */
class SnowflakeEndpoint {
  private final String type;
  private final String host;
  private final int port;
  private final boolean isSecure;

  public SnowflakeEndpoint(String type, String host, int port) {
    this.type = type;
    this.host = host;
    this.port = port;
    this.isSecure = (this.port == 443);
  }

  public String getType() {
    return this.type;
  }

  public String getHost() {
    return this.host;
  }

  public boolean isSslEnabled() {
    return this.isSecure;
  }

  public int getPort() {
    return this.port;
  }

  public boolean isPrivateLink() {
    return PrivateLinkDetector.isPrivateLink(host);
  }

  @Override
  public String toString() {
    return this.host + ":" + this.port;
  }

  @Override
  public boolean equals(Object o) {
    boolean isSnowflakeEndpoint = o instanceof SnowflakeEndpoint;
    if (!isSnowflakeEndpoint) {
      return false;
    }
    if (!((SnowflakeEndpoint) o).getHost().equals(this.host)) {
      return false;
    }
    if (((SnowflakeEndpoint) o).getPort() != this.port) {
      return false;
    }

    if (!((SnowflakeEndpoint) o).getType().equals(this.type)) {
      return false;
    }

    return true;
  }
}
