package net.snowflake.client.jdbc;

/** SnowflakeReauthenticationRequest signals the reauthentication used for SSO */
public class SnowflakeReauthenticationRequest extends SnowflakeSQLException {
  private static final long serialVersionUID = 1L;

  public SnowflakeReauthenticationRequest(
      String queryId, String reason, String sqlState, int vendorCode) {
    super(queryId, reason, sqlState, vendorCode);
  }
}
