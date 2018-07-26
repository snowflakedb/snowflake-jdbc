/*
 * Copyright (c) 2012-2018 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.jdbc;

/**
 * SnowflakeReauthenticationRequest signals the reauthentication used for SSO
 */
public class SnowflakeReauthenticationRequest extends SnowflakeSQLException
{
  public SnowflakeReauthenticationRequest(String queryId,
                                          String reason,
                                          String sqlState,
                                          int vendorCode)
  {
    super(queryId, reason, sqlState, vendorCode);
  }
}
