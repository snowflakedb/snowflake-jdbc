/*
 * Copyright (c) 2012-2018 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.jdbc;

/**
 * SnowflakeReauthenticateException signals the reauthentication mainly used
 * for SSO
 */
public class SnowflakeReauthenticateException extends SnowflakeSQLException
{
  public SnowflakeReauthenticateException(String queryId,
                                          String reason,
                                          String sqlState,
                                          int vendorCode)
  {
    super(queryId, reason, sqlState, vendorCode);
  }
}
