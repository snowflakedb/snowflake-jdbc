package net.snowflake.client.core;

@SnowflakeJdbcInternalApi
public enum CancellationReason {
  UNKNOWN,
  CLIENT_REQUESTED,
  TIMEOUT
}
