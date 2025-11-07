package net.snowflake.client.internal.core;

@SnowflakeJdbcInternalApi
public enum CancellationReason {
  UNKNOWN,
  CLIENT_REQUESTED,
  TIMEOUT
}
