/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core;

@SnowflakeJdbcInternalApi
public enum CancellationReason {
  UNKNOWN,
  CLIENT_REQUESTED,
  TIMEOUT
}
