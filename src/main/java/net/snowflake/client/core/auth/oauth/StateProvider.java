/*
 * Copyright (c) 2024-2025 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core.auth.oauth;

import net.snowflake.client.core.SnowflakeJdbcInternalApi;

@SnowflakeJdbcInternalApi
public interface StateProvider<T> {
  T getState();
}
