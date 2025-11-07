package net.snowflake.client.internal.core.auth.oauth;

import net.snowflake.client.internal.core.SnowflakeJdbcInternalApi;

@SnowflakeJdbcInternalApi
public interface StateProvider<T> {
  T getState();
}
