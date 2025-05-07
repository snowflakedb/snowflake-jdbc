package net.snowflake.client.core.auth.oauth;

import net.snowflake.client.core.SnowflakeJdbcInternalApi;

@SnowflakeJdbcInternalApi
public interface StateProvider<T> {
  T getState();
}
