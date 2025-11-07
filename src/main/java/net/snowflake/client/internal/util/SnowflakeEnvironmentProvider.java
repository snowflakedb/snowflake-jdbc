package net.snowflake.client.internal.util;

import net.snowflake.client.internal.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.internal.jdbc.SnowflakeUtil;

/**
 * Implementation of EnvironmentProvider that delegates to SnowflakeUtil. This wrapper enables
 * thread-safe testing while maintaining existing behavior.
 */
@SnowflakeJdbcInternalApi
public class SnowflakeEnvironmentProvider implements EnvironmentProvider {

  @Override
  public String getEnv(String name) {
    return SnowflakeUtil.systemGetEnv(name);
  }
}
