package net.snowflake.client.util;

import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.jdbc.SnowflakeUtil;

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
