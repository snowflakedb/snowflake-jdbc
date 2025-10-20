package net.snowflake.client.util;

import net.snowflake.client.core.SnowflakeJdbcInternalApi;

/**
 * Interface for providing environment variables to enable thread-safe testing. This abstraction
 * allows dependency injection of environment variable access, making code testable with instance
 * mocks (as opposed to static SnowflakeUtil) that work across threads.
 */
@SnowflakeJdbcInternalApi
public interface EnvironmentProvider {

  String getEnv(String name);
}
