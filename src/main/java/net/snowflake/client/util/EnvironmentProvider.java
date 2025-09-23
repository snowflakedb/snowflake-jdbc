package net.snowflake.client.util;

import net.snowflake.client.core.SnowflakeJdbcInternalApi;

/**
 * Interface for providing environment variables to enable thread-safe testing.
 * This abstraction allows dependency injection of environment variable access,
 * making code testable with instance mocks that work across threads.
 */
@SnowflakeJdbcInternalApi
public interface EnvironmentProvider {
    
    /**
     * Get the value of an environment variable.
     * 
     * @param name the name of the environment variable
     * @return the value of the environment variable, or null if not set
     */
    String getEnv(String name);
} 