package net.snowflake.client.util;

import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.jdbc.SnowflakeUtil;

/**
 * Production implementation of EnvironmentProvider that delegates to SnowflakeUtil.
 * This wrapper enables thread-safe testing while maintaining existing behavior.
 */
@SnowflakeJdbcInternalApi
public class SnowflakeEnvironmentProvider implements EnvironmentProvider {
    
    /**
     * Singleton instance for production use.
     */
    public static final SnowflakeEnvironmentProvider INSTANCE = new SnowflakeEnvironmentProvider();
    
    /**
     * Private constructor to enforce singleton pattern.
     */
    private SnowflakeEnvironmentProvider() {
    }
    
    /**
     * Get environment variable value by delegating to SnowflakeUtil.systemGetEnv.
     * This maintains the existing behavior including security exception handling.
     * 
     * @param name the name of the environment variable
     * @return the value of the environment variable, or null if not set or on security exception
     */
    @Override
    public String getEnv(String name) {
        return SnowflakeUtil.systemGetEnv(name);
    }
} 