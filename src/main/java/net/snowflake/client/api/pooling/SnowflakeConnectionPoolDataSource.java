package net.snowflake.client.api.pooling;

import javax.sql.ConnectionPoolDataSource;
import net.snowflake.client.api.datasource.SnowflakeDataSource;

/**
 * SnowflakeConnectionPoolDataSource is the interface for a connection pool data source. Its
 * implementation is instantiated by {@link SnowflakeConnectionPoolDataSourceFactory}.
 */
public interface SnowflakeConnectionPoolDataSource
    extends ConnectionPoolDataSource, SnowflakeDataSource {}
