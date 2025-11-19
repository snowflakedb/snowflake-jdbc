package net.snowflake.client.api.pooling;

import net.snowflake.client.internal.api.implementation.pooling.SnowflakeConnectionPoolDataSource;

/**
 * Factory for creating {@link SnowflakeConnectionPoolDataSource} instances.
 *
 * <p>This factory provides methods to create different types of Snowflake Connection Pool Data
 * Source implementations. Use this factory instead of directly instantiating Connection Pool Data
 * Source classes.
 *
 * <p><b>Example usage:</b>
 *
 * <pre>{@code
 * SnowflakeConnectionPoolDataSource ds = SnowflakeConnectionPoolDataSourceFactory.createConnectionPoolDataSource();
 * ds.setAccount("myaccount");
 * ds.setUser("myuser");
 * ds.setPassword("mypassword");
 * ds.setDatabase("mydb");
 * ds.setSchema("myschema");
 * ds.setWarehouse("mywh");
 *
 * try (Connection conn = ds.getConnection()) {
 *   // use connection
 * }
 * }</pre>
 *
 * @see SnowflakeConnectionPoolDataSource
 */
public class SnowflakeConnectionPoolDataSourceFactory {

  private SnowflakeConnectionPoolDataSourceFactory() {
    throw new AssertionError("SnowflakeConnectionPoolDataSourceFactory cannot be instantiated");
  }

  /**
   * Creates a new SnowflakeConnectionPoolDataSource instance.
   *
   * @return a new SnowflakeConnectionPoolDataSource instance
   */
  public static SnowflakeConnectionPoolDataSource createConnectionPoolDataSource() {
    return new SnowflakeConnectionPoolDataSource();
  }
}
