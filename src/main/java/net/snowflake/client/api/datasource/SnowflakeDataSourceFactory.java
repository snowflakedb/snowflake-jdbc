package net.snowflake.client.api.datasource;

import net.snowflake.client.internal.api.implementation.datasource.SnowflakeBasicDataSource;

/**
 * Factory for creating {@link SnowflakeDataSource} instances.
 *
 * <p>This factory provides methods to create different types of Snowflake DataSource
 * implementations. Use this factory instead of directly instantiating DataSource classes.
 *
 * <p><b>Example usage:</b>
 *
 * <pre>{@code
 * SnowflakeDataSource ds = SnowflakeDataSourceFactory.createDataSource();
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
 * @see SnowflakeDataSource
 */
public final class SnowflakeDataSourceFactory {

  private SnowflakeDataSourceFactory() {
    throw new AssertionError("SnowflakeDataSourceFactory cannot be instantiated");
  }

  /**
   * Creates a new non-pooled Snowflake DataSource.
   *
   * <p>This DataSource creates a new physical connection for each {@link
   * javax.sql.DataSource#getConnection()} call. For applications that require connection pooling,
   * consider using an external connection pool manager (e.g., HikariCP, Apache DBCP) with this
   * DataSource.
   *
   * @return a new {@link SnowflakeDataSource} instance
   */
  public static SnowflakeDataSource createDataSource() {
    return new SnowflakeBasicDataSource();
  }
}
