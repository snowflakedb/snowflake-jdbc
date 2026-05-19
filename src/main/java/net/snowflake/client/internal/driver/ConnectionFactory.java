package net.snowflake.client.internal.driver;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import net.snowflake.client.api.exception.SnowflakeSQLException;
import net.snowflake.client.internal.api.implementation.connection.SnowflakeConnectionImpl;
import net.snowflake.client.internal.config.ConnectionParameters;
import net.snowflake.client.internal.jdbc.SnowflakeConnectString;
import net.snowflake.client.internal.log.SFLogger;
import net.snowflake.client.internal.log.SFLoggerFactory;

/**
 * Factory for creating Snowflake JDBC connections.
 *
 * <p>This class handles the validation and creation of connections from JDBC URLs and properties.
 * It supports both standard connection URLs and auto-configuration.
 *
 * <p>This class is thread-safe and stateless.
 */
public final class ConnectionFactory {
  private static final SFLogger logger = SFLoggerFactory.getLogger(ConnectionFactory.class);

  private ConnectionFactory() {
    // Utility class - prevent instantiation
  }

  /**
   * Creates a connection to the Snowflake database.
   *
   * <p>This method validates the URL, resolves connection parameters (including auto-configuration
   * if applicable), parses the connection string, and creates a new connection instance.
   *
   * @param url the database URL
   * @param info additional connection properties
   * @return a Connection object, or null if the URL is not accepted by this driver (per JDBC spec)
   * @throws SQLException if a database access error occurs or the connection parameters are invalid
   */
  public static Connection createConnection(String url, Properties info) throws SQLException {
    // Resolve connection parameters (handles auto-configuration if needed)
    ConnectionParameters params = AutoConfigurationHelper.resolveConnectionParameters(url, info);

    // Validate URL is not null
    if (params.getUrl() == null) {
      throw new SnowflakeSQLException("Unable to connect to url of 'null'.");
    }

    // Check if URL has supported prefix (return null if not, per JDBC spec)
    if (!SnowflakeConnectString.hasSupportedPrefix(params.getUrl())) {
      // Per JDBC spec: Driver.connect() should return null if URL is not recognized
      return null;
    }

    // Parse and validate the connection string
    SnowflakeConnectString connectString =
        SnowflakeConnectString.parse(params.getUrl(), params.getParams());

    if (!connectString.isValid()) {
      throw new SnowflakeSQLException("Connection string is invalid. Unable to parse.");
    }

    // Build a defensive copy of the resolved Properties so the user's original
    // `info` is never mutated. Without the copy, two concurrent createConnection
    // calls sharing the same `info` race on the internal carrier keys below
    // (ConnectionFactory puts them in here; DefaultSFConnectionHandler.initialize
    // removes them after consumption). The copy is cheap — Properties is a
    // HashMap-backed bag — and the safety guarantee composes naturally with
    // user code that pools or templates Properties instances.
    Properties effective = new Properties();
    effective.putAll(params.getParams());

    // Transfer deferred log messages from ConnectionParameters to Properties so they survive
    // into DefaultSFConnectionHandler.initialize() for replay after initLogger().
    List<String> deferred = params.getDeferredLogMessages();
    if (deferred != null && !deferred.isEmpty()) {
      effective.put(AutoConfigurationHelper.DEFERRED_LOG_MESSAGES_KEY, deferred);
    }

    // TODO(SNOW-3548350): Forward the captured ConnectionIdentifierShape onto the effective
    // Properties so DefaultSFConnectionHandler.initialize() can attach it to the SFSession
    // before open() emits the client_connection_identifier_shape telemetry. Remove together
    // with the rest of the connection-identifier-shape plumbing.
    if (params.getConnectionIdentifierShape() != null) {
      effective.put(
          AutoConfigurationHelper.CONNECTION_IDENTIFIER_SHAPE_KEY,
          params.getConnectionIdentifierShape());
    }

    // Create and return the connection implementation
    return new SnowflakeConnectionImpl(params.getUrl(), effective);
  }

  /**
   * Creates a connection using auto-configuration.
   *
   * <p>This is a convenience method that uses the auto-configuration URL prefix to load connection
   * parameters from the connections.toml file.
   *
   * @return a Connection object
   * @throws SQLException if a database access error occurs or configuration cannot be loaded
   */
  public static Connection createConnectionWithAutoConfig() throws SQLException {
    logger.debug("Creating connection with auto-configuration");
    return createConnection(AutoConfigurationHelper.AUTO_CONNECTION_PREFIX, null);
  }
}
