package net.snowflake.client.internal.driver;

import java.util.Properties;
import net.snowflake.client.api.exception.SnowflakeSQLException;
import net.snowflake.client.internal.config.ConnectionParameters;
import net.snowflake.client.internal.config.SFConnectionConfigParser;
import net.snowflake.client.internal.log.SFLogger;
import net.snowflake.client.internal.log.SFLoggerFactory;

/**
 * Helper for handling JDBC auto-configuration via connections.toml file.
 *
 * <p>Auto-configuration allows users to specify "jdbc:snowflake:auto" as the connection URL and
 * have connection parameters loaded from a configuration file. This provides a convenient way to
 * manage connection settings without hardcoding them in application code.
 *
 * <p>Configuration files are typically located in:
 *
 * <ul>
 *   <li>~/.snowflake/connections.toml (user-level)
 *   <li>Project-specific locations as configured
 * </ul>
 */
public final class AutoConfigurationHelper {
  private static final SFLogger logger = SFLoggerFactory.getLogger(AutoConfigurationHelper.class);

  /**
   * The URL prefix that indicates auto-configuration should be used.
   *
   * <p>When a connection URL starts with or equals this prefix, the driver will attempt to load
   * connection parameters from a configuration file.
   */
  public static final String AUTO_CONNECTION_PREFIX = "jdbc:snowflake:auto";

  private AutoConfigurationHelper() {
    // Utility class - prevent instantiation
  }

  /**
   * Check if the URL indicates auto-configuration should be used.
   *
   * @param url the JDBC connection URL
   * @return true if auto-configuration is enabled
   */
  public static boolean isAutoConfigurationUrl(String url) {
    return url != null && url.contains(AUTO_CONNECTION_PREFIX);
  }

  /**
   * Load connection parameters from configuration file or use provided parameters.
   *
   * <p>If the URL contains the auto-configuration prefix, this method attempts to load parameters
   * from the configuration file. Otherwise, it creates ConnectionParameters from the provided URL
   * and info Properties.
   *
   * @param url JDBC connection URL
   * @param info connection properties
   * @return ConnectionParameters with resolved configuration
   * @throws SnowflakeSQLException if auto-configuration is requested but fails
   */
  public static ConnectionParameters resolveConnectionParameters(String url, Properties info)
      throws SnowflakeSQLException {

    if (isAutoConfigurationUrl(url)) {
      logger.debug(
          "JDBC connection initializing with URL '{}'. Autoconfiguration is enabled.",
          AUTO_CONNECTION_PREFIX);

      ConnectionParameters params = SFConnectionConfigParser.buildConnectionParameters(url);
      if (params == null) {
        throw new SnowflakeSQLException(
            "Unavailable connection configuration parameters expected for "
                + "auto configuration using file");
      }
      return params;
    } else {
      return new ConnectionParameters(url, info);
    }
  }
}
