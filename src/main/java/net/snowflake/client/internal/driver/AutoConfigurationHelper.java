package net.snowflake.client.internal.driver;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.StringJoiner;
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

      Map<String, String> provenance = new HashMap<>();
      ConnectionParameters params =
          SFConnectionConfigParser.buildConnectionParameters(url, provenance);
      if (params == null) {
        throw new SnowflakeSQLException(
            "Unavailable connection configuration parameters expected for "
                + "auto configuration using file");
      }

      if (info != null && !info.isEmpty()) {
        mergeInfoPropertiesIntoParams(params.getParams(), info, provenance);
      }

      logProvenance(provenance);

      return params;
    } else {
      return new ConnectionParameters(url, info);
    }
  }

  /**
   * Merge user-provided Properties into the resolved connection parameters. Properties override
   * both TOML and URL values (consistent with standard JDBC semantics where Properties overwrite
   * URL query params). Uses alias-aware put to prevent duplicate property errors downstream.
   */
  private static void mergeInfoPropertiesIntoParams(
      Properties resolved, Properties info, Map<String, String> provenance) {
    Map<String, String> resolvedMap = new HashMap<>();
    for (String key : resolved.stringPropertyNames()) {
      resolvedMap.put(key, resolved.getProperty(key));
    }

    for (Map.Entry<Object, Object> entry : info.entrySet()) {
      String key = entry.getKey().toString();
      String value = entry.getValue().toString();
      SFConnectionConfigParser.putResolvingAliases(resolvedMap, key, value, provenance, "PROP");
    }

    resolved.clear();
    resolved.putAll(resolvedMap);
  }

  private static void logProvenance(Map<String, String> provenance) {
    if (!logger.isDebugEnabled()) {
      return;
    }
    StringJoiner sj = new StringJoiner(", ");
    for (Map.Entry<String, String> entry : provenance.entrySet()) {
      sj.add(entry.getKey() + "(" + entry.getValue() + ")");
    }
    logger.debug("Auto-configuration resolved properties: [{}]", sj.toString());
    provenance.clear();
  }
}
