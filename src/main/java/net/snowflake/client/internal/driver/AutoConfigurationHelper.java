package net.snowflake.client.internal.driver;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringJoiner;
import java.util.TreeMap;
import net.snowflake.client.api.exception.SnowflakeSQLException;
import net.snowflake.client.internal.config.ConnectionParameters;
import net.snowflake.client.internal.config.SFConnectionConfigParser;
import net.snowflake.client.internal.core.SFException;
import net.snowflake.client.internal.core.SFSessionProperty;
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
  /**
   * Internal key used to carry deferred log messages on the Properties object. The value is a
   * List&lt;String&gt; (not a String), so it won't interfere with session property parsing.
   * ConnectionParameters is discarded by ConnectionFactory before reaching
   * DefaultSFConnectionHandler, so we stash the messages on Properties which does flow through.
   */
  public static final String DEFERRED_LOG_MESSAGES_KEY = "_snowflake.internal.deferredLogMessages";

  public static ConnectionParameters resolveConnectionParameters(String url, Properties info)
      throws SnowflakeSQLException {

    if (isAutoConfigurationUrl(url)) {
      List<String> deferredMessages = new ArrayList<>();

      String initMsg =
          String.format(
              "JDBC connection initializing with URL '%s'. Autoconfiguration is enabled.",
              AUTO_CONNECTION_PREFIX);
      logger.debug(initMsg);
      deferredMessages.add(initMsg);

      Map<String, String> provenance = new HashMap<>();
      ConnectionParameters params =
          SFConnectionConfigParser.buildConnectionParameters(url, provenance, deferredMessages);
      if (params == null) {
        throw new SnowflakeSQLException(
            "Unavailable connection configuration parameters expected for "
                + "auto configuration using file");
      }

      if (info != null && !info.isEmpty()) {
        try {
          mergeInfoPropertiesIntoParams(params.getParams(), info, provenance);
        } catch (SFException e) {
          throw new SnowflakeSQLException(
              e.getQueryId(), e, e.getSqlState(), e.getVendorCode(), e.getParams());
        }
      }

      logAutoConfigProvenance(provenance, deferredMessages);

      params.getParams().put(DEFERRED_LOG_MESSAGES_KEY, deferredMessages);

      return params;
    } else {
      return new ConnectionParameters(url, info);
    }
  }

  /**
   * Merge user-provided Properties into the resolved connection parameters. Properties override
   * both TOML and URL values (consistent with standard JDBC semantics where Properties overwrite
   * URL query params). Uses alias-aware put to prevent duplicate property errors downstream.
   * Non-String values (e.g. PrivateKey, HttpHeadersCustomizer) are preserved directly in the final
   * Properties without alias resolution since they can only come from the Properties object.
   */
  private static void mergeInfoPropertiesIntoParams(
      Properties resolved, Properties info, Map<String, String> provenance) throws SFException {
    Map<String, String> resolvedMap = new HashMap<>();
    for (String key : resolved.stringPropertyNames()) {
      resolvedMap.put(key, resolved.getProperty(key));
    }

    Map<String, Object> nonStringEntries = new HashMap<>();
    for (Map.Entry<Object, Object> entry : info.entrySet()) {
      String key = entry.getKey().toString();
      Object value = entry.getValue();
      if (value instanceof String) {
        SFSessionProperty.putResolvingAliases(resolvedMap, key, (String) value, provenance, "PROP");
      } else {
        nonStringEntries.put(key, value);
        if (provenance != null) {
          provenance.put(key, "PROP");
        }
      }
    }

    resolved.clear();
    resolved.putAll(resolvedMap);
    resolved.putAll(nonStringEntries);
  }

  // Only covers the jdbc:snowflake:auto path. The standard jdbc:snowflake:// path merges
  // URL+Properties in SnowflakeConnectString.parse without provenance tracking.
  private static void logAutoConfigProvenance(
      Map<String, String> provenance, List<String> deferredMessages) {
    StringJoiner sj = new StringJoiner(", ");
    for (Map.Entry<String, String> entry : new TreeMap<>(provenance).entrySet()) {
      sj.add(entry.getKey() + "(" + entry.getValue() + ")");
    }
    String provenanceMsg =
        String.format("Auto-configuration resolved properties: [%s]", sj.toString());
    logger.debug(provenanceMsg);
    if (deferredMessages != null) {
      deferredMessages.add(provenanceMsg);
    }
    provenance.clear();
  }
}
