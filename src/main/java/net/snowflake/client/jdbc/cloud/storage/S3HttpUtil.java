package net.snowflake.client.jdbc.cloud.storage;

import static net.snowflake.client.jdbc.SnowflakeUtil.isNullOrEmpty;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import net.snowflake.client.core.HttpClientSettingsKey;
import net.snowflake.client.core.HttpProtocol;
import net.snowflake.client.core.HttpUtil;
import net.snowflake.client.core.SFSessionProperty;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.client.log.SFLoggerUtil;
import software.amazon.awssdk.http.nio.netty.ProxyConfiguration;

@SnowflakeJdbcInternalApi
public class S3HttpUtil {
  private static final SFLogger logger = SFLoggerFactory.getLogger(HttpUtil.class);

  /**
   * A static function to create S3 proxy configuration when there is a valid session
   *
   * @param key key to HttpClient map containing OCSP and proxy info
   * @return ProxyConfiguration or null if no proxy is configured
   */
  public static ProxyConfiguration createProxyConfigurationForS3(HttpClientSettingsKey key) {
    if (key != null && key.usesProxy()) {
      String scheme = key.getProxyHttpProtocol() == HttpProtocol.HTTPS ? "https" : "http";

      ProxyConfiguration.Builder proxyBuilder =
          ProxyConfiguration.builder()
              .scheme(scheme)
              .host(key.getProxyHost())
              .port(key.getProxyPort())
              .useEnvironmentVariableValues(false)
              .useSystemPropertyValues(false);

      if (key.getNonProxyHosts() != null && !key.getNonProxyHosts().isEmpty()) {
        Set<String> nonProxyHostsSet =
            new HashSet<>(Arrays.asList(key.getNonProxyHosts().split("\\|")));
        proxyBuilder.nonProxyHosts(nonProxyHostsSet);
      }

      String logMessage =
          "Setting S3 proxy. Host: "
              + key.getProxyHost()
              + ", port: "
              + key.getProxyPort()
              + ", protocol: "
              + key.getProxyHttpProtocol()
              + ", non-proxy hosts: "
              + key.getNonProxyHosts();

      if (!isNullOrEmpty(key.getProxyUser()) && !isNullOrEmpty(key.getProxyPassword())) {
        logMessage +=
            ", user: "
                + key.getProxyUser()
                + ", password is "
                + SFLoggerUtil.isVariableProvided(key.getProxyPassword());
        proxyBuilder.username(key.getProxyUser());
        proxyBuilder.password(key.getProxyPassword());
      }
      logger.debug(logMessage);
      return proxyBuilder.build();
    } else {
      logger.debug("Omitting S3 proxy setup");
      return null;
    }
  }

  /**
   * A static function to create S3 proxy configuration for sessionless connections using the proxy
   * params from the StageInfo
   *
   * @param proxyProperties proxy properties
   * @return ProxyConfiguration for AWS SDK v2, or null if no proxy is configured
   * @throws SnowflakeSQLException when an error is encountered
   */
  public static ProxyConfiguration createSessionlessProxyConfigurationForS3(
      Properties proxyProperties) throws SnowflakeSQLException {
    if (proxyProperties != null
        && proxyProperties.size() > 0
        && proxyProperties.getProperty(SFSessionProperty.USE_PROXY.getPropertyKey()) != null) {
      Boolean useProxy =
          Boolean.valueOf(
              proxyProperties.getProperty(SFSessionProperty.USE_PROXY.getPropertyKey()));
      if (useProxy) {
        // set up other proxy related values.
        String proxyHost =
            proxyProperties.getProperty(SFSessionProperty.PROXY_HOST.getPropertyKey());
        int proxyPort;
        try {
          proxyPort =
              Integer.parseInt(
                  proxyProperties.getProperty(SFSessionProperty.PROXY_PORT.getPropertyKey()));
        } catch (NumberFormatException | NullPointerException e) {
          throw new SnowflakeSQLException(
              ErrorCode.INVALID_PROXY_PROPERTIES, "Could not parse port number");
        }
        String proxyUser =
            proxyProperties.getProperty(SFSessionProperty.PROXY_USER.getPropertyKey());
        String proxyPassword =
            proxyProperties.getProperty(SFSessionProperty.PROXY_PASSWORD.getPropertyKey());
        String nonProxyHosts =
            proxyProperties.getProperty(SFSessionProperty.NON_PROXY_HOSTS.getPropertyKey());
        String proxyProtocol =
            proxyProperties.getProperty(SFSessionProperty.PROXY_PROTOCOL.getPropertyKey());

        String scheme =
            (!isNullOrEmpty(proxyProtocol) && proxyProtocol.equalsIgnoreCase("https"))
                ? "https"
                : "http";

        ProxyConfiguration.Builder proxyBuilder =
            ProxyConfiguration.builder()
                .scheme(scheme)
                .host(proxyHost)
                .port(proxyPort)
                .useEnvironmentVariableValues(false)
                .useSystemPropertyValues(false);

        if (!isNullOrEmpty(nonProxyHosts)) {
          Set<String> nonProxyHostsSet =
              Arrays.stream(nonProxyHosts.split("\\|"))
                  .map(String::trim)
                  .collect(Collectors.toSet());
          proxyBuilder.nonProxyHosts(nonProxyHostsSet);
        }

        String logMessage =
            "Setting sessionless S3 proxy. Host: "
                + proxyHost
                + ", port: "
                + proxyPort
                + ", non-proxy hosts: "
                + nonProxyHosts
                + ", protocol: "
                + proxyProtocol;
        if (!isNullOrEmpty(proxyUser) && !isNullOrEmpty(proxyPassword)) {
          logMessage += ", user: " + proxyUser + " with password provided";
          proxyBuilder.username(proxyUser);
          proxyBuilder.password(proxyPassword);
        }
        logger.debug(logMessage);
        return proxyBuilder.build();
      } else {
        logger.debug("Omitting sessionless S3 proxy setup as proxy is disabled");
        return null;
      }
    } else {
      logger.debug("Omitting sessionless S3 proxy setup");
      return null;
    }
  }
}
