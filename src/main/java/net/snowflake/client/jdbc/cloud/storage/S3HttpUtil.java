package net.snowflake.client.jdbc.cloud.storage;

import static net.snowflake.client.jdbc.SnowflakeUtil.isNullOrEmpty;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import java.util.Properties;
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

@SnowflakeJdbcInternalApi
public class S3HttpUtil {
  private static final SFLogger logger = SFLoggerFactory.getLogger(HttpUtil.class);

  /**
   * A static function to set S3 proxy params when there is a valid session
   *
   * @param key key to HttpClient map containing OCSP and proxy info
   * @param clientConfig the configuration needed by S3 to set the proxy
   */
  public static void setProxyForS3(HttpClientSettingsKey key, ClientConfiguration clientConfig) {
    if (key != null && key.usesProxy()) {
      clientConfig.setProxyProtocol(
          key.getProxyHttpProtocol() == HttpProtocol.HTTPS ? Protocol.HTTPS : Protocol.HTTP);
      clientConfig.setProxyHost(key.getProxyHost());
      clientConfig.setProxyPort(key.getProxyPort());
      clientConfig.setNonProxyHosts(key.getNonProxyHosts());
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
        clientConfig.setProxyUsername(key.getProxyUser());
        clientConfig.setProxyPassword(key.getProxyPassword());
      }
      logger.debug(logMessage);
    } else {
      logger.debug("Omitting S3 proxy setup");
    }
  }

  /**
   * A static function to set S3 proxy params for sessionless connections using the proxy params
   * from the StageInfo
   *
   * @param proxyProperties proxy properties
   * @param clientConfig the configuration needed by S3 to set the proxy
   * @throws SnowflakeSQLException when an error is encountered
   */
  public static void setSessionlessProxyForS3(
      Properties proxyProperties, ClientConfiguration clientConfig) throws SnowflakeSQLException {
    // do nothing yet
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
        Protocol protocolEnum =
            (!isNullOrEmpty(proxyProtocol) && proxyProtocol.equalsIgnoreCase("https"))
                ? Protocol.HTTPS
                : Protocol.HTTP;
        clientConfig.setProxyHost(proxyHost);
        clientConfig.setProxyPort(proxyPort);
        clientConfig.setNonProxyHosts(nonProxyHosts);
        clientConfig.setProxyProtocol(protocolEnum);
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
          clientConfig.setProxyUsername(proxyUser);
          clientConfig.setProxyPassword(proxyPassword);
        }
        logger.debug(logMessage);
      } else {
        logger.debug("Omitting sessionless S3 proxy setup as proxy is disabled");
      }
    } else {
      logger.debug("Omitting sessionless S3 proxy setup");
    }
  }
}
