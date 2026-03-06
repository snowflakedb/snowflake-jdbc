package net.snowflake.client.internal.jdbc.cloud.storage;

import static net.snowflake.client.internal.jdbc.SnowflakeUtil.isNullOrEmpty;

import com.azure.core.http.ProxyOptions;
import com.google.api.client.http.apache.v2.ApacheHttpTransport;
import com.google.auth.http.HttpTransportFactory;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import net.snowflake.client.api.exception.ErrorCode;
import net.snowflake.client.api.exception.SnowflakeSQLException;
import net.snowflake.client.internal.core.HttpClientSettingsKey;
import net.snowflake.client.internal.core.HttpProtocol;
import net.snowflake.client.internal.core.SFSessionProperty;
import net.snowflake.client.internal.core.SdkProxyRoutePlanner;
import net.snowflake.client.internal.jdbc.SnowflakeUtil;
import net.snowflake.client.internal.log.SFLogger;
import net.snowflake.client.internal.log.SFLoggerFactory;
import net.snowflake.client.internal.log.SFLoggerUtil;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;
import software.amazon.awssdk.http.nio.netty.ProxyConfiguration;

public class CloudStorageProxyFactory {
  private static final SFLogger logger = SFLoggerFactory.getLogger(CloudStorageProxyFactory.class);

  // ── Extraction (shared logic) ──────────────────────────────────────────────

  /**
   * Extracts proxy settings from a session's HttpClientSettingsKey.
   *
   * @return ProxySettings or null if no proxy is configured
   */
  static ProxySettings extractFromKey(HttpClientSettingsKey key) {
    if (key != null && key.usesProxy()) {
      return new ProxySettings(
          key.getProxyHost(),
          key.getProxyPort(),
          key.getProxyHttpProtocol(),
          key.getProxyUser(),
          key.getProxyPassword(),
          key.getNonProxyHosts());
    }
    return null;
  }

  /**
   * Extracts proxy settings from sessionless proxy properties.
   *
   * @return ProxySettings or null if no proxy is configured
   * @throws SnowflakeSQLException on invalid port number
   */
  static ProxySettings extractFromProperties(Properties proxyProperties)
      throws SnowflakeSQLException {
    if (proxyProperties != null
        && proxyProperties.size() > 0
        && proxyProperties.getProperty(SFSessionProperty.USE_PROXY.getPropertyKey()) != null) {
      Boolean useProxy =
          Boolean.valueOf(
              proxyProperties.getProperty(SFSessionProperty.USE_PROXY.getPropertyKey()));
      if (useProxy) {
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

        HttpProtocol protocol =
            (!isNullOrEmpty(proxyProtocol) && proxyProtocol.equalsIgnoreCase("https"))
                ? HttpProtocol.HTTPS
                : HttpProtocol.HTTP;

        return new ProxySettings(
            proxyHost, proxyPort, protocol, proxyUser, proxyPassword, nonProxyHosts);
      }
    }
    return null;
  }

  // ── CSP conversion methods ─────────────────────────────────────────────────

  static ProxyConfiguration toS3ProxyConfiguration(ProxySettings s) {
    ProxyConfiguration.Builder proxyBuilder =
        ProxyConfiguration.builder()
            .scheme(s.getProtocol().getScheme())
            .host(s.getHost())
            .port(s.getPort())
            .useEnvironmentVariableValues(false)
            .useSystemPropertyValues(false);

    if (s.hasNonProxyHosts()) {
      proxyBuilder.nonProxyHosts(prepareNonProxyHostsForS3(s.getNonProxyHosts()));
    }

    if (s.hasCredentials()) {
      proxyBuilder.username(s.getUser());
      proxyBuilder.password(s.getPassword());
    }
    return proxyBuilder.build();
  }

  static ProxyOptions toAzureProxyOptions(ProxySettings s) {
    ProxyOptions proxyOptions =
        new ProxyOptions(ProxyOptions.Type.HTTP, new InetSocketAddress(s.getHost(), s.getPort()));
    proxyOptions.setCredentials(s.getUser(), s.getPassword());
    proxyOptions.setNonProxyHosts(s.getNonProxyHosts());
    return proxyOptions;
  }

  static HttpTransportFactory toGCSHttpTransportFactory(ProxySettings s) {
    HttpClientBuilder clientBuilder = HttpClientBuilder.create();

    clientBuilder.setProxy(new HttpHost(s.getHost(), s.getPort(), s.getProtocol().toString()));

    SdkProxyRoutePlanner routePlanner =
        new SdkProxyRoutePlanner(s.getHost(), s.getPort(), s.getProtocol(), s.getNonProxyHosts());
    clientBuilder.setRoutePlanner(routePlanner);

    if (s.hasCredentials()) {
      BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
      credentialsProvider.setCredentials(
          new AuthScope(s.getHost(), s.getPort()),
          new UsernamePasswordCredentials(s.getUser(), s.getPassword()));
      clientBuilder.setDefaultCredentialsProvider(credentialsProvider);
    }

    final ApacheHttpTransport transport = new ApacheHttpTransport(clientBuilder.build());
    return () -> transport;
  }

  // ── S3 convenience methods ─────────────────────────────────────────────────

  public static ProxyConfiguration createProxyConfigurationForS3(HttpClientSettingsKey key) {
    ProxySettings s = extractFromKey(key);
    if (s == null) {
      logger.debug("Omitting S3 proxy setup");
      return null;
    }
    logProxySettings("S3", s);
    return toS3ProxyConfiguration(s);
  }

  public static ProxyConfiguration createSessionlessProxyConfigurationForS3(
      Properties proxyProperties) throws SnowflakeSQLException {
    ProxySettings s = extractFromProperties(proxyProperties);
    if (s == null) {
      logger.debug("Omitting sessionless S3 proxy setup");
      return null;
    }
    logProxySettings("sessionless S3", s);
    return toS3ProxyConfiguration(s);
  }

  // ── Azure convenience methods ──────────────────────────────────────────────

  public static ProxyOptions createProxyOptionsForAzure(HttpClientSettingsKey key) {
    ProxySettings s = extractFromKey(key);
    if (s == null) {
      logger.debug("Omitting Azure proxy setup");
      return null;
    }
    logProxySettings("Azure", s);
    return toAzureProxyOptions(s);
  }

  public static ProxyOptions createSessionlessProxyOptionsForAzure(Properties proxyProperties)
      throws SnowflakeSQLException {
    ProxySettings s = extractFromProperties(proxyProperties);
    if (s == null) {
      logger.debug("Omitting sessionless Azure proxy setup");
      return null;
    }
    logProxySettings("sessionless Azure", s);
    return toAzureProxyOptions(s);
  }

  // ── GCS convenience methods ────────────────────────────────────────────────

  public static HttpTransportFactory createHttpTransportForGCS(HttpClientSettingsKey key) {
    ProxySettings s = extractFromKey(key);
    if (s == null) {
      logger.debug("Omitting GCS proxy setup");
      return null;
    }
    logProxySettings("GCS", s);
    return toGCSHttpTransportFactory(s);
  }

  public static HttpTransportFactory createSessionlessHttpTransportForGCS(
      Properties proxyProperties) throws SnowflakeSQLException {
    ProxySettings s = extractFromProperties(proxyProperties);
    if (s == null) {
      logger.debug("Omitting sessionless GCS proxy setup");
      return null;
    }
    logProxySettings("sessionless GCS", s);
    return toGCSHttpTransportFactory(s);
  }

  // ── Helpers ────────────────────────────────────────────────────────────────

  static Set<String> prepareNonProxyHostsForS3(String nonProxyHosts) {
    return Arrays.stream(nonProxyHosts.split("\\|"))
        .map(String::trim)
        .map(host -> SnowflakeUtil.globToSafePattern(host).pattern())
        .collect(Collectors.toSet());
  }

  private static void logProxySettings(String label, ProxySettings s) {
    String logMessage =
        "Setting "
            + label
            + " proxy. Host: "
            + s.getHost()
            + ", port: "
            + s.getPort()
            + ", protocol: "
            + s.getProtocol()
            + ", non-proxy hosts: "
            + s.getNonProxyHosts();
    if (s.hasCredentials()) {
      logMessage +=
          ", user: "
              + s.getUser()
              + ", password is "
              + SFLoggerUtil.isVariableProvided(s.getPassword());
    }
    logger.debug(logMessage);
  }
}
