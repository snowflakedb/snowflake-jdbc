package net.snowflake.client.core;

import static net.snowflake.client.jdbc.SnowflakeUtil.isNullOrEmpty;
import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;
import static org.apache.http.client.config.CookieSpecs.DEFAULT;
import static org.apache.http.client.config.CookieSpecs.IGNORE_COOKIES;

import com.amazonaws.ClientConfiguration;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.microsoft.azure.storage.OperationContext;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Socket;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import javax.net.ssl.TrustManager;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.RestRequest;
import net.snowflake.client.jdbc.RetryContextManager;
import net.snowflake.client.jdbc.SnowflakeDriver;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.SnowflakeUseDPoPNonceException;
import net.snowflake.client.jdbc.SnowflakeUtil;
import net.snowflake.client.jdbc.cloud.storage.S3HttpUtil;
import net.snowflake.client.log.ArgSupplier;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.client.log.SFLoggerUtil;
import net.snowflake.client.util.SecretDetector;
import net.snowflake.client.util.Stopwatch;
import net.snowflake.common.core.SqlState;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.HttpContext;
import org.apache.http.ssl.SSLInitializationException;
import org.apache.http.util.EntityUtils;

/** HttpUtil class */
public class HttpUtil {
  private static final SFLogger logger = SFLoggerFactory.getLogger(HttpUtil.class);

  static final String ERROR_FIELD_NAME = "error";
  static final String ERROR_USE_DPOP_NONCE = "use_dpop_nonce";
  static final String DPOP_NONCE_HEADER_NAME = "dpop-nonce";

  static final int DEFAULT_MAX_CONNECTIONS = 300;
  static final int DEFAULT_MAX_CONNECTIONS_PER_ROUTE = 300;
  private static final int DEFAULT_HTTP_CLIENT_CONNECTION_TIMEOUT_IN_MS = 60000;
  static final int DEFAULT_HTTP_CLIENT_SOCKET_TIMEOUT_IN_MS = 300000; // ms
  static final int DEFAULT_TTL = 60; // secs
  static final int DEFAULT_IDLE_CONNECTION_TIMEOUT = 5; // secs
  static final int DEFAULT_DOWNLOADED_CONDITION_TIMEOUT = 3600; // secs

  public static final String JDBC_TTL = "net.snowflake.jdbc.ttl";
  public static final String JDBC_MAX_CONNECTIONS_PROPERTY = "net.snowflake.jdbc.max_connections";
  public static final String JDBC_MAX_CONNECTIONS_PER_ROUTE_PROPERTY =
      "net.snowflake.jdbc.max_connections_per_route";

  private static Duration connectionTimeout;
  private static Duration socketTimeout;

  /**
   * The unique httpClient shared by all connections. This will benefit long-lived clients. Key =
   * proxy host + proxy port + nonProxyHosts, Value = Map of [OCSPMode, HttpClient]
   */
  public static Map<HttpClientSettingsKey, CloseableHttpClient> httpClient =
      new ConcurrentHashMap<>();

  /**
   * The unique httpClient map shared by all connections that don't want decompression. This will
   * benefit long-lived clients. Key = proxy host + proxy port + nonProxyHosts, Value = Map
   * [OCSPMode, HttpClient]
   */
  private static Map<HttpClientSettingsKey, CloseableHttpClient> httpClientWithoutDecompression =
      new ConcurrentHashMap<>();

  /** The map of snowflake route planners */
  static Map<HttpClientSettingsKey, SnowflakeMutableProxyRoutePlanner> httpClientRoutePlanner =
      new ConcurrentHashMap<>();

  /** Handle on the static connection manager, to gather statistics mainly */
  private static PoolingHttpClientConnectionManager connectionManager = null;

  /** default request configuration, to be copied on individual requests. */
  private static RequestConfig DefaultRequestConfig = null;

  private static boolean socksProxyDisabled = false;

  @SnowflakeJdbcInternalApi
  public static Duration getConnectionTimeout() {
    return connectionTimeout != null
        ? connectionTimeout
        : Duration.ofMillis(DEFAULT_HTTP_CLIENT_CONNECTION_TIMEOUT_IN_MS);
  }

  @SnowflakeJdbcInternalApi
  public static Duration getSocketTimeout() {
    return socketTimeout != null
        ? socketTimeout
        : Duration.ofMillis(DEFAULT_HTTP_CLIENT_SOCKET_TIMEOUT_IN_MS);
  }

  @SnowflakeJdbcInternalApi
  public static void setConnectionTimeout(int timeout) {
    connectionTimeout = Duration.ofMillis(timeout);
    initDefaultRequestConfig(connectionTimeout.toMillis(), getSocketTimeout().toMillis());
  }

  @SnowflakeJdbcInternalApi
  public static void setSocketTimeout(int timeout) {
    socketTimeout = Duration.ofMillis(timeout);
    initDefaultRequestConfig(getConnectionTimeout().toMillis(), socketTimeout.toMillis());
  }

  public static long getDownloadedConditionTimeoutInSeconds() {
    return DEFAULT_DOWNLOADED_CONDITION_TIMEOUT;
  }

  public static void closeExpiredAndIdleConnections() {
    if (connectionManager != null) {
      synchronized (connectionManager) {
        logger.debug("Connection pool stats: {}", connectionManager.getTotalStats());
        connectionManager.closeExpiredConnections();
        connectionManager.closeIdleConnections(DEFAULT_IDLE_CONNECTION_TIMEOUT, TimeUnit.SECONDS);
      }
    }
  }

  /**
   * A static function to set S3 proxy params when there is a valid session
   *
   * @param key key to HttpClient map containing OCSP and proxy info
   * @param clientConfig the configuration needed by S3 to set the proxy
   * @deprecated Use {@link S3HttpUtil#setProxyForS3(HttpClientSettingsKey, ClientConfiguration)}
   *     instead
   */
  @Deprecated
  public static void setProxyForS3(HttpClientSettingsKey key, ClientConfiguration clientConfig) {
    S3HttpUtil.setProxyForS3(key, clientConfig);
  }

  /**
   * A static function to set S3 proxy params for sessionless connections using the proxy params
   * from the StageInfo
   *
   * @param proxyProperties proxy properties
   * @param clientConfig the configuration needed by S3 to set the proxy
   * @throws SnowflakeSQLException when exception encountered
   * @deprecated Use {@link S3HttpUtil#setSessionlessProxyForS3(Properties, ClientConfiguration)}
   *     instead
   */
  @Deprecated
  public static void setSessionlessProxyForS3(
      Properties proxyProperties, ClientConfiguration clientConfig) throws SnowflakeSQLException {
    S3HttpUtil.setSessionlessProxyForS3(proxyProperties, clientConfig);
  }

  /**
   * A static function to set Azure proxy params for sessionless connections using the proxy params
   * from the StageInfo
   *
   * @param proxyProperties proxy properties
   * @param opContext the configuration needed by Azure to set the proxy
   * @throws SnowflakeSQLException when invalid proxy properties encountered
   */
  public static void setSessionlessProxyForAzure(
      Properties proxyProperties, OperationContext opContext) throws SnowflakeSQLException {
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
        Proxy azProxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyHost, proxyPort));
        logger.debug("Setting sessionless Azure proxy. Host: {}, port: {}", proxyHost, proxyPort);
        opContext.setProxy(azProxy);
      } else {
        logger.debug("Omitting sessionless Azure proxy setup as proxy is disabled");
      }
    } else {
      logger.debug("Omitting sessionless Azure proxy setup");
    }
  }

  /**
   * A static function to set Azure proxy params when there is a valid session
   *
   * @param key key to HttpClient map containing OCSP and proxy info
   * @param opContext the configuration needed by Azure to set the proxy
   */
  public static void setProxyForAzure(HttpClientSettingsKey key, OperationContext opContext) {
    if (key != null && key.usesProxy()) {
      Proxy azProxy =
          new Proxy(Proxy.Type.HTTP, new InetSocketAddress(key.getProxyHost(), key.getProxyPort()));
      logger.debug(
          "Setting Azure proxy. Host: {}, port: {}", key.getProxyHost(), key.getProxyPort());
      opContext.setProxy(azProxy);
    } else {
      logger.debug("Omitting Azure proxy setup");
    }
  }

  /**
   * Constructs a user-agent header with the following pattern: connector_name/connector_version
   * (os-platform_info) language_implementation/language_version
   *
   * @param customSuffix custom suffix that would be appended to user agent to identify the jdbc
   *     usage.
   * @return string for user-agent header
   */
  @VisibleForTesting
  static String buildUserAgent(String customSuffix) {
    // Start with connector name
    StringBuilder builder = new StringBuilder("JDBC/");
    // Append connector version and parenthesis start
    builder.append(SnowflakeDriver.implementVersion);
    builder.append(" (");
    // Generate OS platform and version from system properties
    String osPlatform = (systemGetProperty("os.name") != null) ? systemGetProperty("os.name") : "";
    String osVersion =
        (systemGetProperty("os.version") != null) ? systemGetProperty("os.version") : "";
    // Append OS platform and version separated by a space
    builder.append(osPlatform);
    builder.append(" ");
    builder.append(osVersion);
    // Append language name
    builder.append(") JAVA/");
    // Generate string for language version from system properties and append it
    String languageVersion =
        (systemGetProperty("java.version") != null) ? systemGetProperty("java.version") : "";
    builder.append(languageVersion);
    if (!customSuffix.isEmpty()) {
      builder.append(" " + customSuffix);
    }
    String userAgent = builder.toString();
    return userAgent;
  }

  /**
   * Build an Http client using our set of default.
   *
   * @param key Key to HttpClient hashmap containing OCSP mode and proxy information, could be null
   * @param ocspCacheFile OCSP response cache file. If null, the default OCSP response file will be
   *     used.
   * @param downloadUnCompressed Whether the HTTP client should be built requesting no decompression
   * @return HttpClient object
   */
  public static CloseableHttpClient buildHttpClient(
      @Nullable HttpClientSettingsKey key, File ocspCacheFile, boolean downloadUnCompressed) {
    logger.debug(
        "Building http client with client settings key: {}, ocsp cache file: {}, download uncompressed: {}",
        key != null ? key.toString() : null,
        ocspCacheFile,
        downloadUnCompressed);
    // set timeout so that we don't wait forever.
    // Setup the default configuration for all requests on this client
    int timeToLive = SystemUtil.convertSystemPropertyToIntValue(JDBC_TTL, DEFAULT_TTL);
    long connectTimeout = getConnectionTimeout().toMillis();
    long socketTimeout = getSocketTimeout().toMillis();
    logger.debug(
        "Connection pooling manager connect timeout: {} ms, socket timeout: {} ms, ttl: {} s",
        connectTimeout,
        socketTimeout,
        timeToLive);

    // Create default request config without proxy since different connections could use different
    // proxies in multi tenant environments
    // Proxy is set later with route planner
    if (DefaultRequestConfig == null) {
      initDefaultRequestConfig(connectTimeout, socketTimeout);
    }

    TrustManager[] trustManagers = null;
    if (key != null && key.getOcspMode() != OCSPMode.DISABLE_OCSP_CHECKS) {
      // A custom TrustManager is required only if disableOCSPChecks is disabled,
      // which is by default in the production. disableOCSPChecks can be enabled
      // 1) OCSP service is down for reasons, 2) PowerMock test that doesn't
      // care OCSP checks.
      // OCSP FailOpen is ON by default
      try {
        if (ocspCacheFile == null) {
          logger.debug("Instantiating trust manager with default ocsp cache file");
        } else {
          logger.debug("Instantiating trust manager with ocsp cache file: {}", ocspCacheFile);
        }
        TrustManager[] tm = {new SFTrustManager(key, ocspCacheFile)};
        trustManagers = tm;
      } catch (Exception | Error err) {
        // dump error stack
        StringWriter errors = new StringWriter();
        err.printStackTrace(new PrintWriter(errors));
        logger.error(errors.toString(), true);
        throw new RuntimeException(err); // rethrow the exception
      }
    } else if (key != null) {
      logger.debug(
          "Omitting trust manager instantiation as OCSP mode is set to {}", key.getOcspMode());
    } else {
      logger.debug("Omitting trust manager instantiation as configuration is not provided");
    }
    try {
      logger.debug(
          "Registering https connection socket factory with socks proxy disabled: {} and http "
              + "connection socket factory",
          socksProxyDisabled);

      Registry<ConnectionSocketFactory> registry =
          RegistryBuilder.<ConnectionSocketFactory>create()
              .register(
                  "https", new SFSSLConnectionSocketFactory(trustManagers, socksProxyDisabled))
              .register("http", new SFConnectionSocketFactory())
              .build();

      // Build a connection manager with enough connections
      connectionManager =
          new PoolingHttpClientConnectionManager(
              registry, null, null, null, timeToLive, TimeUnit.SECONDS);
      int maxConnections =
          SystemUtil.convertSystemPropertyToIntValue(
              JDBC_MAX_CONNECTIONS_PROPERTY, DEFAULT_MAX_CONNECTIONS);
      int maxConnectionsPerRoute =
          SystemUtil.convertSystemPropertyToIntValue(
              JDBC_MAX_CONNECTIONS_PER_ROUTE_PROPERTY, DEFAULT_MAX_CONNECTIONS_PER_ROUTE);
      logger.debug(
          "Max connections total in connection pooling manager: {}; max connections per route: {}",
          maxConnections,
          maxConnectionsPerRoute);
      connectionManager.setMaxTotal(maxConnections);
      connectionManager.setDefaultMaxPerRoute(maxConnectionsPerRoute);

      logger.debug("Disabling cookie management for http client");
      String userAgentSuffix = key != null ? key.getUserAgentSuffix() : "";
      HttpClientBuilder httpClientBuilder =
          HttpClientBuilder.create()
              .setConnectionManager(connectionManager)
              // Support JVM proxy settings
              .useSystemProperties()
              .setRedirectStrategy(new DefaultRedirectStrategy())
              .setUserAgent(buildUserAgent(userAgentSuffix)) // needed for Okta
              .disableCookieManagement() // SNOW-39748
              .setDefaultRequestConfig(DefaultRequestConfig);
      if (key != null && key.usesProxy()) {
        HttpHost proxy =
            new HttpHost(
                key.getProxyHost(), key.getProxyPort(), key.getProxyHttpProtocol().getScheme());
        logger.debug(
            "Configuring proxy and route planner - host: {}, port: {}, scheme: {}, nonProxyHosts: {}",
            key.getProxyHost(),
            key.getProxyPort(),
            key.getProxyHttpProtocol().getScheme(),
            key.getNonProxyHosts());
        // use the custom proxy properties
        SnowflakeMutableProxyRoutePlanner sdkProxyRoutePlanner =
            httpClientRoutePlanner.computeIfAbsent(
                key,
                k ->
                    new SnowflakeMutableProxyRoutePlanner(
                        key.getProxyHost(),
                        key.getProxyPort(),
                        key.getProxyHttpProtocol(),
                        key.getNonProxyHosts()));
        httpClientBuilder.setProxy(proxy).setRoutePlanner(sdkProxyRoutePlanner);
        if (!isNullOrEmpty(key.getProxyUser()) && !isNullOrEmpty(key.getProxyPassword())) {
          Credentials credentials =
              new UsernamePasswordCredentials(key.getProxyUser(), key.getProxyPassword());
          AuthScope authScope = new AuthScope(key.getProxyHost(), key.getProxyPort());
          CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
          logger.debug(
              "Using user: {}, password is {} for proxy host: {}, port: {}",
              key.getProxyUser(),
              SFLoggerUtil.isVariableProvided(key.getProxyPassword()),
              key.getProxyHost(),
              key.getProxyPort());
          credentialsProvider.setCredentials(authScope, credentials);
          httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
        }
      }
      if (downloadUnCompressed) {
        logger.debug("Disabling content compression for http client");
        httpClientBuilder.disableContentCompression();
      }
      return httpClientBuilder.build();
    } catch (NoSuchAlgorithmException | KeyManagementException ex) {
      throw new SSLInitializationException(ex.getMessage(), ex);
    }
  }

  private static void initDefaultRequestConfig(long connectTimeout, long socketTimeout) {
    RequestConfig.Builder builder =
        RequestConfig.custom()
            .setConnectTimeout((int) connectTimeout)
            .setConnectionRequestTimeout((int) connectTimeout)
            .setSocketTimeout((int) socketTimeout);
    logger.debug(
        "Rebuilding request config. Connect timeout: {} ms, connection request timeout: {} ms, socket timeout: {} ms",
        connectTimeout,
        connectTimeout,
        socketTimeout);
    DefaultRequestConfig = builder.build();
  }

  public static void updateRoutePlanner(HttpClientSettingsKey key) {
    if (httpClientRoutePlanner.containsKey(key)
        && !httpClientRoutePlanner
            .get(key)
            .getNonProxyHosts()
            .equalsIgnoreCase(key.getNonProxyHosts())) {
      logger.debug(
          "Updating route planner non-proxy hosts for proxy: {}:{} to: {}",
          key.getProxyHost(),
          key.getProxyPort(),
          key.getNonProxyHosts());
      httpClientRoutePlanner.get(key).setNonProxyHosts(key.getNonProxyHosts());
    }
  }

  /**
   * Gets HttpClient with insecureMode false
   *
   * @param ocspAndProxyKey OCSP mode and proxy settings for httpclient
   * @return HttpClient object shared across all connections
   */
  public static CloseableHttpClient getHttpClient(HttpClientSettingsKey ocspAndProxyKey) {
    return initHttpClient(ocspAndProxyKey, null);
  }

  /**
   * Gets HttpClient with insecureMode false and disabling decompression
   *
   * @param ocspAndProxyKey OCSP mode and proxy settings for httpclient
   * @return HttpClient object shared across all connections
   */
  public static CloseableHttpClient getHttpClientWithoutDecompression(
      HttpClientSettingsKey ocspAndProxyKey) {
    return initHttpClientWithoutDecompression(ocspAndProxyKey, null);
  }

  /**
   * Accessor for the HTTP client singleton.
   *
   * @param key contains information needed to build specific HttpClient
   * @param ocspCacheFile OCSP response cache file name. if null, the default file will be used.
   * @return HttpClient object shared across all connections
   */
  public static CloseableHttpClient initHttpClientWithoutDecompression(
      HttpClientSettingsKey key, File ocspCacheFile) {
    updateRoutePlanner(key);
    return httpClientWithoutDecompression.computeIfAbsent(
        key, k -> buildHttpClient(key, ocspCacheFile, true));
  }

  /**
   * Accessor for the HTTP client singleton.
   *
   * @param key contains information needed to build specific HttpClient
   * @param ocspCacheFile OCSP response cache file name. if null, the default file will be used.
   * @return HttpClient object shared across all connections
   */
  public static CloseableHttpClient initHttpClient(HttpClientSettingsKey key, File ocspCacheFile) {
    updateRoutePlanner(key);
    return httpClient.computeIfAbsent(
        key, k -> buildHttpClient(key, ocspCacheFile, key.getGzipDisabled()));
  }

  /**
   * Return a request configuration inheriting from the default request configuration of the shared
   * HttpClient with a different socket timeout.
   *
   * @param soTimeoutMs - custom socket timeout in milli-seconds
   * @param withoutCookies - whether this request should ignore cookies or not
   * @return RequestConfig object
   */
  public static RequestConfig getDefaultRequestConfigWithSocketTimeout(
      int soTimeoutMs, boolean withoutCookies) {
    final String cookieSpec = withoutCookies ? IGNORE_COOKIES : DEFAULT;
    return RequestConfig.copy(DefaultRequestConfig)
        .setSocketTimeout(soTimeoutMs)
        .setCookieSpec(cookieSpec)
        .build();
  }

  /**
   * Return a request configuration inheriting from the default request configuration of the shared
   * HttpClient with a different socket and connect timeout.
   *
   * @param requestSocketAndConnectTimeout - custom socket and connect timeout in milli-seconds
   * @param withoutCookies - whether this request should ignore cookies or not
   * @return RequestConfig object
   */
  public static RequestConfig getDefaultRequestConfigWithSocketAndConnectTimeout(
      int requestSocketAndConnectTimeout, boolean withoutCookies) {
    final String cookieSpec = withoutCookies ? IGNORE_COOKIES : DEFAULT;
    return RequestConfig.copy(DefaultRequestConfig)
        .setSocketTimeout(requestSocketAndConnectTimeout)
        .setConnectTimeout(requestSocketAndConnectTimeout)
        .setCookieSpec(cookieSpec)
        .build();
  }

  /**
   * Return a request configuration inheriting from the default request configuration of the shared
   * HttpClient with the cookie spec set to ignore.
   *
   * @return RequestConfig object
   */
  public static RequestConfig getRequestConfigWithoutCookies() {
    return RequestConfig.copy(DefaultRequestConfig).setCookieSpec(IGNORE_COOKIES).build();
  }

  public static void setRequestConfig(RequestConfig requestConfig) {
    logger.debug("Setting default request config to: {}", requestConfig);
    DefaultRequestConfig = requestConfig;
  }

  /**
   * Accessor for the HTTP client singleton.
   *
   * @return HTTP Client stats in string representation
   */
  private static String getHttpClientStats() {
    return connectionManager == null ? "" : connectionManager.getTotalStats().toString();
  }

  /**
   * Enables/disables use of the SOCKS proxy when creating sockets
   *
   * @param socksProxyDisabled new value
   */
  public static void setSocksProxyDisabled(boolean socksProxyDisabled) {
    logger.debug("Setting socks proxy disabled to {}", socksProxyDisabled);
    HttpUtil.socksProxyDisabled = socksProxyDisabled;
  }

  /**
   * Returns whether the SOCKS proxy is disabled for this JVM
   *
   * @return whether the SOCKS proxy is disabled
   */
  public static boolean isSocksProxyDisabled() {
    return HttpUtil.socksProxyDisabled;
  }

  /**
   * Executes an HTTP request with the cookie spec set to IGNORE_COOKIES
   *
   * @param httpRequest HttpRequestBase
   * @param retryTimeout retry timeout
   * @param authTimeout authenticator specific timeout
   * @param socketTimeout socket timeout (in ms)
   * @param retryCount max retry count for the request - if it is set to 0, it will be ignored and
   *     only retryTimeout will determine when to end the retries
   * @param injectSocketTimeout injecting socket timeout
   * @param canceling canceling?
   * @param ocspAndProxyKey OCSP mode and proxy settings for httpclient
   * @return response
   * @throws SnowflakeSQLException if Snowflake error occurs
   * @throws IOException raises if a general IO error occurs
   */
  static String executeRequestWithoutCookies(
      HttpRequestBase httpRequest,
      int retryTimeout,
      int authTimeout,
      int socketTimeout,
      int retryCount,
      int injectSocketTimeout,
      AtomicBoolean canceling,
      HttpClientSettingsKey ocspAndProxyKey)
      throws SnowflakeSQLException, IOException {
    logger.debug("Executing request without cookies");
    return executeRequestInternal(
        httpRequest,
        retryTimeout,
        authTimeout,
        socketTimeout,
        retryCount,
        injectSocketTimeout,
        canceling,
        true, // no cookie
        false, // no retry parameter
        true, // guid? (do we need this?)
        false, // no retry on HTTP 403
        getHttpClient(ocspAndProxyKey),
        new ExecTimeTelemetryData(),
        null);
  }

  /**
   * Executes an HTTP request for Snowflake.
   *
   * @param httpRequest HttpRequestBase
   * @param retryTimeout retry timeout
   * @param authTimeout authenticator specific timeout
   * @param socketTimeout socket timeout (in ms)
   * @param retryCount max retry count for the request - if it is set to 0, it will be ignored and
   *     only retryTimeout will determine when to end the retries
   * @param ocspAndProxyAndGzipKey OCSP mode and proxy settings for httpclient
   * @return response
   * @throws SnowflakeSQLException if Snowflake error occurs
   * @throws IOException raises if a general IO error occurs
   */
  public static String executeGeneralRequest(
      HttpRequestBase httpRequest,
      int retryTimeout,
      int authTimeout,
      int socketTimeout,
      int retryCount,
      HttpClientSettingsKey ocspAndProxyAndGzipKey)
      throws SnowflakeSQLException, IOException {
    return executeGeneralRequest(
        httpRequest,
        retryTimeout,
        authTimeout,
        socketTimeout,
        retryCount,
        ocspAndProxyAndGzipKey,
        null);
  }

  @SnowflakeJdbcInternalApi
  public static String executeGeneralRequestOmitRequestGuid(
      HttpRequestBase httpRequest,
      int retryTimeout,
      int authTimeout,
      int socketTimeout,
      int retryCount,
      HttpClientSettingsKey ocspAndProxyAndGzipKey)
      throws SnowflakeSQLException, IOException {
    return executeRequestInternal(
        httpRequest,
        retryTimeout,
        authTimeout,
        socketTimeout,
        retryCount,
        0,
        null,
        false,
        false,
        false,
        false,
        getHttpClient(ocspAndProxyAndGzipKey),
        new ExecTimeTelemetryData(),
        null);
  }

  /**
   * Executes an HTTP request for Snowflake.
   *
   * @param httpRequest HttpRequestBase
   * @param retryTimeout retry timeout
   * @param authTimeout authenticator specific timeout
   * @param socketTimeout socket timeout (in ms)
   * @param retryCount max retry count for the request - if it is set to 0, it will be ignored and
   *     only retryTimeout will determine when to end the retries
   * @param ocspAndProxyAndGzipKey OCSP mode and proxy settings for httpclient
   * @param retryContextManager RetryContext used to customize retry handling functionality
   * @return response
   * @throws SnowflakeSQLException if Snowflake error occurs
   * @throws IOException raises if a general IO error occurs
   */
  @SnowflakeJdbcInternalApi
  public static String executeGeneralRequest(
      HttpRequestBase httpRequest,
      int retryTimeout,
      int authTimeout,
      int socketTimeout,
      int retryCount,
      HttpClientSettingsKey ocspAndProxyAndGzipKey,
      RetryContextManager retryContextManager)
      throws SnowflakeSQLException, IOException {
    logger.debug("Executing general request");
    return executeRequest(
        httpRequest,
        retryTimeout,
        authTimeout,
        socketTimeout,
        retryCount,
        0, // no inject socket timeout
        null, // no canceling
        false, // no retry parameter
        false, // no retry on HTTP 403
        ocspAndProxyAndGzipKey,
        new ExecTimeTelemetryData(),
        retryContextManager);
  }

  /**
   * Executes an HTTP request for Snowflake
   *
   * @param httpRequest HttpRequestBase
   * @param retryTimeout retry timeout
   * @param authTimeout authenticator specific timeout
   * @param socketTimeout socket timeout (in ms)
   * @param retryCount max retry count for the request - if it is set to 0, it will be ignored and
   *     only retryTimeout will determine when to end the retries
   * @param httpClient client object used to communicate with other machine
   * @return response
   * @throws SnowflakeSQLException if Snowflake error occurs
   * @throws IOException raises if a general IO error occurs
   */
  public static String executeGeneralRequest(
      HttpRequestBase httpRequest,
      int retryTimeout,
      int authTimeout,
      int socketTimeout,
      int retryCount,
      CloseableHttpClient httpClient)
      throws SnowflakeSQLException, IOException {
    logger.debug("Executing general request");
    return executeRequestInternal(
        httpRequest,
        retryTimeout,
        authTimeout,
        socketTimeout,
        retryCount,
        0, // no inject socket timeout
        null, // no canceling
        false, // with cookie
        false, // no retry parameter
        true, // include request GUID
        false, // no retry on HTTP 403
        httpClient,
        new ExecTimeTelemetryData(),
        null);
  }

  /**
   * Executes an HTTP request for Snowflake.
   *
   * @param httpRequest HttpRequestBase
   * @param retryTimeout retry timeout
   * @param authTimeout authenticator timeout
   * @param socketTimeout socket timeout (in ms)
   * @param maxRetries retry count for the request
   * @param injectSocketTimeout injecting socket timeout
   * @param canceling canceling?
   * @param includeRetryParameters whether to include retry parameters in retried requests
   * @param retryOnHTTP403 whether to retry on HTTP 403 or not
   * @param ocspAndProxyKey OCSP mode and proxy settings for httpclient
   * @param execTimeData query execution time telemetry data object
   * @return response
   * @throws SnowflakeSQLException if Snowflake error occurs
   * @throws IOException raises if a general IO error occurs
   */
  public static String executeRequest(
      HttpRequestBase httpRequest,
      int retryTimeout,
      int authTimeout,
      int socketTimeout,
      int maxRetries,
      int injectSocketTimeout,
      AtomicBoolean canceling,
      boolean includeRetryParameters,
      boolean retryOnHTTP403,
      HttpClientSettingsKey ocspAndProxyKey,
      ExecTimeTelemetryData execTimeData)
      throws SnowflakeSQLException, IOException {
    return executeRequest(
        httpRequest,
        retryTimeout,
        authTimeout,
        socketTimeout,
        maxRetries,
        injectSocketTimeout,
        canceling,
        includeRetryParameters,
        retryOnHTTP403,
        ocspAndProxyKey,
        execTimeData,
        null);
  }

  /**
   * Executes an HTTP request for Snowflake.
   *
   * @param httpRequest HttpRequestBase
   * @param retryTimeout retry timeout
   * @param authTimeout authenticator timeout
   * @param socketTimeout socket timeout (in ms)
   * @param maxRetries retry count for the request
   * @param injectSocketTimeout injecting socket timeout
   * @param canceling canceling?
   * @param includeRetryParameters whether to include retry parameters in retried requests
   * @param retryOnHTTP403 whether to retry on HTTP 403 or not
   * @param ocspAndProxyKey OCSP mode and proxy settings for httpclient
   * @param execTimeData query execution time telemetry data object
   * @param retryContextManager RetryContext used to customize retry handling functionality
   * @return response
   * @throws SnowflakeSQLException if Snowflake error occurs
   * @throws IOException raises if a general IO error occurs
   */
  public static String executeRequest(
      HttpRequestBase httpRequest,
      int retryTimeout,
      int authTimeout,
      int socketTimeout,
      int maxRetries,
      int injectSocketTimeout,
      AtomicBoolean canceling,
      boolean includeRetryParameters,
      boolean retryOnHTTP403,
      HttpClientSettingsKey ocspAndProxyKey,
      ExecTimeTelemetryData execTimeData,
      RetryContextManager retryContextManager)
      throws SnowflakeSQLException, IOException {
    boolean ocspEnabled = !(ocspAndProxyKey.getOcspMode().equals(OCSPMode.DISABLE_OCSP_CHECKS));
    logger.debug("Executing request with OCSP enabled: {}", ocspEnabled);
    execTimeData.setOCSPStatus(ocspEnabled);
    return executeRequestInternal(
        httpRequest,
        retryTimeout,
        authTimeout,
        socketTimeout,
        maxRetries,
        injectSocketTimeout,
        canceling,
        false, // with cookie (do we need cookie?)
        includeRetryParameters,
        true, // include request GUID
        retryOnHTTP403,
        getHttpClient(ocspAndProxyKey),
        execTimeData,
        retryContextManager);
  }

  /**
   * Helper to execute a request with retry and check and throw exception if response is not
   * success. This should be used only for small request has it execute the REST request and get
   * back the result as a string.
   *
   * <p>Connection under the httpRequest is released.
   *
   * @param httpRequest request object contains all the information
   * @param retryTimeout retry timeout (in seconds)
   * @param authTimeout authenticator specific timeout (in seconds)
   * @param socketTimeout socket timeout (in ms)
   * @param maxRetries retry count for the request
   * @param injectSocketTimeout simulate socket timeout
   * @param canceling canceling flag
   * @param withoutCookies whether this request should ignore cookies
   * @param includeRetryParameters whether to include retry parameters in retried requests
   * @param includeRequestGuid whether to include request_guid
   * @param retryOnHTTP403 whether to retry on HTTP 403
   * @param httpClient client object used to communicate with other machine
   * @param retryContextManager RetryContext used to customize retry handling functionality
   * @return response in String
   * @throws SnowflakeSQLException if Snowflake error occurs
   * @throws IOException raises if a general IO error occurs
   */
  private static String executeRequestInternal(
      HttpRequestBase httpRequest,
      int retryTimeout,
      int authTimeout,
      int socketTimeout,
      int maxRetries,
      int injectSocketTimeout,
      AtomicBoolean canceling,
      boolean withoutCookies,
      boolean includeRetryParameters,
      boolean includeRequestGuid,
      boolean retryOnHTTP403,
      CloseableHttpClient httpClient,
      ExecTimeTelemetryData execTimeData,
      RetryContextManager retryContextManager)
      throws SnowflakeSQLException, IOException {
    // HttpRequest.toString() contains request URI. Scrub any credentials, if
    // present, before logging
    String requestInfoScrubbed = SecretDetector.maskSASToken(httpRequest.toString());

    logger.debug(
        "Pool: {} Executing: {}", (ArgSupplier) HttpUtil::getHttpClientStats, requestInfoScrubbed);

    String theString;
    StringWriter writer = null;
    CloseableHttpResponse response = null;
    Stopwatch stopwatch = null;

    if (logger.isDebugEnabled()) {
      stopwatch = new Stopwatch();
      stopwatch.start();
    }

    try {
      response =
          RestRequest.execute(
              httpClient,
              httpRequest,
              retryTimeout,
              authTimeout,
              socketTimeout,
              maxRetries,
              injectSocketTimeout,
              canceling,
              withoutCookies,
              includeRetryParameters,
              includeRequestGuid,
              retryOnHTTP403,
              execTimeData,
              retryContextManager);
      if (logger.isDebugEnabled() && stopwatch != null) {
        stopwatch.stop();
      }

      if (response == null || response.getStatusLine().getStatusCode() != 200) {
        logger.error("Error executing request: {}", requestInfoScrubbed);

        if (response != null
            && response.getStatusLine().getStatusCode() == 400
            && response.getEntity() != null) {
          checkForDPoPNonceError(response);
        }

        SnowflakeUtil.logResponseDetails(response, logger);

        if (response != null) {
          EntityUtils.consume(response.getEntity());
        }

        // We throw here exception if timeout was reached for login
        throw new SnowflakeSQLException(
            SqlState.IO_ERROR,
            ErrorCode.NETWORK_ERROR.getMessageCode(),
            "HTTP status="
                + ((response != null)
                    ? response.getStatusLine().getStatusCode()
                    : "null response"));
      }

      execTimeData.setResponseIOStreamStart();
      writer = new StringWriter();
      try (InputStream ins = response.getEntity().getContent()) {
        IOUtils.copy(ins, writer, "UTF-8");
      }
      theString = writer.toString();
      execTimeData.setResponseIOStreamEnd();
    } finally {
      IOUtils.closeQuietly(writer);
      IOUtils.closeQuietly(response);
    }

    logger.debug(
        "Pool: {} Request returned for: {} took {} ms",
        (ArgSupplier) HttpUtil::getHttpClientStats,
        requestInfoScrubbed,
        stopwatch == null ? "n/a" : stopwatch.elapsedMillis());

    return theString;
  }

  private static void checkForDPoPNonceError(HttpResponse response) throws IOException {
    String errorResponse = EntityUtils.toString(response.getEntity());
    if (!isNullOrEmpty(errorResponse)) {
      ObjectMapper objectMapper = ObjectMapperFactory.getObjectMapper();
      JsonNode rootNode = objectMapper.readTree(errorResponse);
      JsonNode errorNode = rootNode.get(ERROR_FIELD_NAME);
      if (errorNode != null
          && errorNode.isValueNode()
          && errorNode.isTextual()
          && errorNode.textValue().equals(ERROR_USE_DPOP_NONCE)) {
        throw new SnowflakeUseDPoPNonceException(
            response.getFirstHeader(DPOP_NONCE_HEADER_NAME).getValue());
      }
    }
  }

  // This is a workaround for JDK-7036144.
  //
  // The GZIPInputStream prematurely closes its input if a) it finds
  // a whole GZIP block and b) input.available() returns 0. In order
  // to work around this issue, we inject a thin wrapper for the
  // InputStream whose available() method always returns at least 1.
  //
  // Further details on this bug:
  //   http://bugs.java.com/bugdatabase/view_bug.do?bug_id=7036144
  public static final class HttpInputStream extends InputStream {
    private final InputStream httpIn;

    public HttpInputStream(InputStream httpIn) {
      this.httpIn = httpIn;
    }

    // This is the only modified function, all other
    // methods are simple wrapper around the HTTP stream.
    @Override
    public final int available() throws IOException {
      int available = httpIn.available();
      return available == 0 ? 1 : available;
    }

    // ONLY WRAPPER METHODS FROM HERE ON.
    @Override
    public final int read() throws IOException {
      return httpIn.read();
    }

    @Override
    public final int read(byte b[]) throws IOException {
      return httpIn.read(b);
    }

    @Override
    public final int read(byte b[], int off, int len) throws IOException {
      return httpIn.read(b, off, len);
    }

    @Override
    public final long skip(long n) throws IOException {
      return httpIn.skip(n);
    }

    @Override
    public final void close() throws IOException {
      httpIn.close();
    }

    @Override
    public synchronized void mark(int readlimit) {
      httpIn.mark(readlimit);
    }

    @Override
    public synchronized void reset() throws IOException {
      httpIn.reset();
    }

    @Override
    public final boolean markSupported() {
      return httpIn.markSupported();
    }
  }

  static final class SFConnectionSocketFactory extends PlainConnectionSocketFactory {
    @Override
    public Socket createSocket(HttpContext ctx) throws IOException {
      if (socksProxyDisabled) {
        logger.trace("Creating socket with no proxy");
        return new Socket(Proxy.NO_PROXY);
      }
      logger.trace("Creating socket with proxy");
      return super.createSocket(ctx);
    }
  }

  /**
   * Helper function to attach additional headers to a request if present. This takes a (nullable)
   * map of headers in <name,value> format and adds them to the incoming request using addHeader.
   *
   * <p>Snowsight uses this to attach headers with additional telemetry information, see
   * https://snowflakecomputing.atlassian.net/wiki/spaces/EN/pages/2960557006/GS+Communication
   *
   * @param request The request to add headers to. Must not be null.
   * @param additionalHeaders The headers to add. May be null.
   */
  static void applyAdditionalHeadersForSnowsight(
      HttpRequestBase request, Map<String, String> additionalHeaders) {
    if (additionalHeaders != null && !additionalHeaders.isEmpty()) {
      additionalHeaders.forEach(request::addHeader);
    }
  }
}
