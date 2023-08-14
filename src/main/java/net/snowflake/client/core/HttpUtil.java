/*
 * Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;
import static org.apache.http.client.config.CookieSpecs.DEFAULT;
import static org.apache.http.client.config.CookieSpecs.IGNORE_COOKIES;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.microsoft.azure.storage.OperationContext;
import com.snowflake.client.jdbc.SnowflakeDriver;
import java.io.*;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Socket;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import javax.net.ssl.TrustManager;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.RestRequest;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.SnowflakeUtil;
import net.snowflake.client.log.ArgSupplier;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.client.util.SecretDetector;
import net.snowflake.common.core.SqlState;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHost;
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

public class HttpUtil {
  static final SFLogger logger = SFLoggerFactory.getLogger(HttpUtil.class);

  static final int DEFAULT_MAX_CONNECTIONS = 300;
  static final int DEFAULT_MAX_CONNECTIONS_PER_ROUTE = 300;
  static final int DEFAULT_CONNECTION_TIMEOUT = 60000;
  static final int DEFAULT_HTTP_CLIENT_SOCKET_TIMEOUT = 300000; // ms
  static final int DEFAULT_TTL = 60; // secs
  static final int DEFAULT_IDLE_CONNECTION_TIMEOUT = 5; // secs
  static final int DEFAULT_DOWNLOADED_CONDITION_TIMEOUT = 3600; // secs

  public static final String JDBC_TTL = "net.snowflake.jdbc.ttl";
  public static final String JDBC_MAX_CONNECTIONS_PROPERTY = "net.snowflake.jdbc.max_connections";
  public static final String JDBC_MAX_CONNECTIONS_PER_ROUTE_PROPERTY =
      "net.snowflake.jdbc.max_connections_per_route";

  /**
   * The unique httpClient shared by all connections. This will benefit long- lived clients. Key =
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

  public static long getDownloadedConditionTimeoutInSeconds() {
    return DEFAULT_DOWNLOADED_CONDITION_TIMEOUT;
  }

  public static void closeExpiredAndIdleConnections() {
    if (connectionManager != null) {
      synchronized (connectionManager) {
        logger.debug("connection pool stats: {}", connectionManager.getTotalStats());
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
   */
  public static void setProxyForS3(HttpClientSettingsKey key, ClientConfiguration clientConfig) {
    if (key != null && key.usesProxy()) {
      clientConfig.setProxyProtocol(key.getProxyProtocol());
      clientConfig.setProxyHost(key.getProxyHost());
      clientConfig.setProxyPort(key.getProxyPort());
      clientConfig.setNonProxyHosts(key.getNonProxyHosts());
      if (!Strings.isNullOrEmpty(key.getProxyUser())
          && !Strings.isNullOrEmpty(key.getProxyPassword())) {
        clientConfig.setProxyUsername(key.getProxyUser());
        clientConfig.setProxyPassword(key.getProxyPassword());
      }
    }
  }

  /**
   * A static function to set S3 proxy params for sessionless connections using the proxy params
   * from the StageInfo
   *
   * @param proxyProperties proxy properties
   * @param clientConfig the configuration needed by S3 to set the proxy
   * @throws SnowflakeSQLException
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
            (!Strings.isNullOrEmpty(proxyProtocol) && proxyProtocol.equalsIgnoreCase("https"))
                ? Protocol.HTTPS
                : Protocol.HTTP;
        clientConfig.setProxyHost(proxyHost);
        clientConfig.setProxyPort(proxyPort);
        clientConfig.setNonProxyHosts(nonProxyHosts);
        clientConfig.setProxyProtocol(protocolEnum);
        if (!Strings.isNullOrEmpty(proxyUser) && !Strings.isNullOrEmpty(proxyPassword)) {
          clientConfig.setProxyUsername(proxyUser);
          clientConfig.setProxyPassword(proxyPassword);
        }
      }
    }
  }

  /**
   * A static function to set Azure proxy params for sessionless connections using the proxy params
   * from the StageInfo
   *
   * @param proxyProperties proxy properties
   * @param opContext the configuration needed by Azure to set the proxy
   * @throws SnowflakeSQLException
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
        opContext.setProxy(azProxy);
      }
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
      opContext.setProxy(azProxy);
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
    // set timeout so that we don't wait forever.
    // Setup the default configuration for all requests on this client

    int timeToLive = convertSystemPropertyToIntValue(JDBC_TTL, DEFAULT_TTL);
    logger.debug("time to live in connection pooling manager: {}", timeToLive);

    // Set proxy settings for DefaultRequestConfig. If current proxy settings are the same as for
    // the last request, keep the current DefaultRequestConfig. If not, build a new
    // DefaultRequestConfig and set the new proxy settings for it
    HttpHost proxy =
        (key != null && key.usesProxy())
            ? new HttpHost(
                key.getProxyHost(), key.getProxyPort(), key.getProxyProtocol().toString())
            : null;
    // If defaultrequestconfig is not initialized or its proxy settings do not match current proxy
    // settings, re-build it (current or old proxy settings could be null, so null check is
    // included)
    boolean noDefaultRequestConfig =
        DefaultRequestConfig == null || DefaultRequestConfig.getProxy() == null;
    if (noDefaultRequestConfig || !DefaultRequestConfig.getProxy().equals(proxy)) {
      RequestConfig.Builder builder =
          RequestConfig.custom()
              .setConnectTimeout(DEFAULT_CONNECTION_TIMEOUT)
              .setConnectionRequestTimeout(DEFAULT_CONNECTION_TIMEOUT)
              .setSocketTimeout(DEFAULT_HTTP_CLIENT_SOCKET_TIMEOUT);
      // only set the proxy settings if they are not null
      // but no value has been specified for nonProxyHosts
      // the route planner will determine whether to use a proxy based on nonProxyHosts value.
      if (proxy != null && Strings.isNullOrEmpty(key.getNonProxyHosts())) {
        builder.setProxy(proxy);
      }
      DefaultRequestConfig = builder.build();
    }

    TrustManager[] trustManagers = null;
    if (key != null && key.getOcspMode() != OCSPMode.INSECURE) {
      // A custom TrustManager is required only if insecureMode is disabled,
      // which is by default in the production. insecureMode can be enabled
      // 1) OCSP service is down for reasons, 2) PowerMock test that doesn't
      // care OCSP checks.
      // OCSP FailOpen is ON by default
      try {
        TrustManager[] tm = {new SFTrustManager(key, ocspCacheFile)};
        trustManagers = tm;
      } catch (Exception | Error err) {
        // dump error stack
        StringWriter errors = new StringWriter();
        err.printStackTrace(new PrintWriter(errors));
        logger.error(errors.toString(), true);
        throw new RuntimeException(err); // rethrow the exception
      }
    }
    try {
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
          convertSystemPropertyToIntValue(JDBC_MAX_CONNECTIONS_PROPERTY, DEFAULT_MAX_CONNECTIONS);
      int maxConnectionsPerRoute =
          convertSystemPropertyToIntValue(
              JDBC_MAX_CONNECTIONS_PER_ROUTE_PROPERTY, DEFAULT_MAX_CONNECTIONS_PER_ROUTE);
      logger.debug(
          "Max connections total in connection pooling manager: {}; max connections per route: {}",
          maxConnections,
          maxConnectionsPerRoute);
      connectionManager.setMaxTotal(maxConnections);
      connectionManager.setDefaultMaxPerRoute(maxConnectionsPerRoute);

      String userAgentSuffix = key != null ? key.getUserAgentSuffix() : "";
      HttpClientBuilder httpClientBuilder =
          HttpClientBuilder.create()
              .setConnectionManager(connectionManager)
              // Support JVM proxy settings
              .useSystemProperties()
              .setRedirectStrategy(new DefaultRedirectStrategy())
              .setUserAgent(buildUserAgent(userAgentSuffix)) // needed for Okta
              .disableCookieManagement(); // SNOW-39748

      if (key != null && key.usesProxy()) {
        // use the custom proxy properties
        SnowflakeMutableProxyRoutePlanner sdkProxyRoutePlanner =
            httpClientRoutePlanner.computeIfAbsent(
                key,
                k ->
                    new SnowflakeMutableProxyRoutePlanner(
                        key.getProxyHost(),
                        key.getProxyPort(),
                        key.getProxyProtocol(),
                        key.getNonProxyHosts()));
        httpClientBuilder = httpClientBuilder.setProxy(proxy).setRoutePlanner(sdkProxyRoutePlanner);
        if (!Strings.isNullOrEmpty(key.getProxyUser())
            && !Strings.isNullOrEmpty(key.getProxyPassword())) {
          Credentials credentials =
              new UsernamePasswordCredentials(key.getProxyUser(), key.getProxyPassword());
          AuthScope authScope = new AuthScope(key.getProxyHost(), key.getProxyPort());
          CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
          credentialsProvider.setCredentials(authScope, credentials);
          httpClientBuilder = httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
        }
      }
      httpClientBuilder.setDefaultRequestConfig(DefaultRequestConfig);
      if (downloadUnCompressed) {
        httpClientBuilder = httpClientBuilder.disableContentCompression();
      }
      return httpClientBuilder.build();
    } catch (NoSuchAlgorithmException | KeyManagementException ex) {
      throw new SSLInitializationException(ex.getMessage(), ex);
    }
  }

  public static void updateRoutePlanner(HttpClientSettingsKey key) {
    if (httpClientRoutePlanner.containsKey(key)
        && !httpClientRoutePlanner
            .get(key)
            .getNonProxyHosts()
            .equalsIgnoreCase(key.getNonProxyHosts())) {
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
   * Executes a HTTP request with the cookie spec set to IGNORE_COOKIES
   *
   * @param httpRequest HttpRequestBase
   * @param retryTimeout retry timeout
   * @param authTimeout authenticator specific timeout
   * @param socketTimeout socket timeout (in ms)
   * @param retryCount retry count for the request
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
        new ExecTimeTelemetryData());
  }

  /**
   * Executes a HTTP request for Snowflake.
   *
   * @param httpRequest HttpRequestBase
   * @param retryTimeout retry timeout
   * @param authTimeout authenticator specific timeout
   * @param socketTimeout socket timeout (in ms)
   * @param retryCount retry count for the request
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
        new ExecTimeTelemetryData());
  }

  /**
   * Executes a HTTP request for Snowflake
   *
   * @param httpRequest HttpRequestBase
   * @param retryTimeout retry timeout
   * @param authTimeout authenticator specific timeout
   * @param socketTimeout socket timeout (in ms)
   * @param retryCount retry count for the request
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
        new ExecTimeTelemetryData());
  }

  /**
   * Executes a HTTP request for Snowflake.
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
    boolean ocspEnabled = !(ocspAndProxyKey.getOcspMode().equals(OCSPMode.INSECURE));
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
        execTimeData);
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
      ExecTimeTelemetryData execTimeData)
      throws SnowflakeSQLException, IOException {
    // HttpRequest.toString() contains request URI. Scrub any credentials, if
    // present, before logging
    String requestInfoScrubbed = SecretDetector.maskSASToken(httpRequest.toString());

    logger.debug(
        "Pool: {} Executing: {}", (ArgSupplier) HttpUtil::getHttpClientStats, requestInfoScrubbed);

    String theString;
    StringWriter writer = null;
    CloseableHttpResponse response = null;

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
              execTimeData);

      if (response == null || response.getStatusLine().getStatusCode() != 200) {
        logger.error("Error executing request: {}", requestInfoScrubbed);

        SnowflakeUtil.logResponseDetails(response, logger);

        if (response != null) {
          EntityUtils.consume(response.getEntity());
        }

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
        "Pool: {} Request returned for: {}",
        (ArgSupplier) HttpUtil::getHttpClientStats,
        requestInfoScrubbed);

    return theString;
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
        return new Socket(Proxy.NO_PROXY);
      }
      return super.createSocket(ctx);
    }
  }

  /**
   * Helper function to convert system properties to integers
   *
   * @param systemProperty name of the system property
   * @param defaultValue default value used
   * @return the value of the system property, else the default value
   */
  static int convertSystemPropertyToIntValue(String systemProperty, int defaultValue) {
    String systemPropertyValue = systemGetProperty(systemProperty);
    int returnVal = defaultValue;
    if (systemPropertyValue != null) {
      try {
        returnVal = Integer.parseInt(systemPropertyValue);
      } catch (NumberFormatException ex) {
        logger.info(
            "Failed to parse the system parameter {} with value {}",
            systemProperty,
            systemPropertyValue);
      }
    }
    return returnVal;
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
