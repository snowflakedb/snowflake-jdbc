/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetEnv;
import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;
import static org.apache.http.client.config.CookieSpecs.DEFAULT;
import static org.apache.http.client.config.CookieSpecs.IGNORE_COOKIES;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.http.apache.SdkProxyRoutePlanner;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
  static final int DEFAULT_TTL = -1; // secs
  static final int DEFAULT_IDLE_CONNECTION_TIMEOUT = 5; // secs
  static final int DEFAULT_DOWNLOADED_CONDITION_TIMEOUT = 3600; // secs

  public static final String JDBC_TTL = "net.snowflake.jdbc.ttl";

  /** The unique httpClient shared by all connections. This will benefit long- lived clients */
  private static Map<OCSPMode, CloseableHttpClient> httpClient = new ConcurrentHashMap<>();

  /**
   * The unique httpClient shared by all connections that don't want decompression. This will
   * benefit long-lived clients
   */
  private static Map<OCSPMode, CloseableHttpClient> httpClientWithoutDecompression =
      new ConcurrentHashMap<>();

  /** Handle on the static connection manager, to gather statistics mainly */
  private static PoolingHttpClientConnectionManager connectionManager = null;

  /** default request configuration, to be copied on individual requests. */
  private static RequestConfig DefaultRequestConfig = null;

  private static boolean socksProxyDisabled = false;

  /** customized proxy properties */
  static boolean useProxy = false;

  static boolean httpUseProxy = false;

  static String proxyHost;
  static int proxyPort;
  static String proxyUser;
  static String proxyPassword;
  static String nonProxyHosts;

  public static long getDownloadedConditionTimeoutInSeconds() {
    return DEFAULT_DOWNLOADED_CONDITION_TIMEOUT;
  }

  public static void closeExpiredAndIdleConnections() {
    synchronized (connectionManager) {
      logger.debug("connection pool stats: {}", connectionManager.getTotalStats());
      connectionManager.closeExpiredConnections();
      connectionManager.closeIdleConnections(DEFAULT_IDLE_CONNECTION_TIMEOUT, TimeUnit.SECONDS);
    }
  }

  public static void setProxyForS3(ClientConfiguration clientConfig) {
    if (useProxy) {
      clientConfig.setProxyHost(proxyHost);
      clientConfig.setProxyPort(proxyPort);
      clientConfig.setNonProxyHosts(nonProxyHosts);
      if (!Strings.isNullOrEmpty(proxyUser) && !Strings.isNullOrEmpty(proxyPassword)) {
        clientConfig.setProxyUsername(proxyUser);
        clientConfig.setProxyPassword(proxyPassword);
      }
    }
  }

  public static void setProxyForAzure(OperationContext opContext) {
    if (useProxy) {
      // currently, only host and port are supported. Username and password are not supported.
      Proxy azProxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyHost, proxyPort));
      opContext.setProxy(azProxy);
    }
  }

  /**
   * Constructs a user-agent header with the following pattern: connector_name/connector_version
   * (os-platform_info) language_implementation/language_version
   *
   * @return string for user-agent header
   */
  private static String buildUserAgent() {
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
    String userAgent = builder.toString();
    return userAgent;
  }

  /**
   * Build an Http client using our set of default.
   *
   * @param ocspMode OCSP mode
   * @param ocspCacheFile OCSP response cache file. If null, the default OCSP response file will be
   *     used.
   * @param downloadCompressed Whether the HTTP client should be built requesting no decompression
   * @return HttpClient object
   */
  static CloseableHttpClient buildHttpClient(
      OCSPMode ocspMode, File ocspCacheFile, boolean downloadCompressed) {
    // set timeout so that we don't wait forever.
    // Setup the default configuration for all requests on this client

    String ttlSystemProperty = systemGetProperty(JDBC_TTL);
    int timeToLive = DEFAULT_TTL;
    if (ttlSystemProperty != null) {
      try {
        timeToLive = Integer.parseInt(ttlSystemProperty);
      } catch (NumberFormatException ex) {
        logger.info("Failed to parse the system parameter {}", JDBC_TTL, ttlSystemProperty);
      }
    }
    logger.debug("time to live in connection pooling manager: {}", timeToLive);
    if (DefaultRequestConfig == null) {
      DefaultRequestConfig =
          RequestConfig.custom()
              .setConnectTimeout(DEFAULT_CONNECTION_TIMEOUT)
              .setConnectionRequestTimeout(DEFAULT_CONNECTION_TIMEOUT)
              .setSocketTimeout(DEFAULT_HTTP_CLIENT_SOCKET_TIMEOUT)
              .build();
    }

    TrustManager[] trustManagers = null;
    if (ocspMode != OCSPMode.INSECURE) {
      // A custom TrustManager is required only if insecureMode is disabled,
      // which is by default in the production. insecureMode can be enabled
      // 1) OCSP service is down for reasons, 2) PowerMock test tht doesn't
      // care OCSP checks.
      // OCSP FailOpen is ON by default
      try {
        TrustManager[] tm = {new SFTrustManager(ocspMode, ocspCacheFile)};
        trustManagers = tm;
      } catch (Exception | Error err) {
        // dump error stack
        StringWriter errors = new StringWriter();
        err.printStackTrace(new PrintWriter(errors));
        logger.error(errors.toString());
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
      connectionManager.setMaxTotal(DEFAULT_MAX_CONNECTIONS);
      connectionManager.setDefaultMaxPerRoute(DEFAULT_MAX_CONNECTIONS_PER_ROUTE);

      HttpClientBuilder httpClientBuilder =
          HttpClientBuilder.create()
              .setDefaultRequestConfig(DefaultRequestConfig)
              .setConnectionManager(connectionManager)
              // Support JVM proxy settings
              .useSystemProperties()
              .setRedirectStrategy(new DefaultRedirectStrategy())
              .setUserAgent(buildUserAgent()) // needed for Okta
              .disableCookieManagement(); // SNOW-39748

      if (useProxy) {
        // use the custom proxy properties
        HttpHost proxy = new HttpHost(proxyHost, proxyPort);
        SdkProxyRoutePlanner sdkProxyRoutePlanner =
            new SdkProxyRoutePlanner(proxyHost, proxyPort, nonProxyHosts);
        httpClientBuilder = httpClientBuilder.setProxy(proxy).setRoutePlanner(sdkProxyRoutePlanner);
        if (!Strings.isNullOrEmpty(proxyUser) && !Strings.isNullOrEmpty(proxyPassword)) {
          Credentials credentials = new UsernamePasswordCredentials(proxyUser, proxyPassword);
          AuthScope authScope = new AuthScope(proxyHost, proxyPort);
          CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
          credentialsProvider.setCredentials(authScope, credentials);
          httpClientBuilder = httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
        }
      }
      if (downloadCompressed) {
        httpClientBuilder = httpClientBuilder.disableContentCompression();
      }
      return httpClientBuilder.build();
    } catch (NoSuchAlgorithmException | KeyManagementException ex) {
      throw new SSLInitializationException(ex.getMessage(), ex);
    }
  }

  /**
   * Gets HttpClient with insecureMode false
   *
   * @param ocspMode OCSP mode
   * @return HttpClient object shared across all connections
   */
  public static CloseableHttpClient getHttpClient(OCSPMode ocspMode) {
    return initHttpClient(ocspMode != null ? ocspMode : OCSPMode.FAIL_OPEN, null);
  }

  /**
   * Gets HttpClient with insecureMode false and disabling decompression
   *
   * @param ocspMode OCSP mode
   * @return HttpClient object shared across all connections
   */
  public static CloseableHttpClient getHttpClientWithoutDecompression(OCSPMode ocspMode) {
    return initHttpClientWithoutDecompression(ocspMode, null);
  }

  /**
   * Accessor for the HTTP client singleton.
   *
   * @param ocspMode OCSP mode
   * @param ocspCacheFile OCSP response cache file name. if null, the default file will be used.
   * @return HttpClient object shared across all connections
   */
  public static CloseableHttpClient initHttpClientWithoutDecompression(
      OCSPMode ocspMode, File ocspCacheFile) {
    return httpClientWithoutDecompression.computeIfAbsent(
        ocspMode, k -> buildHttpClient(ocspMode, ocspCacheFile, true));
  }

  /**
   * Accessor for the HTTP client singleton.
   *
   * @param ocspMode OCSP mode
   * @param ocspCacheFile OCSP response cache file name. if null, the default file will be used.
   * @return HttpClient object shared across all connections
   */
  public static CloseableHttpClient initHttpClient(OCSPMode ocspMode, File ocspCacheFile) {
    return httpClient.computeIfAbsent(
        ocspMode, k -> buildHttpClient(ocspMode, ocspCacheFile, false));
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
   * HttpClient with the coopkie spec set to ignore.
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
   * @param injectSocketTimeout injecting socket timeout
   * @param canceling canceling?
   * @return response
   * @throws SnowflakeSQLException if Snowflake error occurs
   * @throws IOException raises if a general IO error occurs
   */
  static String executeRequestWithoutCookies(
      HttpRequestBase httpRequest,
      int retryTimeout,
      int injectSocketTimeout,
      AtomicBoolean canceling,
      OCSPMode ocspMode)
      throws SnowflakeSQLException, IOException {
    return executeRequestInternal(
        httpRequest,
        retryTimeout,
        injectSocketTimeout,
        canceling,
        true, // no cookie
        false, // no retry parameter
        true, // guid? (do we need this?)
        false, // no retry on HTTP 403
        ocspMode);
  }

  /**
   * Executes a HTTP request for Snowflake.
   *
   * @param httpRequest HttpRequestBase
   * @param retryTimeout retry timeout
   * @param ocspMode OCSP mode
   * @return response
   * @throws SnowflakeSQLException if Snowflake error occurs
   * @throws IOException raises if a general IO error occurs
   */
  public static String executeGeneralRequest(
      HttpRequestBase httpRequest, int retryTimeout, OCSPMode ocspMode)
      throws SnowflakeSQLException, IOException {
    return executeRequest(
        httpRequest,
        retryTimeout,
        0, // no inject socket timeout
        null, // no canceling
        false, // no retry parameter
        false, // no retry on HTTP 403
        ocspMode);
  }

  /**
   * Executes a HTTP request for Snowflake.
   *
   * @param httpRequest HttpRequestBase
   * @param retryTimeout retry timeout
   * @param injectSocketTimeout injecting socket timeout
   * @param canceling canceling?
   * @param includeRetryParameters whether to include retry parameters in retried requests
   * @param retryOnHTTP403 whether to retry on HTTP 403 or not
   * @param ocspMode OCSP mode
   * @return response
   * @throws SnowflakeSQLException if Snowflake error occurs
   * @throws IOException raises if a general IO error occurs
   */
  public static String executeRequest(
      HttpRequestBase httpRequest,
      int retryTimeout,
      int injectSocketTimeout,
      AtomicBoolean canceling,
      boolean includeRetryParameters,
      boolean retryOnHTTP403,
      OCSPMode ocspMode)
      throws SnowflakeSQLException, IOException {
    return executeRequestInternal(
        httpRequest,
        retryTimeout,
        injectSocketTimeout,
        canceling,
        false, // with cookie (do we need cookie?)
        includeRetryParameters,
        true, // include request GUID
        retryOnHTTP403,
        ocspMode);
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
   * @param injectSocketTimeout simulate socket timeout
   * @param canceling canceling flag
   * @param withoutCookies whether this request should ignore cookies
   * @param includeRetryParameters whether to include retry parameters in retried requests
   * @param includeRequestGuid whether to include request_guid
   * @param retryOnHTTP403 whether to retry on HTTP 403
   * @param ocspMode OCSPMode
   * @return response in String
   * @throws SnowflakeSQLException if Snowflake error occurs
   * @throws IOException raises if a general IO error occurs
   */
  private static String executeRequestInternal(
      HttpRequestBase httpRequest,
      int retryTimeout,
      int injectSocketTimeout,
      AtomicBoolean canceling,
      boolean withoutCookies,
      boolean includeRetryParameters,
      boolean includeRequestGuid,
      boolean retryOnHTTP403,
      OCSPMode ocspMode)
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
              getHttpClient(ocspMode),
              httpRequest,
              retryTimeout,
              injectSocketTimeout,
              canceling,
              withoutCookies,
              includeRetryParameters,
              includeRequestGuid,
              retryOnHTTP403);

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

      writer = new StringWriter();
      try (InputStream ins = response.getEntity().getContent()) {
        IOUtils.copy(ins, writer, "UTF-8");
      }

      theString = writer.toString();
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

  /** configure custom proxy properties from connectionPropertiesMap */
  public static void configureCustomProxyProperties(
      Map<SFSessionProperty, Object> connectionPropertiesMap) throws SnowflakeSQLException {
    if (connectionPropertiesMap.containsKey(SFSessionProperty.USE_PROXY)) {
      useProxy = (boolean) connectionPropertiesMap.get(SFSessionProperty.USE_PROXY);
    }

    if (useProxy) {
      proxyHost = (String) connectionPropertiesMap.get(SFSessionProperty.PROXY_HOST);
      try {
        proxyPort =
            Integer.parseInt(connectionPropertiesMap.get(SFSessionProperty.PROXY_PORT).toString());
      } catch (NumberFormatException | NullPointerException e) {
        throw new SnowflakeSQLException(
            ErrorCode.INVALID_PROXY_PROPERTIES, "Could not parse port number");
      }

      proxyUser = (String) connectionPropertiesMap.get(SFSessionProperty.PROXY_USER);
      proxyPassword = (String) connectionPropertiesMap.get(SFSessionProperty.PROXY_PASSWORD);
      nonProxyHosts = (String) connectionPropertiesMap.get(SFSessionProperty.NON_PROXY_HOSTS);
    }

    // parse JVM proxy settings. Print them out if JVM proxy is in usage.
    httpUseProxy = Boolean.parseBoolean(systemGetProperty("http.useProxy"));
    if (httpUseProxy) {
      String httpProxyHost = systemGetProperty("http.proxyHost");
      String httpProxyPort = systemGetProperty("http.proxyPort");
      String httpsProxyHost = systemGetProperty("https.proxyHost");
      String httpsProxyPort = systemGetProperty("https.proxyPort");
      String noProxy = systemGetEnv("NO_PROXY");
      logger.debug(
          "http.useProxy={}, http.proxyHost={}, http.proxyPort={}, https.proxyHost={}, https.proxyPort={}, NO_PROXY={}",
          httpUseProxy,
          httpProxyHost,
          httpProxyPort,
          httpsProxyHost,
          httpsProxyPort,
          noProxy);
    } else {
      logger.debug("http.useProxy={}. JVM proxy not used.", httpUseProxy);
    }
    // Always print off connection string proxy parameters. These override JVM proxy parameters if
    // use_proxy=true.
    logger.debug(
        "connection proxy parameters: use_proxy={}, proxy_host={}, proxy_port={}, proxy_user={}, proxy_password={}, non_proxy_hosts={}",
        useProxy,
        proxyHost,
        proxyPort,
        proxyUser,
        !Strings.isNullOrEmpty(proxyPassword) ? "***" : "(empty)",
        nonProxyHosts);
  }
}
