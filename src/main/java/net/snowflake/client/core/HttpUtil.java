package net.snowflake.client.core;

import static com.google.common.base.Throwables.getRootCause;
import static net.snowflake.client.jdbc.RestRequest.getNewBackoffInMilli;
import static net.snowflake.client.jdbc.RestRequest.isNonRetryableHTTPCode;
import static net.snowflake.client.jdbc.SnowflakeUtil.isNullOrEmpty;
import static net.snowflake.client.jdbc.ErrorCode.NETWORK_ERROR;
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
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLKeyException;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLProtocolException;
import javax.net.ssl.TrustManager;

import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.RestRequest;
import net.snowflake.client.jdbc.RetryContextManager;
import net.snowflake.client.jdbc.SnowflakeDriver;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.SnowflakeSQLLoggedException;
import net.snowflake.client.jdbc.SnowflakeUseDPoPNonceException;
import net.snowflake.client.jdbc.SnowflakeUtil;
import net.snowflake.client.jdbc.cloud.storage.S3HttpUtil;
import net.snowflake.client.jdbc.telemetryOOB.TelemetryService;
import net.snowflake.client.log.ArgSupplier;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.client.log.SFLoggerUtil;
import net.snowflake.client.util.SecretDetector;
import net.snowflake.client.util.Stopwatch;
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
import org.apache.http.client.utils.URIBuilder;
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

/**
 * HttpUtil class
 */
public class HttpUtil {
    private static final SFLogger logger = SFLoggerFactory.getLogger(HttpUtil.class);

    static final String ERROR_FIELD_NAME = "error";
    static final String ERROR_USE_DPOP_NONCE = "use_dpop_nonce";
    static final String DPOP_NONCE_HEADER_NAME = "dpop-nonce";

    static final int DEFAULT_MAX_CONNECTIONS = 300;
    static final int DEFAULT_MAX_CONNECTIONS_PER_ROUTE = 300;
    private static final int DEFAULT_HTTP_CLIENT_CONNECTION_TIMEOUT_IN_MS = 60000;
    static final int DEFAULT_HTTP_CLIENT_SOCKET_TIMEOUT_IN_MS = 300000; // ms
    static final int DEFAULT_MALFORMED_RESPONSE_MAX_RETRY_COUNT = 3; // ms
    static final int DEFAULT_MALFORMED_RESPONSE_RETRY_DELAY = 500; // ms
    static final int DEFAULT_TTL = 60; // secs
    static final int DEFAULT_IDLE_CONNECTION_TIMEOUT = 5; // secs
    static final int DEFAULT_DOWNLOADED_CONDITION_TIMEOUT = 3600; // secs

    public static final String JDBC_TTL = "net.snowflake.jdbc.ttl";
    public static final String JDBC_MAX_CONNECTIONS_PROPERTY = "net.snowflake.jdbc.max_connections";
    public static final String JDBC_MAX_CONNECTIONS_PER_ROUTE_PROPERTY =
            "net.snowflake.jdbc.max_connections_per_route";
//    public static final String JDBC_MALFORMED_RESPONSE_MAX_RETRY_COUNT_PROPERTY =
//            "net.snowflake.jdbc.default_malformed_response_max_retry_count";
//    public static final String JDBC_MALFORMED_RESPONSE_RETRY_DELAY =
//            "net.snowflake.jdbc.malformed_response_retry_delay";

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

    /**
     * The map of snowflake route planners
     */
    static Map<HttpClientSettingsKey, SnowflakeMutableProxyRoutePlanner> httpClientRoutePlanner =
            new ConcurrentHashMap<>();

    /**
     * Handle on the static connection manager, to gather statistics mainly
     */
    private static PoolingHttpClientConnectionManager connectionManager = null;

    /**
     * default request configuration, to be copied on individual requests.
     */
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
     * @param key          key to HttpClient map containing OCSP and proxy info
     * @param clientConfig the configuration needed by S3 to set the proxy
     * @deprecated Use {@link S3HttpUtil#setProxyForS3(HttpClientSettingsKey, ClientConfiguration)}
     * instead
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
     * @param clientConfig    the configuration needed by S3 to set the proxy
     * @throws SnowflakeSQLException when exception encountered
     * @deprecated Use {@link S3HttpUtil#setSessionlessProxyForS3(Properties, ClientConfiguration)}
     * instead
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
     * @param opContext       the configuration needed by Azure to set the proxy
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
     * @param key       key to HttpClient map containing OCSP and proxy info
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
     *                     usage.
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
     * @param key                  Key to HttpClient hashmap containing OCSP mode and proxy information, could be null
     * @param ocspCacheFile        OCSP response cache file. If null, the default OCSP response file will be
     *                             used.
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
     * @param key           contains information needed to build specific HttpClient
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
     * @param key           contains information needed to build specific HttpClient
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
     * @param soTimeoutMs    - custom socket timeout in milli-seconds
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
     * @param withoutCookies                 - whether this request should ignore cookies or not
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
     * @param httpRequest         HttpRequestBase
     * @param retryTimeout        retry timeout
     * @param authTimeout         authenticator specific timeout
     * @param socketTimeout       socket timeout (in ms)
     * @param retryCount          max retry count for the request - if it is set to 0, it will be ignored and
     *                            only retryTimeout will determine when to end the retries
     * @param injectSocketTimeout injecting socket timeout
     * @param canceling           canceling?
     * @param ocspAndProxyKey     OCSP mode and proxy settings for httpclient
     * @return response
     * @throws SnowflakeSQLException if Snowflake error occurs
     * @throws IOException           raises if a general IO error occurs
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
     * @param httpRequest            HttpRequestBase
     * @param retryTimeout           retry timeout
     * @param authTimeout            authenticator specific timeout
     * @param socketTimeout          socket timeout (in ms)
     * @param retryCount             max retry count for the request - if it is set to 0, it will be ignored and
     *                               only retryTimeout will determine when to end the retries
     * @param ocspAndProxyAndGzipKey OCSP mode and proxy settings for httpclient
     * @return response
     * @throws SnowflakeSQLException if Snowflake error occurs
     * @throws IOException           raises if a general IO error occurs
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
     * @param httpRequest   HttpRequestBase
     * @param retryTimeout  retry timeout
     * @param authTimeout   authenticator specific timeout
     * @param socketTimeout socket timeout (in ms)
     * @param retryCount    max retry count for the request - if it is set to 0, it will be ignored and
     *                      only retryTimeout will determine when to end the retries
     * @param httpClient    client object used to communicate with other machine
     * @return response
     * @throws SnowflakeSQLException if Snowflake error occurs
     * @throws IOException           raises if a general IO error occurs
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
     * @param httpRequest            HttpRequestBase
     * @param retryTimeout           retry timeout
     * @param authTimeout            authenticator timeout
     * @param socketTimeout          socket timeout (in ms)
     * @param maxRetries             retry count for the request
     * @param injectSocketTimeout    injecting socket timeout
     * @param canceling              canceling?
     * @param includeRetryParameters whether to include retry parameters in retried requests
     * @param retryOnHTTP403         whether to retry on HTTP 403 or not
     * @param ocspAndProxyKey        OCSP mode and proxy settings for httpclient
     * @param execTimeData           query execution time telemetry data object
     * @return response
     * @throws SnowflakeSQLException if Snowflake error occurs
     * @throws IOException           raises if a general IO error occurs
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
     * @param httpRequest            HttpRequestBase
     * @param retryTimeout           retry timeout
     * @param authTimeout            authenticator timeout
     * @param socketTimeout          socket timeout (in ms)
     * @param maxRetries             retry count for the request
     * @param injectSocketTimeout    injecting socket timeout
     * @param canceling              canceling?
     * @param includeRetryParameters whether to include retry parameters in retried requests
     * @param retryOnHTTP403         whether to retry on HTTP 403 or not
     * @param ocspAndProxyKey        OCSP mode and proxy settings for httpclient
     * @param execTimeData           query execution time telemetry data object
     * @param retryContextManager    RetryContext used to customize retry handling functionality
     * @return response
     * @throws SnowflakeSQLException if Snowflake error occurs
     * @throws IOException           raises if a general IO error occurs
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

//    public static <T> void mapNotRetryableException(Predicate<Exception> condition, SnowflakeSQLLoggedException action, IllegalStateException input) {
//        if (condition.test(input)) {
//            action.accept(input);  // Execute the action if the condition is true
//        } else {
//            System.out.println("Condition not met.");
//        }
//    }

//    public  Supplier<Exception> handleIllegalStateException() {
//        return (ex) -> {
//            try {
//                throw new SnowflakeSQLLoggedException(
//                        null, ErrorCode.INVALID_STATE, ex, /* session= */ ex.getMessage());
//        };
//    }
//
//        Consumer<IllegalStateException> exceptionHandler = ex -> {
//            // Perform some action with the IllegalStateException (e.g., log it)
//            System.out.println("Handling IllegalStateException: " + ex.getMessage());
//
//            // Throw a different exception, for example, a RuntimeException
//
//            throw new SnowflakeSQLLoggedException(
//                    null, ErrorCode.INVALID_STATE, ex, /* session= */ ex.getMessage());
//        };

//    Consumer<Throwable> throwException = ex -> {
//        throw new SnowflakeSQLLoggedException(
//                null, ErrorCode.INVALID_STATE, ex, /* session= */ ex.getMessage()); // Wraps the input exception in a RuntimeException
//    };
//
//
//    Consumer<Throwable> throwUncheckedException = ex -> {
//        if (ex instanceof IOException) {
//            throwUnchecked((IOException) ex);
//        }
//    };
//
//        static Function<IllegalStateException, SnowflakeSQLLoggedException> mapException = ex -> {
//             You can customize the message and logic here as needed
//            new SnowflakeSQLLoggedException(
//                    null, ErrorCode.INVALID_STATE, ex, /* session= */ ex.getMessage());
//        };

    private static Exception handlingNotRetryableException(Exception ex, HttpExecutingContext httpExecutingContext) throws SnowflakeSQLLoggedException {
        Set<Class<?>> sslExceptions = new HashSet<>();
        sslExceptions.add(SSLHandshakeException.class);
        sslExceptions.add(SSLKeyException.class);
        sslExceptions.add(SSLPeerUnverifiedException.class);
        sslExceptions.add(SSLProtocolException.class);
        Exception savedEx = null;
        if (ex instanceof IllegalStateException) {
            throw new SnowflakeSQLLoggedException(
                    null, ErrorCode.INVALID_STATE, ex, /* session= */ ex.getMessage());
        } else if (isExceptionInGroup(ex, sslExceptions)) {
            String formattedMsg =
                    ex.getMessage()
                            + "\n"
                            + "Verify that the hostnames and portnumbers in SYSTEM$ALLOWLIST are added to your firewall's allowed list.\n"
                            + "To troubleshoot your connection further, you can refer to this article:\n"
                            + "https://docs.snowflake.com/en/user-guide/client-connectivity-troubleshooting/overview";

            throw new SnowflakeSQLLoggedException(null, ErrorCode.NETWORK_ERROR, ex, formattedMsg);
        } else if (ex instanceof Exception) {
            savedEx = ex;
            // if the request took more than socket timeout log a warning
            long currentMillis = System.currentTimeMillis();
            if ((currentMillis - httpExecutingContext.getStartTimePerRequest()) > HttpUtil.getSocketTimeout().toMillis()) {
                logger.warn(
                        "{}HTTP request took longer than socket timeout {} ms: {} ms",
                        httpExecutingContext.getRequestId(),
                        HttpUtil.getSocketTimeout().toMillis(),
                        (currentMillis - httpExecutingContext.getStartTimePerRequest()));
            }
            StringWriter sw = new StringWriter();
            savedEx.printStackTrace(new PrintWriter(sw));
            logger.debug(
                    "{}Exception encountered for: {}, {}, {}",
                    httpExecutingContext.getRequestId(),
                    httpExecutingContext.getRequestInfoScrubbed(),
                    ex.getLocalizedMessage(),
                    (ArgSupplier) sw::toString);
        }
        return ex;
    }

    private static boolean isExceptionInGroup(Exception e, Set<Class<?>> group) {
        for (Class<?> clazz : group) {
            if (clazz.isInstance(e)) {
                return true;
            }
        }
        return false;
    }

    private static boolean handleCertificateRevoked(Exception savedEx, HttpExecutingContext httpExecutingContext, boolean skipRetrying) {
        if (!skipRetrying && RestRequest.isCertificateRevoked(savedEx)) {
            String msg = "Unknown reason";
            Throwable rootCause = RestRequest.getRootCause(savedEx);
            msg = rootCause.getMessage() != null && !rootCause.getMessage().isEmpty() ? rootCause.getMessage() : msg;
            logger.debug(
                    "{}Error response not retryable, " + msg + ", request: {}",
                    httpExecutingContext.getRequestId(),
                    httpExecutingContext.getRequestInfoScrubbed());
            EventUtil.triggerBasicEvent(
                    Event.EventType.NETWORK_ERROR, msg + ", Request: " + httpExecutingContext.getRequestInfoScrubbed(), false);

            httpExecutingContext.setBreakRetryReason("certificate revoked error");
            httpExecutingContext.setBreakRetryEventNam("HttpRequestRetryVertificateRevoked");
            httpExecutingContext.setShouldRetry(false);
            return true;
        }
        return skipRetrying;
    }

    private static boolean handleNoRetryiableHttpCode(CloseableHttpResponse response, Exception savedEx, HttpExecutingContext httpExecutingContext, boolean skipRetrying) {
        if (!skipRetrying && isNonRetryableHTTPCode(response, httpExecutingContext.isRetryHTTP403())) {
            String msg = "Unknown reason";
            if (response != null) {
                logger.debug(
                        "{}HTTP response code for request {}: {}",
                        httpExecutingContext.getRequestId(),
                        httpExecutingContext.getRequestInfoScrubbed(),
                        response.getStatusLine().getStatusCode());
                msg =
                        "StatusCode: "
                                + response.getStatusLine().getStatusCode()
                                + ", Reason: "
                                + response.getStatusLine().getReasonPhrase();
            } else if (savedEx != null) // may be null.
            {
                Throwable rootCause = RestRequest.getRootCause(savedEx);
                msg = rootCause.getMessage();
            }

            if (response == null || response.getStatusLine().getStatusCode() != 200) {
                logger.debug(
                        "{}Error response not retryable, " + msg + ", request: {}",
                        httpExecutingContext.getRequestId(),
                        httpExecutingContext.getRequestInfoScrubbed());
                EventUtil.triggerBasicEvent(
                        Event.EventType.NETWORK_ERROR, msg + ", Request: " + httpExecutingContext.getRequestInfoScrubbed(), false);
            }
            httpExecutingContext.setBreakRetryReason("status code does not need retry");
//            httpExecutingContext.resetRetryCount();
            httpExecutingContext.setShouldRetry(false);
            System.out.println(" status code does not need retry");
            return true;
        }
        return skipRetrying;
    }

    private static void logTelemetryEvent(HttpRequestBase request, CloseableHttpResponse response, Exception savedEx, HttpExecutingContext httpExecutingContext) {
        TelemetryService.getInstance()
                .logHttpRequestTelemetryEvent(
                        httpExecutingContext.getBreakRetryEventNam(),
                        request,
                        httpExecutingContext.getInjectSocketTimeout(),
                        httpExecutingContext.getCanceling(),
                        httpExecutingContext.isWithoutCookies(),
                        httpExecutingContext.includeRetryParameters,
                        httpExecutingContext.isIncludeRequestGuid(),
                        response,
                        savedEx,
                        httpExecutingContext.getBreakRetryReason(),
                        httpExecutingContext.getRetryTimeout(),
                        httpExecutingContext.getRetryCount(),
                        SqlState.IO_ERROR,
                        ErrorCode.NETWORK_ERROR.getMessageCode());
    }

    private static boolean handleMaxRetriesExceeded(HttpExecutingContext httpExecutingContext, boolean skipRetrying) {
        if (!skipRetrying && httpExecutingContext.maxRetriesExceeded()) {
            logger.error(
                    "{}Stop retrying as max retries have been reached for request: {}! Max retry count: {}",
                    httpExecutingContext.getRequestId(),
                    httpExecutingContext.getRequestInfoScrubbed(),
                    httpExecutingContext.getMaxRetries());

            System.out.println("Stop retrying as max retries have been reached for request");
            httpExecutingContext.setBreakRetryReason("max retries reached");
            httpExecutingContext.setBreakRetryEventNam("HttpRequestRetryLimitExceeded");
            httpExecutingContext.setShouldRetry(false);
            return true;
        }
        return skipRetrying;
    }

    private static boolean handleElapsedTimeoutExceeded(HttpExecutingContext httpExecutingContext, boolean skipRetrying) {
        if (!skipRetrying && httpExecutingContext.getRetryTimeoutInMilliseconds() > 0) {
            // Check for retry time-out.
            // increment total elapsed due to transient issues
            long elapsedMilliForLastCall = System.currentTimeMillis() - httpExecutingContext.getStartTimePerRequest();
            httpExecutingContext.increaseElapsedMilliForTransientIssues(elapsedMilliForLastCall);

            // check if the total elapsed time for transient issues has exceeded
            // the retry timeout and we retry at least the min, if so, we will not
            // retry
            if (httpExecutingContext.elapsedTimeExceeded() && httpExecutingContext.moreThanMinRetries()) {
                logger.error(
                        "{}Stop retrying since elapsed time due to network "
                                + "issues has reached timeout. "
                                + "Elapsed: {} ms, timeout: {} ms",
                        httpExecutingContext.getRequestId(),
                        httpExecutingContext.getElapsedMilliForTransientIssues(),
                        httpExecutingContext.getRetryTimeoutInMilliseconds());

                System.out.println("{}Stop retrying since elapsed time due to network \"\n" +
                        "                                + \"issues has reached timeout. \"\n" +
                        "                                + \"Elapsed: {} ms, timeout: {} ms\"");

                httpExecutingContext.setBreakRetryReason("retry timeout");
                httpExecutingContext.setBreakRetryEventNam("HttpRequestRetryTimeout");
                httpExecutingContext.setShouldRetry(false);
                return true;
            }
        }
        return skipRetrying;
    }

    private static boolean handleCancelingSignal(HttpExecutingContext httpExecutingContext, boolean skipRetrying) {
        if (!skipRetrying && httpExecutingContext.getCanceling() != null && httpExecutingContext.getCanceling().get()) {
            logger.debug("{}Stop retrying since canceling is requested", httpExecutingContext.getRequestId());
            System.out.println("{}Stop retrying since canceling is requested");
            httpExecutingContext.setBreakRetryReason("canceling is requested");
            httpExecutingContext.setShouldRetry(false);
            return true;
        }
        return skipRetrying;
    }

    private static boolean handleNoRetryFlag(HttpExecutingContext httpExecutingContext, boolean skipRetrying) {
        if (!skipRetrying && httpExecutingContext.isNoRetry()) {
            logger.debug(
                    "{}HTTP retry disabled for this request. noRetry: {}", httpExecutingContext.getRequestId(), httpExecutingContext.isNoRetry());
            httpExecutingContext.setBreakRetryReason("retry is disabled");
            httpExecutingContext.resetRetryCount();
            httpExecutingContext.setShouldRetry(false);
            return true;
        }
        return skipRetrying;
    }

    private static boolean shouldSkipRetryWithLoggedReason(HttpRequestBase request, CloseableHttpResponse response, Exception savedEx, HttpExecutingContext httpExecutingContext) throws SnowflakeSQLException {
        boolean skipRetrying = false;
        skipRetrying = handleNoRetryFlag(httpExecutingContext, skipRetrying);
        skipRetrying = handleCancelingSignal(httpExecutingContext, skipRetrying);
        skipRetrying = handleElapsedTimeoutExceeded(httpExecutingContext, skipRetrying);
        skipRetrying = handleMaxRetriesExceeded(httpExecutingContext, skipRetrying);
        skipRetrying = handleCertificateRevoked(savedEx, httpExecutingContext, skipRetrying);
        skipRetrying = handleNoRetryiableHttpCode(response, savedEx, httpExecutingContext, skipRetrying);
        logTelemetryEvent(request, response, savedEx, httpExecutingContext);
        return skipRetrying;
    }

    public static String executeWitRetries(
            CloseableHttpClient httpClient,
            HttpRequestBase httpRequest,
            HttpExecutingContext httpExecutingContext,
            ExecTimeTelemetryData execTimeData,
            RetryContextManager retryManager)
            throws SnowflakeSQLException {
        String responseText = null;
        Stopwatch networkComunnicationStapwatch = null;
        Stopwatch requestReponseStopWatch = null;
        CloseableHttpResponse response = null;
        Exception savedEx = null;

        if (logger.isDebugEnabled()) {
            networkComunnicationStapwatch = new Stopwatch();
            networkComunnicationStapwatch.start();
            logger.debug(
                    "{}Executing rest request: {}, retry timeout: {}, socket timeout: {}, max retries: {},"
                            + " inject socket timeout: {}, canceling: {}, without cookies: {}, include retry parameters: {},"
                            + " include request guid: {}, retry http 403: {}, no retry: {}",
                    httpExecutingContext.getRequestId(),
                    httpExecutingContext.getRequestInfoScrubbed(),
                    httpExecutingContext.getRetryTimeoutInMilliseconds(),
                    httpExecutingContext.getOrigSocketTimeout(),
                    httpExecutingContext.getMaxRetries(),
                    httpExecutingContext.isInjectSocketTimeout(),
                    httpExecutingContext.getCanceling(),
                    httpExecutingContext.isWithoutCookies(),
                    httpExecutingContext.isIncludeRetryParameters(),
                    httpExecutingContext.isIncludeRequestGuid(),
                    httpExecutingContext.isRetryHTTP403(),
                    httpExecutingContext.isNoRetry());

        }

        if (httpExecutingContext.isLoginRequest()) {
            logger.debug("{}Request is a login/auth request. Using new retry strategy", httpExecutingContext.getRequestId());
        }

        RestRequest.setRequestConfig(
                httpRequest, httpExecutingContext.isWithoutCookies(), httpExecutingContext.getInjectSocketTimeout(),
                httpExecutingContext.getRequestId(), httpExecutingContext.getAuthTimeoutInMilliseconds());
        // try request till we get a good response or retry timeout
        while (true) {
            System.out.println(" RETRY: " + httpExecutingContext.getRetryCount());
            logger.debug(
                    "{}Retry count: {}, max retries: {}, retry timeout: {} s, backoff: {} ms. Attempting request: {}",
                    httpExecutingContext.getRequestId(),
                    httpExecutingContext.getRetryCount(),
                    httpExecutingContext.getMaxRetries(),
                    httpExecutingContext.getRetryTimeout(),
                    httpExecutingContext.getMinBackoffInMillis(),
                    httpExecutingContext.getRequestInfoScrubbed());
            try {
                // update start time
                httpExecutingContext.setStartTimePerRequest(System.currentTimeMillis());

                RestRequest.setRequestURI(
                        httpRequest,
                        httpExecutingContext.getRequestId(),
                        httpExecutingContext.isIncludeRetryParameters(),
                        httpExecutingContext.isIncludeRequestGuid(),
                        httpExecutingContext.getRetryCount(),
                        httpExecutingContext.getLastStatusCodeForRetry(),
                        httpExecutingContext.getStartTime(),
                        httpExecutingContext.getRequestInfoScrubbed());

                execTimeData.setHttpClientStart();
                response = httpClient.execute(httpRequest);
                execTimeData.setHttpClientEnd();
            } catch (Exception ex) {
                savedEx = handlingNotRetryableException(ex, httpExecutingContext);
            } finally {
                // Reset the socket timeout to its original value if it is not the
                // very first iteration.
                if (httpExecutingContext.getInjectSocketTimeout() != 0 && httpExecutingContext.getRetryCount() == 0) {
                    // test code path
                    httpRequest.setConfig(
                            HttpUtil.getDefaultRequestConfigWithSocketTimeout(httpExecutingContext.getOrigSocketTimeout(), httpExecutingContext.isWithoutCookies()));
                }
            }
            System.out.println("UNPACK RESPONSE shouldRetry: ");
            boolean shouldSkipRetry = shouldSkipRetryWithLoggedReason(httpRequest, response, savedEx, httpExecutingContext);
            System.out.println("UNPACK RESPONSE shouldSkipRetry end: " + shouldSkipRetry);
            httpExecutingContext.setShouldRetry(!shouldSkipRetry);

            if ((!shouldSkipRetry && response != null && response.getStatusLine().getStatusCode() == 200)) {
                responseText = processHttpResponse(httpExecutingContext, execTimeData, response, savedEx);
            }


            if (!httpExecutingContext.isShouldRetry()) {
                if (response == null) {
                    if (savedEx != null) {
                        logger.error(
                                "{}Returning null response. Cause: {}, request: {}",
                                httpExecutingContext.getRequestId(),
                                getRootCause(savedEx),
                                httpExecutingContext.getRequestInfoScrubbed());
                    } else {
                        logger.error(
                                "{}Returning null response for request: {}", httpExecutingContext.getRequestId(), httpExecutingContext.getRequestInfoScrubbed());
                    }
                } else if (response.getStatusLine().getStatusCode() != 200) {
                    logger.error(
                            "{}Error response: HTTP Response code: {}, request: {}",
                            httpExecutingContext.getRequestId(),
                            response.getStatusLine().getStatusCode(),
                            httpExecutingContext.getRequestInfoScrubbed());
                } else if ((response == null || response.getStatusLine().getStatusCode() != 200)) {

                    sendTelemetryEvent(httpRequest, httpExecutingContext, response, savedEx);

                }
                break;
            } else {
                System.out.println("PREPARE RETRY: ");
                savedEx = prepareRetry(httpRequest, httpExecutingContext, retryManager, response, savedEx);
            }
        }


        logger.debug(
                "{}Execution of request {} took {} ms with total of {} retries",
                httpExecutingContext.getRequestId(),
                httpExecutingContext.getRequestInfoScrubbed(),
                networkComunnicationStapwatch == null ? "n/a" : networkComunnicationStapwatch.elapsedMillis(),
                httpExecutingContext.getRetryCount());

        if (response != null && responseText == null)
            try {
                System.out.println("FINAL UNPACK RESPONSE: ");
                responseText = verifyAndUnpackResponse(response, httpExecutingContext.getRequestInfoScrubbed(), execTimeData);
            } catch (IOException ex) {
                System.out.println("UNPACK RESPONSE ERROR: " + ex.getMessage());
                savedEx = ex;
            }

        System.out.println(" RESPONSE: " + responseText);
        System.out.println(" SAVED EX: " + savedEx);
        httpExecutingContext.resetRetryCount();
        if (logger.isDebugEnabled() && networkComunnicationStapwatch != null) {
            networkComunnicationStapwatch.stop();
        }
        if (savedEx != null) {
            throw new SnowflakeSQLException(
                    savedEx,
                    ErrorCode.NETWORK_ERROR,
                    "Exception encountered for HTTP request: " + savedEx.getMessage());
        }
        return responseText;
    }

    private static String processHttpResponse(HttpExecutingContext httpExecutingContext, ExecTimeTelemetryData execTimeData, CloseableHttpResponse response, Exception savedEx){
        try {
            String responseText;
            System.out.println("UNPACK RESPONSE: ");
            responseText = verifyAndUnpackResponse(response, httpExecutingContext.getRequestInfoScrubbed(), execTimeData);
            httpExecutingContext.setShouldRetry(false);
            return responseText;
        } catch (IOException | SnowflakeSQLException ex) {
            System.out.println("UNPACK RESPONSE ERROR: " + ex.getMessage());
            httpExecutingContext.setShouldRetry(true);
            savedEx = ex;
        }
        return null;
    }

    private static Exception prepareRetry(HttpRequestBase httpRequest, HttpExecutingContext
            httpExecutingContext, RetryContextManager retryManager, CloseableHttpResponse response, Exception savedEx) throws
            SnowflakeSQLException {
        //        Potentially retryable error
        logRequestResult(response, httpExecutingContext.getRequestId(), httpExecutingContext.getRequestInfoScrubbed(), savedEx);

        // get the elapsed time for the last request
        // elapsed in millisecond for last call, used for calculating the
        // remaining amount of time to sleep:
        // (backoffInMilli - elapsedMilliForLastCall)
        long elapsedMilliForLastCall = System.currentTimeMillis() - httpExecutingContext.getStartTimePerRequest();


        if (httpExecutingContext.socketOrConnectTimeoutReached())
            /* socket timeout not reached */ {
            /* connect timeout not reached */
            // check if this is a login-request
            if (String.valueOf(httpRequest.getURI()).contains("login-request")) {
                throw new SnowflakeSQLException(
                        ErrorCode.AUTHENTICATOR_REQUEST_TIMEOUT,
                        httpExecutingContext.getRetryCount(),
                        true,
                        httpExecutingContext.getElapsedMilliForTransientIssues() / 1000);
            }
        }

        // sleep for backoff - elapsed amount of time
        System.out.println("SLEEP");
        sleepForBackoffAndPrepareNext(elapsedMilliForLastCall, httpExecutingContext);


        httpExecutingContext.incrementRetryCount();
        System.out.println("INCREMENTED " + httpExecutingContext.getRetryCount());
        System.out.println("MAxRetries " + httpExecutingContext.getMaxRetries());
        httpExecutingContext.setLastStatusCodeForRetry(response == null ? "0" : String.valueOf(response.getStatusLine().getStatusCode()));
        // If the request failed with any other retry-able error and auth timeout is reached
        // increase the retry count and throw special exception to renew the token before retrying.

        RetryContextManager.RetryHook retryManagerHook = null;
        if (retryManager != null) {
            retryManagerHook = retryManager.getRetryHook();
            retryManager
                    .getRetryContext()
                    .setElapsedTimeInMillis(httpExecutingContext.getElapsedMilliForTransientIssues())
                    .setRetryTimeoutInMillis(httpExecutingContext.getRetryTimeoutInMilliseconds());
        }

        // Make sure that any authenticator specific info that needs to be
        // updated gets updated before the next retry. Ex - OKTA OTT, JWT token
        // Aim is to achieve this using RetryContextManager, but raising
        // AUTHENTICATOR_REQUEST_TIMEOUT Exception is still supported as well. In both cases the
        // retried request must be aware of the elapsed time not to exceed the timeout limit.
        if (retryManagerHook == RetryContextManager.RetryHook.ALWAYS_BEFORE_RETRY) {
            retryManager.executeRetryCallbacks(httpRequest);
        }

        if (httpExecutingContext.getAuthTimeout() > 0 && httpExecutingContext.getElapsedMilliForTransientIssues() >= httpExecutingContext.getAuthTimeout()) {
            System.out.println("ErrorCode.AUTHENTICATOR_REQUEST_TIMEOUT");
            throw new SnowflakeSQLException(
                    ErrorCode.AUTHENTICATOR_REQUEST_TIMEOUT,
                    httpExecutingContext.getRetryCount(),
                    false,
                    httpExecutingContext.getElapsedMilliForTransientIssues() / 1000);
        }

        int numOfRetryToTriggerTelemetry =
                TelemetryService.getInstance().getNumOfRetryToTriggerTelemetry();
        if (httpExecutingContext.getRetryCount() == numOfRetryToTriggerTelemetry) {
            TelemetryService.getInstance()
                    .logHttpRequestTelemetryEvent(
                            String.format("HttpRequestRetry%dTimes", numOfRetryToTriggerTelemetry),
                            httpRequest,
                            httpExecutingContext.getInjectSocketTimeout(),
                            httpExecutingContext.getCanceling(),
                            httpExecutingContext.isWithoutCookies(),
                            httpExecutingContext.isIncludeRetryParameters(),
                            httpExecutingContext.isIncludeRequestGuid(),
                            response,
                            savedEx,
                            httpExecutingContext.getBreakRetryReason(),
                            httpExecutingContext.getRetryTimeout(),
                            httpExecutingContext.getRetryCount(),
                            SqlState.IO_ERROR,
                            ErrorCode.NETWORK_ERROR.getMessageCode());
        }
        savedEx = null;

        // release connection before retry
        httpRequest.releaseConnection();
        System.out.println("SLEEP savedEx" + savedEx);
        return savedEx;
    }

    private static void sendTelemetryEvent(HttpRequestBase httpRequest, HttpExecutingContext
            httpExecutingContext, CloseableHttpResponse response, Exception savedEx) {
        String eventName;
        if (response == null) {
            eventName = "NullResponseHttpError";
        } else {
            if (response.getStatusLine() == null) {
                eventName = "NullResponseStatusLine";
            } else {
                eventName = String.format("HttpError%d", response.getStatusLine().getStatusCode());
            }
        }
        TelemetryService.getInstance()
                .logHttpRequestTelemetryEvent(
                        eventName,
                        httpRequest,
                        httpExecutingContext.getInjectSocketTimeout(),
                        httpExecutingContext.getCanceling(),
                        httpExecutingContext.isWithoutCookies(),
                        httpExecutingContext.isIncludeRetryParameters(),
                        httpExecutingContext.isIncludeRequestGuid(),
                        response,
                        savedEx,
                        httpExecutingContext.getBreakRetryReason(),
                        httpExecutingContext.getRetryTimeout(),
                        httpExecutingContext.getRetryCount(),
                        null,
                        0);
    }

    private static void sleepForBackoffAndPrepareNext(long elapsedMilliForLastCall, HttpExecutingContext
            context) {
        if (context.getMinBackoffInMillis() > elapsedMilliForLastCall) {
            try {
                logger.debug(
                        "{}Retry request {}: sleeping for {} ms",
                        context.getRequestId(),
                        context.getRequestInfoScrubbed(),
                        context.getBackoffInMillis());
                System.out.println("SLEEPTIME: " + context.getBackoffInMillis());
                Thread.sleep(context.getBackoffInMillis());
            } catch (InterruptedException ex1) {
                System.out.println("Backoff sleep before retrying login got in");
                logger.debug("{}Backoff sleep before retrying login got interrupted", context.getRequestId());
            }
            context.increaseElapsedMilliForTransientIssues(context.getBackoffInMillis());
            context.setBackoffInMillis(
                    getNewBackoffInMilli(
                            context.getBackoffInMillis(),
                            context.isLoginRequest(),
                            context.getBackoff(),
                            context.getRetryCount(),
                            context.getRetryTimeoutInMilliseconds(),
                            context.getElapsedMilliForTransientIssues()));
        }
    }

    private static void logRequestResult(CloseableHttpResponse response, String requestIdStr, String
            requestInfoScrubbed, Exception savedEx) {
        if (response != null) {
            logger.debug(
                    "{}HTTP response not ok: status code: {}, request: {}",
                    requestIdStr,
                    response.getStatusLine().getStatusCode(),
                    requestInfoScrubbed);
        } else if (savedEx != null) {
            logger.debug(
                    "{}Null response for cause: {}, request: {}",
                    requestIdStr,
                    getRootCause(savedEx).getMessage(),
                    requestInfoScrubbed);
        } else {
            logger.debug("{}Null response for request: {}", requestIdStr, requestInfoScrubbed);
        }
    }


    /**
     * Helper to execute a request with retry and check and throw exception if response is not
     * success. This should be used only for small request has it execute the REST request and get
     * back the result as a string.
     *
     * <p>Connection under the httpRequest is released.
     *
     * @param httpRequest            request object contains all the information
     * @param retryTimeout           retry timeout (in seconds)
     * @param authTimeout            authenticator specific timeout (in seconds)
     * @param socketTimeout          socket timeout (in ms)
     * @param maxRetries             retry count for the request
     * @param injectSocketTimeout    simulate socket timeout
     * @param canceling              canceling flag
     * @param withoutCookies         whether this request should ignore cookies
     * @param includeRetryParameters whether to include retry parameters in retried requests
     * @param includeRequestGuid     whether to include request_guid
     * @param retryOnHTTP403         whether to retry on HTTP 403
     * @param httpClient             client object used to communicate with other machine
     * @param retryContextManager    RetryContext used to customize retry handling functionality
     * @return response in String
     * @throws SnowflakeSQLException if Snowflake error occurs
     * @throws IOException           raises if a general IO error occurs
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
        String requestInfoScrubbed = SecretDetector.maskSASToken(httpRequest.toString());
        String responseText = "";

        logger.debug(
                "Pool: {} Executing: {}",
                (ArgSupplier) HttpUtil::getHttpClientStats,
                requestInfoScrubbed);

        CloseableHttpResponse response = null;
        Stopwatch stopwatch = null;

        try {
//            if (includeRetryParameters && retryCount > 0) {
//                URIBuilder uriBuilder = new URIBuilder(httpRequest.getURI());
//                RestRequest.updateRetryParameters(
//                        uriBuilder, retryCount, String.valueOf(NETWORK_ERROR), System.currentTimeMillis());
//                httpRequest.setURI(uriBuilder.build());
//            }
            String requestIdStr = URLUtil.getRequestIdLogStr(httpRequest.getURI());
            HttpExecutingContext context = new HttpExecutingContext(requestIdStr, requestInfoScrubbed);
            context.setRetryTimeout(retryTimeout);
            context.setAuthTimeout(authTimeout);
            context.setOrigSocketTimeout(socketTimeout);
            context.setMaxRetries(maxRetries);
            context.setInjectSocketTimeout(injectSocketTimeout);
            context.setCanceling(canceling);
            context.setWithoutCookies(withoutCookies);
            context.setIncludeRetryParameters(includeRetryParameters);
            context.setIncludeRequestGuid(includeRequestGuid);
            context.setRetryHTTP403(retryOnHTTP403);
            context.setNoRetry(false);
            responseText =
                    executeWitRetries(
                            httpClient,
                            httpRequest,
                            context,
                            execTimeData,
                            retryContextManager);


//        } catch (URISyntaxException e) {
//            throw new RuntimeException(e);
        } catch (SnowflakeSQLException e) {
            throw e;
        }

        logger.debug(
                "Pool: {} Request returned for: {} took {} ms",
                (ArgSupplier) HttpUtil::getHttpClientStats,
                requestInfoScrubbed,
                stopwatch == null ? "n/a" : stopwatch.elapsedMillis());

        return responseText;
    }

    private static String verifyAndUnpackResponse(CloseableHttpResponse response, String
            requestInfoScrubbed, ExecTimeTelemetryData execTimeData) throws IOException, SnowflakeSQLException {
        try (StringWriter writer = new StringWriter()) {
            if (response == null || response.getStatusLine().getStatusCode() != 200) {
                logger.error("Error executing request: {}", requestInfoScrubbed);

                if (response != null
                        && response.getStatusLine().getStatusCode() == 400
                        && response.getEntity() != null) {
                    checkForDPoPNonceError(response);
                }

                SnowflakeUtil.logResponseDetails(response, logger);
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
            try (InputStream ins = response.getEntity().getContent()) {
                IOUtils.copy(ins, writer, "UTF-8");
            }

            execTimeData.setResponseIOStreamEnd();
            return writer.toString();
        } finally {
            IOUtils.closeQuietly(response);
        }
    }

    private static void checkForDPoPNonceError(CloseableHttpResponse response) throws IOException {
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
     * @param request           The request to add headers to. Must not be null.
     * @param additionalHeaders The headers to add. May be null.
     */
    static void applyAdditionalHeadersForSnowsight(
            HttpRequestBase request, Map<String, String> additionalHeaders) {
        if (additionalHeaders != null && !additionalHeaders.isEmpty()) {
            additionalHeaders.forEach(request::addHeader);
        }
    }
}
