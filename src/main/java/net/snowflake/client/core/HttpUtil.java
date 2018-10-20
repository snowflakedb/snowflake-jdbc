/*
 * Copyright (c) 2012-2018 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.RestRequest;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.SnowflakeUtil;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.common.core.SqlState;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.HttpContext;
import org.apache.http.ssl.SSLInitializationException;
import org.apache.http.util.EntityUtils;

import javax.net.ssl.TrustManager;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.Proxy;
import java.net.Socket;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.http.client.config.CookieSpecs.DEFAULT;
import static org.apache.http.client.config.CookieSpecs.IGNORE_COOKIES;

/**
 * Created by jhuang on 1/19/16.
 */
public class HttpUtil
{
  static final SFLogger logger = SFLoggerFactory.getLogger(HttpUtil.class);

  static final int DEFAULT_MAX_CONNECTIONS = 100;
  static final int DEFAULT_MAX_CONNECTIONS_PER_ROUTE = 100;
  static final int DEFAULT_CONNECTION_TIMEOUT = 60000;
  static final int DEFAULT_HTTP_CLIENT_SOCKET_TIMEOUT = 300000; // ms

  /**
   * The unique httpClient shared by all connections, this will benefit long
   * lived clients
   */
  private static CloseableHttpClient httpClient = null;

  /**
   * Handle on the static connection manager, to gather statistics mainly
   */
  private static PoolingHttpClientConnectionManager connectionManager = null;

  /**
   * default request configuration, to be copied on individual requests.
   */
  private static RequestConfig DefaultRequestConfig = null;


  private static boolean socksProxyDisabled = false;

  /**
   * Build an Http client using our set of default.
   *
   * @param insecureMode  skip OCSP revocation check if true or false.
   * @param ocspCacheFile OCSP response cache file. If null, the default
   *                      OCSP response file will be used.
   * @return HttpClient object
   */
  static CloseableHttpClient buildHttpClient(
      boolean insecureMode, File ocspCacheFile, boolean useOcspCacheServer)
  {
    // set timeout so that we don't wait forever.
    // Setup the default configuration for all requests on this client
    DefaultRequestConfig =
        RequestConfig.custom()
            .setConnectTimeout(DEFAULT_CONNECTION_TIMEOUT)
            .setConnectionRequestTimeout(DEFAULT_CONNECTION_TIMEOUT)
            .setSocketTimeout(DEFAULT_HTTP_CLIENT_SOCKET_TIMEOUT)
            .build();

    TrustManager[] trustManagers = null;
    if (!insecureMode)
    {
      // A custom TrustManager is required only if insecureMode is disabled,
      // which is by default in the production. insecureMode can be enabled
      // 1) OCSP service is down for reasons, 2) PowerMock test tht doesn't
      // care OCSP checks.
      TrustManager[] tm = {
          new SFTrustManager(ocspCacheFile, useOcspCacheServer)};
      trustManagers = tm;
    }
    try
    {
      Registry<ConnectionSocketFactory> registry =
          RegistryBuilder.<ConnectionSocketFactory>create()
              .register("https",
                  new SFSSLConnectionSocketFactory(trustManagers, socksProxyDisabled))
              .register("http",
                  new SFConnectionSocketFactory())
              .build();

      // Build a connection manager with enough connections
      connectionManager = new PoolingHttpClientConnectionManager(registry);
      connectionManager.setMaxTotal(DEFAULT_MAX_CONNECTIONS);
      connectionManager.setDefaultMaxPerRoute(DEFAULT_MAX_CONNECTIONS_PER_ROUTE);

      httpClient =
          HttpClientBuilder.create()
              .setDefaultRequestConfig(DefaultRequestConfig)
              .setConnectionManager(connectionManager)
              // Support JVM proxy settings
              .useSystemProperties()
              .setRedirectStrategy(new DefaultRedirectStrategy())
              .setUserAgent("-")     // needed for Okta
              .disableCookieManagement() // SNOW-39748
              .build();

      return httpClient;
    }
    catch (NoSuchAlgorithmException | KeyManagementException ex)
    {
      throw new SSLInitializationException(ex.getMessage(), ex);
    }
  }

  /**
   * Gets HttpClient with insecureMode false
   *
   * @return HttpClient object shared across all connections
   */
  public static CloseableHttpClient getHttpClient()
  {
    return initHttpClient(true, null);
  }

  /**
   * Accessor for the HTTP client singleton.
   *
   * @param insecureMode skip OCSP revocation check if true.
   * @param ocspCacheFile OCSP response cache file name. if null, the default
   *                      file will be used.
   * @return HttpClient object shared across all connections
   */
  public static CloseableHttpClient initHttpClient(boolean insecureMode, File ocspCacheFile)
  {
    if (httpClient == null)
    {
      synchronized (HttpUtil.class)
      {
        if (httpClient == null)
        {
          String flag = System.getenv("SF_OCSP_RESPONSE_CACHE_SERVER_ENABLED");
          if (flag == null)
          {
            flag = System.getProperty(
                "net.snowflake.jdbc.ocsp_response_cache_server_enabled");
          }
          httpClient = buildHttpClient(
              insecureMode,
              ocspCacheFile,
              flag == null || !"false".equalsIgnoreCase(flag));
        }
      }
    }
    return httpClient;
  }

  /**
   * Return a request configuration inheriting from the default request
   * configuration of the shared HttpClient with a different socket timeout.
   *
   * @param soTimeoutMs    - custom socket timeout in milli-seconds
   * @param withoutCookies - whether this request should ignore cookies or not
   * @return RequestConfig object
   */
  public static RequestConfig
  getDefaultRequestConfigWithSocketTimeout(int soTimeoutMs,
                                           boolean withoutCookies)
  {
    getHttpClient();
    final String cookieSpec = withoutCookies ? IGNORE_COOKIES : DEFAULT;
    return RequestConfig.copy(DefaultRequestConfig)
        .setSocketTimeout(soTimeoutMs)
        .setCookieSpec(cookieSpec)
        .build();
  }

  /**
   * Return a request configuration inheriting from the default request
   * configuration of the shared HttpClient with the coopkie spec set to ignore.
   *
   * @return RequestConfig object
   */
  public static RequestConfig getRequestConfigWithoutcookies()
  {
    getHttpClient();
    return RequestConfig.copy(DefaultRequestConfig)
        .setCookieSpec(IGNORE_COOKIES)
        .build();
  }

  /**
   * Accessor for the HTTP client singleton.
   *
   * @return HTTP Client stats in string representation
   */
  private static String getHttpClientStats()
  {
    return connectionManager == null ?
        "" :
        connectionManager.getTotalStats().toString();
  }

  /**
   * Enables/disables use of the SOCKS proxy when creating sockets
   * @param socksProxyDisabled new value
   */
  public static void setSocksProxyDisabled(boolean socksProxyDisabled)
  {
    HttpUtil.socksProxyDisabled = socksProxyDisabled;
  }

  /**
   * Returns whether the SOCKS proxy is disabled for this JVM
   * @return whether the SOCKS proxy is disabled
   */
  public static boolean isSocksProxyDisabled()
  {
    return HttpUtil.socksProxyDisabled;
  }

  /**
   * Executes a HTTP request with the cookie spec set to IGNORE_COOKIES
   *
   * @param httpRequest HttpRequestBase
   * @param retryTimeout retry timeout
   * @param injectSocketTimeout injecting socket timeout
   * @param canceling canceling?
   * @throws SnowflakeSQLException if Snowflake error occurs
   * @throws IOException raises if a general IO error occurs
   * @return response
   */
  static String executeRequestWithoutCookies(HttpRequestBase httpRequest,
                                             int retryTimeout,
                                             int injectSocketTimeout,
                                             AtomicBoolean canceling)
      throws SnowflakeSQLException, IOException
  {
    return executeRequestInternal(
        httpRequest,
        retryTimeout,
        injectSocketTimeout,
        canceling,
        true,
        false,
        true);
  }

  /**
   * Executes a HTTP request for Snowflake.
   *
   * @param httpRequest HttpRequestBase
   * @param retryTimeout retry timeout
   * @param injectSocketTimeout injecting socket timeout
   * @param canceling canceling?
   * @return response
   * @throws SnowflakeSQLException if Snowflake error occurs
   * @throws IOException raises if a general IO error occurs
   */
  public static String executeRequest(HttpRequestBase httpRequest,
                                      int retryTimeout,
                                      int injectSocketTimeout,
                                      AtomicBoolean canceling)
      throws SnowflakeSQLException, IOException
  {
    return executeRequest(
        httpRequest,
        retryTimeout,
        injectSocketTimeout,
        canceling,
        false);
  }

  /**
   * Executes a HTTP request for Snowflake.
   *
   * @param httpRequest HttpRequestBase
   * @param retryTimeout retry timeout
   * @param injectSocketTimeout injecting socket timeout
   * @param canceling canceling?
   * @param includeRetryParameters whether to include retry parameters in
   *                               retried requests
   * @return response
   * @throws SnowflakeSQLException if Snowflake error occurs
   * @throws IOException raises if a general IO error occurs
   */
  public static String executeRequest(HttpRequestBase httpRequest,
                                      int retryTimeout,
                                      int injectSocketTimeout,
                                      AtomicBoolean canceling,
                                      boolean includeRetryParameters)
      throws SnowflakeSQLException, IOException
  {
    return executeRequestInternal(
        httpRequest,
        retryTimeout,
        injectSocketTimeout,
        canceling,
        false,
        includeRetryParameters,
        true);
  }

  /**
   * Helper to execute a request with retry and check and throw exception if
   * response is not success.
   * This should be used only for small request has it execute the REST
   * request and get back the result as a string.
   * <p>
   * Connection under the httpRequest is released.
   *
   * @param httpRequest         request object contains all the information
   * @param retryTimeout        retry timeout (in seconds)
   * @param injectSocketTimeout simulate socket timeout
   * @param canceling           canceling flag
   * @param withoutCookies      whether this request should ignore cookies
   * @param includeRetryParameters whether to include retry parameters in
   *                               retried requests
   * @param includeRequestGuid whether to include request_guid
   * @return response in String
   * @throws SnowflakeSQLException if Snowflake error occurs
   * @throws IOException raises if a general IO error occurs
   */
  private static String executeRequestInternal(HttpRequestBase httpRequest,
                                               int retryTimeout,
                                               int injectSocketTimeout,
                                               AtomicBoolean canceling,
                                               boolean withoutCookies,
                                               boolean includeRetryParameters,
                                               boolean includeRequestGuid)
      throws SnowflakeSQLException, IOException
  {
    if (logger.isDebugEnabled())
    {
      logger.debug("Pool: {} Executing: {}",
          HttpUtil.getHttpClientStats(),
          httpRequest);
    }

    String theString;
    StringWriter writer = null;
    CloseableHttpResponse response = null;
    try
    {
      response = RestRequest.execute(getHttpClient(),
          httpRequest,
          retryTimeout,
          injectSocketTimeout,
          canceling,
          withoutCookies,
          includeRetryParameters,
          includeRequestGuid);

      if (response == null ||
          response.getStatusLine().getStatusCode() != 200)
      {
        logger.error("Error executing request: {}",
            httpRequest.toString());

        SnowflakeUtil.logResponseDetails(response, logger);

        if (response != null)
        {
          EntityUtils.consume(response.getEntity());
        }

        throw new SnowflakeSQLException(SqlState.IO_ERROR,
            ErrorCode.NETWORK_ERROR.getMessageCode(),
            "HTTP status="
                + ((response != null) ?
                response.getStatusLine().getStatusCode() :
                "null response"));
      }

      writer = new StringWriter();
      try (InputStream ins = response.getEntity().getContent())
      {
        IOUtils.copy(ins, writer, "UTF-8");
      }

      theString = writer.toString();
    }
    finally
    {
      IOUtils.closeQuietly(writer);
      IOUtils.closeQuietly(response);
    }

    if (logger.isDebugEnabled())
    {
      logger.debug(
          "Pool: {} Request returned for: {}",
          HttpUtil.getHttpClientStats(),
          httpRequest);
    }

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
  public final static class HttpInputStream extends InputStream
  {
    private final InputStream httpIn;

    public HttpInputStream(InputStream httpIn)
    {
      this.httpIn = httpIn;
    }

    // This is the only modified function, all other
    // methods are simple wrapper around the HTTP stream.
    @Override
    public final int available() throws IOException
    {
      int available = httpIn.available();
      return available == 0 ? 1 : available;
    }

    // ONLY WRAPPER METHODS FROM HERE ON.
    @Override
    public final int read() throws IOException
    {
      return httpIn.read();
    }

    @Override
    public final int read(byte b[]) throws IOException
    {
      return httpIn.read(b);
    }

    @Override
    public final int read(byte b[], int off, int len) throws IOException
    {
      return httpIn.read(b, off, len);
    }

    @Override
    public final long skip(long n) throws IOException
    {
      return httpIn.skip(n);
    }

    @Override
    public final void close() throws IOException
    {
      httpIn.close();
    }

    @Override
    public synchronized void mark(int readlimit)
    {
      httpIn.mark(readlimit);
    }

    @Override
    public synchronized void reset() throws IOException
    {
      httpIn.reset();
    }

    @Override
    public final boolean markSupported()
    {
      return httpIn.markSupported();
    }
  }

  private final static class SFConnectionSocketFactory
  extends PlainConnectionSocketFactory
  {
    @Override
    public Socket createSocket(HttpContext ctx) throws IOException
    {
      if (socksProxyDisabled)
      {
        return new Socket(Proxy.NO_PROXY);
      }
      return super.createSocket(ctx);
    }
  }


}
