/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.RestRequest;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.SnowflakeUtil;
import net.snowflake.common.core.SqlState;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContexts;
import org.apache.http.cookie.CookieSpec;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.concurrent.atomic.AtomicBoolean;

import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

import javax.net.ssl.SSLContext;

import static org.apache.http.client.config.CookieSpecs.DEFAULT;
import static org.apache.http.client.config.CookieSpecs.IGNORE_COOKIES;

/**
 * Created by jhuang on 1/19/16.
 */
public class HttpUtil
{
  static final SFLogger logger = SFLoggerFactory.getLogger(HttpUtil.class);

  public static final int DEFAULT_MAX_CONNECTIONS = 100;
  public static final int DEFAULT_MAX_CONNECTIONS_PER_ROUTE = 100;
  public static final int DEFAULT_CONNECTION_TIMEOUT = 60000;
  public static final int DEFAULT_HTTP_CLIENT_SOCKET_TIMEOUT = 300000; // ms


  /**
   * The unique httpClient shared by all connections, this will benefit long
   * lived clients
   */
  private static HttpClient httpClient = null;

  /** Handle on the static connection manager, to gather statistics mainly */
  private static PoolingHttpClientConnectionManager connectionManager = null;

  /** default request configuration, to be copied on individual requests. */
  private static RequestConfig DefaultRequestConfig = null;

  /**
   * Build an Http client using our set of default.
   *
   * @return HttpClient object
   */
  private static HttpClient buildHttpClient()
  {
    // set timeout so that we don't wait forever.
    // Setup the default configuration for all requests on this client
    DefaultRequestConfig =
        RequestConfig.custom()
                     .setConnectTimeout(DEFAULT_CONNECTION_TIMEOUT)
                     .setConnectionRequestTimeout(DEFAULT_CONNECTION_TIMEOUT)
                     .setSocketTimeout(DEFAULT_HTTP_CLIENT_SOCKET_TIMEOUT)
                     .build();

    // enforce using tlsv1.2
    SSLContext sslContext = SSLContexts.createDefault();

    SSLConnectionSocketFactory sslSocketFactory = new SSLConnectionSocketFactory(
        sslContext,
        new String[] {"TLSv1.2"},
        null,
        SSLConnectionSocketFactory.STRICT_HOSTNAME_VERIFIER);

    Registry<ConnectionSocketFactory> registry =
        RegistryBuilder.<ConnectionSocketFactory>create()
                       .register("https", sslSocketFactory)
                       .register("http",
                           PlainConnectionSocketFactory.getSocketFactory())
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
                         .build();

    return httpClient;
  }

  /**
   * Accessor for the HTTP client singleton.
   *
   * @return HttpClient object shared across all connections
   */
  public static HttpClient getHttpClient()
  {
    if (httpClient == null)
    {
      synchronized (HttpUtil.class)
      {
        if (httpClient == null)
        {
          httpClient = buildHttpClient();
        }
      }
    }
    return httpClient;
  }

  /**
   * Return a request configuration inheriting from the default request
   * configuration of the shared HttpClient with a different socket timeout.
   *
   * @param soTimeoutMs - custom socket timeout in milli-seconds
   * @param withoutCookies - whether this request should ignore cookies or not
   * @return RequestConfig object
   */
  public static final RequestConfig
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
  public static final RequestConfig getRequestConfigWithoutcookies()
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
  public static String getHttpClientStats()
  {
    return connectionManager == null ?
        "" :
        connectionManager.getTotalStats().toString();
  }

  /**
   * Executes a HTTP request with the cookie spec set to IGNORE_COOKIES
   * @param httpRequest
   * @param httpClient
   * @param retryTimeout
   * @param injectSocketTimeout
   * @param canceling
   * @return
   */
  static String executeRequestWithoutCookies(HttpRequestBase httpRequest,
                                             HttpClient httpClient,
                                             int retryTimeout,
                                             int injectSocketTimeout,
                                             AtomicBoolean canceling)
      throws SnowflakeSQLException, IOException
  {
    return executeRequestInternal(
        httpRequest,
        httpClient,
        retryTimeout,
        injectSocketTimeout,
        canceling,
        true);
  }

  /**
   * Executes a HTTP request for Snowflake.
   * @param httpRequest
   * @param httpClient
   * @param retryTimeout
   * @param injectSocketTimeout
   * @param canceling
   * @return
   * @throws SnowflakeSQLException
   * @throws IOException
   */
  static String executeRequest(HttpRequestBase httpRequest,
                               HttpClient httpClient,
                               int retryTimeout,
                               int injectSocketTimeout,
                               AtomicBoolean canceling)
      throws SnowflakeSQLException, IOException
  {
    return executeRequestInternal(
        httpRequest,
        httpClient,
        retryTimeout,
        injectSocketTimeout,
        canceling,
        false);
  }

  /**
   * Helper to execute a request with retry and check and throw exception if
   * response is not success.
   * This should be used only for small request has it execute the REST
   * request and get back the result as a string.
   *
   * Connection under the httpRequest is released.
   *
   * @param httpRequest request object contains all the information
   * @param httpClient client object used to communicate with other machine
   * @param retryTimeout retry timeout (in seconds)
   * @param injectSocketTimeout simulate socket timeout
   * @param canceling canceling flag
   * @param withoutCookies whether this request should ignore cookies
   * @return response in String
   * @throws net.snowflake.client.jdbc.SnowflakeSQLException
   * @throws java.io.IOException
   */
  private static String executeRequestInternal(HttpRequestBase httpRequest,
                                               HttpClient httpClient,
                                               int retryTimeout,
                                               int injectSocketTimeout,
                                               AtomicBoolean canceling,
                                               boolean withoutCookies)
      throws SnowflakeSQLException, IOException
  {
    if (logger.isDebugEnabled())
    {
      logger.debug("Pool: {} Executing: {}",
                     HttpUtil.getHttpClientStats(),
                     httpRequest);
    }

    String theString = null;
    StringWriter writer = null;
    try
    {
      HttpResponse response = RestRequest.execute(httpClient,
                                                  httpRequest,
                                                  retryTimeout,
                                                  injectSocketTimeout,
                                                  canceling,
                                                  withoutCookies);

      if (response == null ||
              response.getStatusLine().getStatusCode() != 200)
      {
        logger.error( "Error executing request: {}",
                   httpRequest.toString());

        SnowflakeUtil.logResponseDetails(response, logger);

        throw new SnowflakeSQLException(SqlState.IO_ERROR,
                                        ErrorCode.NETWORK_ERROR.getMessageCode(),
                                        "HTTP status="
                                            + ((response != null)?
                                     response.getStatusLine() .getStatusCode():
                                                   "null response"));
      }

      writer = new StringWriter();
      IOUtils.copy(response.getEntity().getContent(), writer, "UTF-8");
      theString = writer.toString();
    }
    finally
    {
      // Make sure the connection is released
      httpRequest.releaseConnection();
      if (writer != null)
      {
        writer.close();
      }
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
  };
}
