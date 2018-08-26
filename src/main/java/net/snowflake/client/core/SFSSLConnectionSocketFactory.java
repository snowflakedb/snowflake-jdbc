/*
 * Copyright (c) 2012-2018 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.protocol.HttpContext;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.TrustManager;
import java.io.IOException;
import java.net.Proxy;
import java.net.Socket;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/**
 * Snowflake custom SSLConnectionSocketFactory
 */
public class SFSSLConnectionSocketFactory extends SSLConnectionSocketFactory
{
  static final SFLogger logger = SFLoggerFactory.getLogger(
      SFSSLConnectionSocketFactory.class);

  private static final String SSL_VERSION = "TLSv1.2";

  private final boolean socksProxyDisabled;

  public SFSSLConnectionSocketFactory(TrustManager[] trustManagers,
                                      boolean socksProxyDisabled)
      throws NoSuchAlgorithmException, KeyManagementException
  {
    super(
        initSSLContext(trustManagers),
        new String[]{SSL_VERSION},
        decideCipherSuites(),
        SSLConnectionSocketFactory.getDefaultHostnameVerifier()
    );
    this.socksProxyDisabled = socksProxyDisabled;
  }

  private static SSLContext initSSLContext(TrustManager[] trustManagers)
      throws NoSuchAlgorithmException, KeyManagementException
  {
    // enforce using SSL_VERSION
    SSLContext sslContext = SSLContext.getInstance(SSL_VERSION);
    sslContext.init(
        null, // key manager
        trustManagers, // trust manager
        null); // secure random
    return sslContext;
  }

  @Override
  public Socket createSocket(HttpContext ctx) throws IOException
  {
    return socksProxyDisabled ? new Socket(Proxy.NO_PROXY)
        : super.createSocket(ctx);
  }

  /**
   * Decide cipher suites that will be passed into the SSLConnectionSocketFactory
   *
   * @return List of cipher suites.
   */
  private static String[] decideCipherSuites()
  {
    String sysCipherSuites = System.getProperty("https.cipherSuites");

    String[] cipherSuites =  sysCipherSuites != null ? sysCipherSuites.split(",") :
        // use jdk default cipher suites
        ((SSLServerSocketFactory) SSLServerSocketFactory.getDefault())
            .getDefaultCipherSuites();

    // cipher suites need to be picked up in code explicitly for jdk 1.7
    // https://stackoverflow.com/questions/44378970/
    if (logger.isTraceEnabled())
    {
      logger.trace("Cipher suites used: {}", Arrays.toString(cipherSuites));
    }

    return cipherSuites;
  }

}
