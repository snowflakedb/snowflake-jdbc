package net.snowflake.client.core;

import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;

import java.io.IOException;
import java.net.Proxy;
import java.net.Socket;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.TrustManager;
import net.snowflake.client.log.ArgSupplier;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.protocol.HttpContext;

/** Snowflake custom SSLConnectionSocketFactory */
public class SFSSLConnectionSocketFactory extends SSLConnectionSocketFactory {
  private static final SFLogger logger =
      SFLoggerFactory.getLogger(SFSSLConnectionSocketFactory.class);

  private static final String SSL_VERSION = "TLSv1.2";

  private final boolean socksProxyDisabled;

  public SFSSLConnectionSocketFactory(TrustManager[] trustManagers, boolean socksProxyDisabled)
      throws NoSuchAlgorithmException, KeyManagementException {
    super(
        initSSLContext(trustManagers),
        new String[] {SSL_VERSION},
        decideCipherSuites(),
        SSLConnectionSocketFactory.getDefaultHostnameVerifier());
    this.socksProxyDisabled = socksProxyDisabled;
  }

  private static SSLContext initSSLContext(TrustManager[] trustManagers)
      throws NoSuchAlgorithmException, KeyManagementException {
    // enforce using SSL_VERSION
    SSLContext sslContext = SSLContext.getInstance(SSL_VERSION);
    sslContext.init(
        null, // key manager
        trustManagers, // trust manager
        null); // secure random
    return sslContext;
  }

  @Override
  public Socket createSocket(HttpContext ctx) throws IOException {
    return socksProxyDisabled ? new Socket(Proxy.NO_PROXY) : super.createSocket(ctx);
  }

  /**
   * Decide cipher suites that will be passed into the SSLConnectionSocketFactory
   *
   * @return List of cipher suites.
   */
  private static String[] decideCipherSuites() {
    String sysCipherSuites = systemGetProperty("https.cipherSuites");

    String[] cipherSuites =
        sysCipherSuites != null
            ? sysCipherSuites.split(",")
            :
            // use jdk default cipher suites
            ((SSLServerSocketFactory) SSLServerSocketFactory.getDefault()).getDefaultCipherSuites();

    // cipher suites need to be picked up in code explicitly for jdk 1.7
    // https://stackoverflow.com/questions/44378970/
    logger.trace("Cipher suites used: {}", (ArgSupplier) () -> Arrays.toString(cipherSuites));

    return cipherSuites;
  }
}
