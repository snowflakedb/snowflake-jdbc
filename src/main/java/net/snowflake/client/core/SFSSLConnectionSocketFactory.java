package net.snowflake.client.core;

import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;

import java.io.IOException;
import java.net.Proxy;
import java.net.Socket;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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

  private final boolean socksProxyDisabled;

  public SFSSLConnectionSocketFactory(TrustManager[] trustManagers, boolean socksProxyDisabled)
      throws NoSuchAlgorithmException, KeyManagementException {
    super(
        initSSLContext(trustManagers),
        getSupportedTlsVersions(),
        decideCipherSuites(),
        SSLConnectionSocketFactory.getDefaultHostnameVerifier());
    this.socksProxyDisabled = socksProxyDisabled;
  }

  private static String[] getSupportedTlsVersions() {
    List<String> supportedVersions = new ArrayList<>();

    // Check if TLS 1.3 is available and add it first (preferred)
    if (isTls13Available()) {
      supportedVersions.add("TLSv1.3");
      logger.debug("TLS versions available: [TLSv1.3, TLSv1.2] (TLS 1.3 preferred)");
    } else {
      logger.debug("TLS versions available: [TLSv1.2] (TLS 1.3 not supported)");
    }

    // Always include TLS 1.2 as baseline
    supportedVersions.add("TLSv1.2");

    return supportedVersions.toArray(new String[0]);
  }

  private static boolean isTls13Available() {
    try {
      SSLContext.getInstance("TLSv1.3");
      return true;
    } catch (NoSuchAlgorithmException e) {
      return false;
    }
  }

  private static SSLContext initSSLContext(TrustManager[] trustManagers)
      throws NoSuchAlgorithmException, KeyManagementException {
    // Use generic TLS context to support multiple versions
    SSLContext sslContext = SSLContext.getInstance("TLS");
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
