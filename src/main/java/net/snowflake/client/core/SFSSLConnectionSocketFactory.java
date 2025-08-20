package net.snowflake.client.core;

import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;

import java.io.IOException;
import java.net.Proxy;
import java.net.Socket;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
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
  private static TlsVersion minTlsVersion = TlsVersion.TLS_1_2;
  private static TlsVersion maxTlsVersion = TlsVersion.TLS_1_3;
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
    if (minTlsVersion.compareTo(maxTlsVersion) > 0) {
      throw new IllegalArgumentException(
          String.format(
              "Minimum TLS version %s cannot be greater than the maximum TLS version %s",
              minTlsVersion.getProtocolName(), maxTlsVersion.getProtocolName()));
    }
    List<String> supported =
        Arrays.stream(TlsVersion.values())
            .filter(TlsVersion::isAvailable)
            .filter(v -> v.compareTo(minTlsVersion) >= 0)
            .filter(v -> v.compareTo(maxTlsVersion) <= 0)
            .map(TlsVersion::getProtocolName)
            .collect(Collectors.toList());

    if (supported.isEmpty()) {
      throw new IllegalStateException(
          String.format(
              "No TLS versions match constraints: min=%s, max=%s",
              minTlsVersion.getProtocolName(), maxTlsVersion.getProtocolName()));
    }
    return supported.toArray(new String[0]);
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

  public static void setMinTlsVersion(String minTlsVersion) {
    logger.debug("Setting minimum TLS version to: {}", minTlsVersion);
    SFSSLConnectionSocketFactory.minTlsVersion = TlsVersion.fromString(minTlsVersion);
  }

  public static void setMaxTlsVersion(String maxTlsVersion) {
    logger.debug("Setting maximum TLS version to: {}", maxTlsVersion);
    SFSSLConnectionSocketFactory.maxTlsVersion = TlsVersion.fromString(maxTlsVersion);
  }

  private enum TlsVersion {
    TLS_1_2("TLSv1.2"),
    TLS_1_3("TLSv1.3");

    private final String protocolName;

    TlsVersion(String protocolName) {
      this.protocolName = protocolName;
    }

    String getProtocolName() {
      return protocolName;
    }

    boolean isAvailable() {
      try {
        SSLContext.getInstance(this.protocolName);
        return true;
      } catch (NoSuchAlgorithmException e) {
        logger.debug("TLS protocol {} is not available", this.protocolName);
        return false;
      }
    }

    static TlsVersion fromString(String text) {
      if (text == null) {
        return null;
      }
      for (TlsVersion v : TlsVersion.values()) {
        if (v.protocolName.equalsIgnoreCase(text)) {
          return v;
        }
      }
      throw new IllegalArgumentException("Unsupported TLS version: " + text);
    }
  }
}
