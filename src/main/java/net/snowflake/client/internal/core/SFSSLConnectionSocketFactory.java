package net.snowflake.client.internal.core;

import static net.snowflake.client.internal.jdbc.SnowflakeUtil.systemGetProperty;

import java.io.IOException;
import java.net.Proxy;
import java.net.Socket;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.EnumSet;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.TrustManager;
import net.snowflake.client.internal.log.ArgSupplier;
import net.snowflake.client.internal.log.SFLogger;
import net.snowflake.client.internal.log.SFLoggerFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.protocol.HttpContext;

/** Snowflake custom SSLConnectionSocketFactory */
public class SFSSLConnectionSocketFactory extends SSLConnectionSocketFactory {
  private static final SFLogger logger =
      SFLoggerFactory.getLogger(SFSSLConnectionSocketFactory.class);

  /**
   * Standard JSSE system property listing the protocols enabled by default for client connections.
   * When set it governs every JSSE client in the JVM, including the cloud storage SDKs used for
   * PUT/GET, so it takes precedence over the deprecated per-connection TLS version properties.
   */
  static final String JDK_TLS_CLIENT_PROTOCOLS = "jdk.tls.client.protocols";

  private final boolean socksProxyDisabled;

  public SFSSLConnectionSocketFactory(TrustManager[] trustManagers, boolean socksProxyDisabled)
      throws NoSuchAlgorithmException, KeyManagementException {
    this(trustManagers, socksProxyDisabled, TlsVersion.DEFAULT_MIN, TlsVersion.DEFAULT_MAX);
  }

  public SFSSLConnectionSocketFactory(
      TrustManager[] trustManagers,
      boolean socksProxyDisabled,
      TlsVersion minTlsVersion,
      TlsVersion maxTlsVersion)
      throws NoSuchAlgorithmException, KeyManagementException {
    super(
        initSSLContext(trustManagers),
        resolveEnabledProtocols(minTlsVersion, maxTlsVersion),
        decideCipherSuites(),
        SSLConnectionSocketFactory.getDefaultHostnameVerifier());
    this.socksProxyDisabled = socksProxyDisabled;
  }

  /**
   * Decide which TLS versions this socket factory offers.
   *
   * <p>When {@value #JDK_TLS_CLIENT_PROTOCOLS} is set it takes precedence over the deprecated
   * per-connection properties. The versions it names are still intersected with the range this
   * driver models, so the property can tighten the handshake but cannot lower the driver's TLS 1.2
   * floor. Deferring to the JSSE defaults instead would let an externally configured {@code TLSv1}
   * or {@code TLSv1.1} be offered to Snowflake, since Apache only strips {@code SSL*} names of its
   * own accord.
   *
   * @return protocol names to enable, never null or empty
   * @throws IllegalStateException if no supported version remains
   */
  static String[] resolveEnabledProtocols(TlsVersion minTlsVersion, TlsVersion maxTlsVersion) {
    TlsVersion min = minTlsVersion;
    TlsVersion max = maxTlsVersion;

    String jdkClientProtocols = configuredJdkClientProtocols();
    if (jdkClientProtocols != null) {
      EnumSet<TlsVersion> fromJvm = TlsVersion.parseProtocolList(jdkClientProtocols);
      if (fromJvm.isEmpty()) {
        throw new IllegalStateException(
            String.format(
                "System property %s is set to '%s', which names no TLS version supported by this"
                    + " driver (%s)",
                JDK_TLS_CLIENT_PROTOCOLS, jdkClientProtocols, TlsVersion.supportedValues()));
      }
      // EnumSet iterates in declaration order, so the first is the oldest and the last the newest
      min = fromJvm.iterator().next();
      for (TlsVersion version : fromJvm) {
        max = version;
      }
      logger.debug(
          "System property {} is set to '{}'; offering {} to {}",
          JDK_TLS_CLIENT_PROTOCOLS,
          jdkClientProtocols,
          min.getProtocolName(),
          max.getProtocolName());
    }

    String[] protocols = TlsVersion.protocolsInRange(min, max);
    logger.debug("TLS versions offered: {}", (ArgSupplier) () -> Arrays.toString(protocols));
    return protocols;
  }

  /**
   * @return the value of {@value #JDK_TLS_CLIENT_PROTOCOLS}, or null when unset or blank. Shared so
   *     that the precedence decision here and the deprecation warnings in {@code SFBaseSession}
   *     cannot disagree about whether the property counts as configured.
   */
  static String configuredJdkClientProtocols() {
    String value = systemGetProperty(JDK_TLS_CLIENT_PROTOCOLS);
    return value == null || value.trim().isEmpty() ? null : value;
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
