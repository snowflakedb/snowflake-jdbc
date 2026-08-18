package net.snowflake.client.internal.jdbc.diagnostic;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.Proxy;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import net.snowflake.client.internal.log.SFLogger;
import net.snowflake.client.internal.log.SFLoggerFactory;

class CertificateDiagnosticCheck extends DiagnosticCheck {

  private static final String SECURE_SOCKET_PROTOCOL = "TLS";

  private static final SFLogger logger =
      SFLoggerFactory.getLogger(CertificateDiagnosticCheck.class);

  public CertificateDiagnosticCheck(ProxyConfig proxyConfig) {
    super("SSL/TLS Certificate Test", proxyConfig);
  }

  /**
   * Returns a host that can be sent as a TLS server name (SNI).
   *
   * <p>The JDK only populates the TLS {@code server_name} extension for hosts that satisfy the
   * "Letter, Digit, Hyphen" rule of RFC 952 - {@link SNIHostName} rejects anything else and the
   * handshake then proceeds <em>silently</em> without SNI. A server fronting many names can respond
   * to such a handshake with a default certificate, which makes this check report on a certificate
   * that a real client would never be served.
   *
   * <p>Snowflake account names may contain underscores, and for this reason Snowflake also serves a
   * variant of the account name with each underscore replaced by a hyphen. Prefer that variant so
   * the probe sends SNI, and log loudly when no valid variant exists.
   *
   * @param hostname host as listed in the allowlist file
   * @return a host suitable for SNI, or the original host if no valid variant exists
   */
  static String toSniCompatibleHost(String hostname) {
    if (isValidSniHostName(hostname)) {
      return hostname;
    }
    String hyphenated = hostname.replace('_', '-');
    if (isValidSniHostName(hyphenated)) {
      logger.info(
          "Host {} cannot be sent as a TLS server name (SNI) because it does not comply with the"
              + " Letter-Digit-Hyphen rule of RFC 952. Using the hyphenated form {} instead, which"
              + " Snowflake also serves for account names containing underscores.",
          hostname,
          hyphenated);
      return hyphenated;
    }
    logger.warn(
        "Host {} cannot be sent as a TLS server name (SNI) because it does not comply with the"
            + " Letter-Digit-Hyphen rule of RFC 952, and no hyphenated variant is valid either."
            + " Connecting without SNI; the server may return a default certificate rather than"
            + " the one a real client would be served.",
        hostname);
    return hostname;
  }

  private static boolean isValidSniHostName(String hostname) {
    try {
      new SNIHostName(hostname);
      return true;
    } catch (IllegalArgumentException | NullPointerException e) {
      return false;
    }
  }

  @Override
  protected void doCheck(SnowflakeEndpoint snowflakeEndpoint) {
    String hostname = toSniCompatibleHost(snowflakeEndpoint.getHost());
    String port = Integer.toString(snowflakeEndpoint.getPort());
    if (snowflakeEndpoint.isSslEnabled()) {
      String urlString = "https://" + hostname + ":" + port;
      try {
        SSLContext sslContext = SSLContext.getInstance(SECURE_SOCKET_PROTOCOL);
        sslContext.init(null, new TrustManager[] {new DiagnosticTrustManager()}, null);
        HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.getSocketFactory());
        Proxy proxy = this.proxyConf.getProxy(snowflakeEndpoint);
        new URL(urlString).openConnection(proxy).connect();
      } catch (NoSuchAlgorithmException e) {
        logger.error(
            "None of the security provider's implementation of SSLContextSpi supports "
                + SECURE_SOCKET_PROTOCOL,
            e);
      } catch (KeyManagementException e) {
        logger.error("Failed to initialize SSLContext", e);
      } catch (MalformedURLException e) {
        logger.error("Failed to create new URL object: " + urlString, e);
      } catch (IOException e) {
        logger.error("Failed to open a connection to: " + urlString, e);
      } catch (Exception e) {
        logger.error(
            "Unexpected error occurred when trying to retrieve certificate from: " + hostname, e);
      } finally {
        HttpsURLConnection.setDefaultSSLSocketFactory(
            (SSLSocketFactory) SSLSocketFactory.getDefault());
      }
    } else {
      logger.info("Host " + hostname + ":" + port + " is not secure. Skipping certificate check.");
    }
  }
}
