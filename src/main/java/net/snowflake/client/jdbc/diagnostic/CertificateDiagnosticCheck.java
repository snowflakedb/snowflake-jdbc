package net.snowflake.client.jdbc.diagnostic;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.Proxy;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

class CertificateDiagnosticCheck extends DiagnosticCheck {

  private static final String SECURE_SOCKET_PROTOCOL = "TLS";

  private static final SFLogger logger =
      SFLoggerFactory.getLogger(CertificateDiagnosticCheck.class);

  public CertificateDiagnosticCheck(ProxyConfig proxyConfig) {
    super("SSL/TLS Certificate Test", proxyConfig);
  }

  @Override
  protected void doCheck(SnowflakeEndpoint snowflakeEndpoint) {
    String hostname = snowflakeEndpoint.getHost();
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
