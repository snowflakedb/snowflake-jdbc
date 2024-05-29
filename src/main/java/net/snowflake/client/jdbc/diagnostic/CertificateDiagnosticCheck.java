package net.snowflake.client.jdbc.diagnostic;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.Proxy;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import javax.net.ssl.*;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

public class CertificateDiagnosticCheck extends DiagnosticCheck {

  private static final String SECURE_SOCKET_PROTOCOL = "TLS";

  private static final SFLogger logger =
      SFLoggerFactory.getLogger(CertificateDiagnosticCheck.class);

  public CertificateDiagnosticCheck(ProxyConfig proxyConfig) {
    super("SSL/TLS Certificate Test", proxyConfig);
  }

  @Override
  public void run(SnowflakeEndpoint snowflakeEndpoint) {
    super.run(snowflakeEndpoint);
    if (snowflakeEndpoint.isSslEnabled()) {
      String hostname = snowflakeEndpoint.getHost();
      String urlString = "https://" + hostname + ":" + snowflakeEndpoint.getPort();
      try {
        SSLContext sslContext = SSLContext.getInstance(SECURE_SOCKET_PROTOCOL);
        sslContext.init(null, new TrustManager[] {new DiagnosticTrustManager()}, null);
        HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.getSocketFactory());
        Proxy proxy = this.proxyConf.getProxy(snowflakeEndpoint);
        new URL(urlString).openConnection(proxy).connect();
      } catch (NoSuchAlgorithmException e) {
        logger.error(
            "None of the security provider's implementation of SSLContextSpi supports the {} protocol",
            SECURE_SOCKET_PROTOCOL);
        logger.error(e.getLocalizedMessage(), e);
      } catch (KeyManagementException e) {
        logger.error("Failed to initialize SSLContext");
        logger.error(e.getLocalizedMessage(), e);
      } catch (MalformedURLException e) {
        logger.error("Failed to create new URL object: {}", urlString);
        logger.error(e.getLocalizedMessage(), e);
      } catch (IOException e) {
        logger.error("Failed to open a connection to: {}", urlString);
        logger.error(e.getLocalizedMessage(), e);
      } catch (Exception e) {
        logger.error(
            "Unknown error occurred when trying to retrieve certificate from: {}", hostname);
        logger.error(e.getLocalizedMessage(), e);
      } finally {
        HttpsURLConnection.setDefaultSSLSocketFactory(
            (SSLSocketFactory) SSLSocketFactory.getDefault());
      }
    } else {
      logger.debug(
          "Host "
              + snowflakeEndpoint.getHost()
              + ":"
              + snowflakeEndpoint.getPort()
              + " is not secure. Skipping certificate check.");
    }
  }
}
