package net.snowflake.client.core;

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedTrustManager;

public class SFExtendedTrustManager extends X509ExtendedTrustManager {
  private final SFTrustManager sfTrustManager;
  private final X509ExtendedTrustManager exTrustManager;

  public SFExtendedTrustManager(
      SFTrustManager sfTrustManager, X509ExtendedTrustManager exTrustManager) {
    this.sfTrustManager = sfTrustManager;
    this.exTrustManager = exTrustManager;
  }

  @Override
  public void checkClientTrusted(X509Certificate[] chain, String authType, java.net.Socket socket)
      throws CertificateException {
    this.exTrustManager.checkClientTrusted(chain, authType, socket);
  }

  @Override
  public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine sslEngine)
      throws CertificateException {
    exTrustManager.checkClientTrusted(chain, authType, sslEngine);
  }

  @Override
  public void checkServerTrusted(X509Certificate[] chain, String authType, java.net.Socket socket)
      throws CertificateException {
    exTrustManager.checkServerTrusted(chain, authType, socket);
    String host = socket.getInetAddress().getHostName();
    sfTrustManager.validateRevocationStatus(chain, host);
  }

  @Override
  public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine sslEngine)
      throws CertificateException {
    // default behavior
    exTrustManager.checkServerTrusted(chain, authType, sslEngine);
    sfTrustManager.validateRevocationStatus(chain, sslEngine.getPeerHost());
  }

  @Override
  public void checkClientTrusted(X509Certificate[] chain, String authType)
      throws CertificateException {
    sfTrustManager.checkClientTrusted(chain, authType);
  }

  @Override
  public void checkServerTrusted(X509Certificate[] chain, String authType)
      throws CertificateException {
    sfTrustManager.checkServerTrusted(chain, authType);
    sfTrustManager.validateRevocationStatus(chain, authType);
  }

  @Override
  public X509Certificate[] getAcceptedIssuers() {
    return sfTrustManager.getAcceptedIssuers();
  }
}
