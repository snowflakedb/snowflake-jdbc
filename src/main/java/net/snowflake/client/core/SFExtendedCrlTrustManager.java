package net.snowflake.client.core;

import java.net.Socket;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedTrustManager;
import net.snowflake.client.core.crl.CrlRevocationManager;

@SnowflakeJdbcInternalApi
public class SFExtendedCrlTrustManager extends X509ExtendedTrustManager {
  private final X509ExtendedTrustManager exTrustManager;
  private final CrlRevocationManager revocationManager;

  public SFExtendedCrlTrustManager(
      CrlRevocationManager revocationManager, X509ExtendedTrustManager trustManager) {
    this.revocationManager = revocationManager;
    this.exTrustManager = trustManager;
  }

  @Override
  public void checkClientTrusted(X509Certificate[] chain, String authType)
      throws CertificateException {
    exTrustManager.checkClientTrusted(chain, authType);
  }

  @Override
  public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket)
      throws CertificateException {
    exTrustManager.checkClientTrusted(chain, authType, socket);
  }

  @Override
  public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine)
      throws CertificateException {
    exTrustManager.checkClientTrusted(chain, authType, engine);
  }

  @Override
  public void checkServerTrusted(X509Certificate[] chain, String authType)
      throws CertificateException {
    exTrustManager.checkServerTrusted(chain, authType);
    revocationManager.validateRevocationStatus(chain, authType);
  }

  @Override
  public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine)
      throws CertificateException {
    exTrustManager.checkServerTrusted(chain, authType, engine);
    revocationManager.validateRevocationStatus(chain, authType);
  }

  @Override
  public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket)
      throws CertificateException {
    exTrustManager.checkServerTrusted(chain, authType, socket);
    revocationManager.validateRevocationStatus(chain, authType);
  }

  @Override
  public X509Certificate[] getAcceptedIssuers() {
    return exTrustManager.getAcceptedIssuers();
  }
}
