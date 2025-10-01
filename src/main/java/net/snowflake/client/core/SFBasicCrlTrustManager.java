package net.snowflake.client.core;

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import javax.net.ssl.X509TrustManager;
import net.snowflake.client.core.crl.CrlRevocationManager;

@SnowflakeJdbcInternalApi
public class SFBasicCrlTrustManager implements X509TrustManager {
  private final X509TrustManager trustManager;
  private final CrlRevocationManager revocationManager;

  public SFBasicCrlTrustManager(
      CrlRevocationManager revocationManager, X509TrustManager trustManager) {
    this.revocationManager = revocationManager;
    this.trustManager = trustManager;
  }

  @Override
  public void checkClientTrusted(X509Certificate[] chain, String authType)
      throws CertificateException {
    trustManager.checkClientTrusted(chain, authType);
  }

  @Override
  public void checkServerTrusted(X509Certificate[] chain, String authType)
      throws CertificateException {
    trustManager.checkServerTrusted(chain, authType);
    revocationManager.validateRevocationStatus(chain, authType);
  }

  @Override
  public X509Certificate[] getAcceptedIssuers() {
    return trustManager.getAcceptedIssuers();
  }
}
