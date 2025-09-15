package net.snowflake.client.core.crl;

import java.security.cert.CertPathBuilderException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.List;
import javax.net.ssl.X509TrustManager;
import net.snowflake.client.core.HttpClientSettingsKey;
import net.snowflake.client.core.HttpUtil;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.jdbc.SnowflakeSQLLoggedException;
import net.snowflake.client.jdbc.telemetry.PreSessionTelemetryClient;
import org.apache.http.impl.client.CloseableHttpClient;

@SnowflakeJdbcInternalApi
public class CrlRevocationManager {
  private static final CRLCacheManager crlCacheManager;
  private final VerifiedCertPathBuilder certPathBuilder;
  private final CRLValidator crlValidator;

  static {
    try {
      crlCacheManager =
          CRLCacheManager.build(
              CRLCacheConfig.getInMemoryCacheEnabled(),
              CRLCacheConfig.getOnDiskCacheEnabled(),
              CRLCacheConfig.getOnDiskCacheDir(),
              CRLCacheConfig.getCrlOnDiskCacheRemovalDelay(),
              CRLCacheConfig.getCacheValidityTime());
    } catch (SnowflakeSQLLoggedException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  public CrlRevocationManager(HttpClientSettingsKey key, X509TrustManager trustManager)
      throws CertificateException {
    CloseableHttpClient httpClient = HttpUtil.getHttpClientForCrl(key);
    this.certPathBuilder = new VerifiedCertPathBuilder(trustManager);
    this.crlValidator =
        new CRLValidator(
            key.getRevocationCheckMode(),
            key.isAllowCertificatesWithoutCrlUrl(),
            httpClient,
            crlCacheManager,
            new PreSessionTelemetryClient());
    CRLValidator.registerValidator(key, this.crlValidator);
  }

  public void validateRevocationStatus(X509Certificate[] chain, String authType)
      throws CertificateException {
    try {
      List<X509Certificate[]> certificates =
          this.certPathBuilder.buildAllVerifiedPaths(chain, authType);
      boolean validationResult = this.crlValidator.validateCertificateChains(certificates);
      if (!validationResult) {
        throw new CertificateException(
            "No not revoked certificate chains found during CRL revocation check or transient error happened and not in advisory mode");
      }
    } catch (CertPathBuilderException e) {
      throw new CertificateException("Certificate revocation check failed", e);
    }
  }
}
