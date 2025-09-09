package net.snowflake.client.core;


import java.net.Socket;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertPathBuilderException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.List;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.net.ssl.X509TrustManager;
import net.snowflake.client.core.crl.CRLCacheConfig;
import net.snowflake.client.core.crl.CRLCacheManager;
import net.snowflake.client.core.crl.CRLValidator;
import net.snowflake.client.core.crl.VerifiedCertPathBuilder;
import net.snowflake.client.jdbc.SnowflakeSQLLoggedException;
import net.snowflake.client.jdbc.telemetry.BufferedTelemetryClient;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.ssl.SSLInitializationException;

public class SFCrlTrustManager extends X509ExtendedTrustManager {
  private static final SFLogger logger = SFLoggerFactory.getLogger(SFCrlTrustManager.class);
  private static final CRLCacheManager crlCacheManager;

  private final X509TrustManager trustManager;
  /** The default JVM Extended Trust Manager */
  private final X509ExtendedTrustManager exTrustManager;

  private final VerifiedCertPathBuilder certPathBuilder;
  private final CRLValidator crlValidator;

  static {
    try {
      crlCacheManager =
          CRLCacheManager.build(
              CRLCacheConfig.getInMemoryCacheEnabled(),
              CRLCacheConfig.getOnDiskCacheEnabled(),
              CRLCacheConfig.getOnDiskCacheDir(),
              CRLCacheConfig.getOnDiskCacheRemovalDelay(),
              CRLCacheConfig.getCacheValidityTime());
    } catch (SnowflakeSQLLoggedException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  public SFCrlTrustManager(HttpClientSettingsKey key) throws CertificateException {
    this.trustManager = getTrustManager(TrustManagerFactory.getDefaultAlgorithm());

    if (trustManager instanceof X509ExtendedTrustManager) {
      this.exTrustManager = (X509ExtendedTrustManager) trustManager;
    } else {
      logger.debug("Standard X509TrustManager is used instead of X509ExtendedTrustManager.");
      this.exTrustManager = null;
    }
    CloseableHttpClient httpClient = HttpUtil.getHttpClientForCrl(key);
    this.certPathBuilder = new VerifiedCertPathBuilder(this.trustManager);
    this.crlValidator =
        new CRLValidator(
            key.getRevocationCheckMode(),
            key.isAllowCertificatesWithoutCrlUrl(),
            httpClient,
            crlCacheManager,
            new BufferedTelemetryClient());
    CRLValidator.registerValidator(key, this.crlValidator);
  }

  @Override
  public void checkClientTrusted(X509Certificate[] chain, String authType)
      throws CertificateException {
    // default behavior
    trustManager.checkClientTrusted(chain, authType);
  }

  @Override
  public void checkServerTrusted(X509Certificate[] chain, String authType)
      throws CertificateException {
    trustManager.checkServerTrusted(chain, authType);
  }

  @Override
  public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket)
      throws CertificateException {
    if (exTrustManager != null) {
      exTrustManager.checkClientTrusted(chain, authType, socket);
    } else {
      trustManager.checkClientTrusted(chain, authType);
    }
  }

  @Override
  public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine sslEngine)
      throws CertificateException {
    if (exTrustManager != null) {
      exTrustManager.checkClientTrusted(chain, authType, sslEngine);
    } else {
      trustManager.checkClientTrusted(chain, authType);
    }
  }

  @Override
  public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket)
      throws CertificateException {
    if (exTrustManager != null) {
      exTrustManager.checkServerTrusted(chain, authType, socket);
    } else {
      trustManager.checkServerTrusted(chain, authType);
    }
    this.validateRevocationStatus(chain, authType);
  }

  @Override
  public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine sslEngine)
      throws CertificateException {
    if (exTrustManager != null) {
      exTrustManager.checkServerTrusted(chain, authType, sslEngine);
    } else {
      trustManager.checkServerTrusted(chain, authType);
    }
    this.validateRevocationStatus(chain, authType);
  }

  @Override
  public X509Certificate[] getAcceptedIssuers() {
    return trustManager.getAcceptedIssuers();
  }

  private void validateRevocationStatus(X509Certificate[] chain, String authType)
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

  private X509TrustManager getTrustManager(String algorithm) {
    try {
      TrustManagerFactory factory = TrustManagerFactory.getInstance(algorithm);
      factory.init((KeyStore) null);
      X509TrustManager ret = null;
      for (TrustManager tm : factory.getTrustManagers()) {
        if (tm instanceof X509TrustManager) {
          ret = (X509TrustManager) tm;
          break;
        }
      }
      return ret;
    } catch (NoSuchAlgorithmException | KeyStoreException ex) {
      throw new SSLInitializationException(ex.getMessage(), ex);
    }
  }
}
