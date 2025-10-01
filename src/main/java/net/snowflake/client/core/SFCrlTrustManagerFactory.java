package net.snowflake.client.core;

import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.net.ssl.X509TrustManager;
import net.snowflake.client.core.crl.CrlRevocationManager;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import org.apache.http.ssl.SSLInitializationException;

@SnowflakeJdbcInternalApi
class SFCrlTrustManagerFactory {
  private static final SFLogger logger = SFLoggerFactory.getLogger(SFCrlTrustManagerFactory.class);

  static X509TrustManager createCrlTrustManager(HttpClientSettingsKey key)
      throws CertificateException {
    X509TrustManager systemTrustManager = getSystemTrustManager();
    CrlRevocationManager revocationManager = new CrlRevocationManager(key, systemTrustManager);

    if (systemTrustManager instanceof X509ExtendedTrustManager) {
      logger.debug("JVM provides X509ExtendedTrustManager - creating extended CRL trust manager");
      return new SFExtendedCrlTrustManager(
          revocationManager, (X509ExtendedTrustManager) systemTrustManager);
    } else {
      logger.debug("JVM provides basic X509TrustManager - creating basic CRL trust manager");
      return new SFBasicCrlTrustManager(revocationManager, systemTrustManager);
    }
  }

  private static X509TrustManager getSystemTrustManager() {
    try {
      TrustManagerFactory factory =
          TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      factory.init((KeyStore) null);

      for (TrustManager tm : factory.getTrustManagers()) {
        if (tm instanceof X509TrustManager) {
          return (X509TrustManager) tm;
        }
      }

      throw new SSLInitializationException(
          "No X509TrustManager found in default trust managers", null);
    } catch (NoSuchAlgorithmException | KeyStoreException ex) {
      throw new SSLInitializationException("Failed to initialize default trust manager", ex);
    }
  }
}
