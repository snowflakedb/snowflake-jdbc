package net.snowflake.client.core;

import java.io.File;
import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.util.Map;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.net.ssl.X509TrustManager;
import org.apache.http.ssl.SSLInitializationException;
import org.bouncycastle.asn1.x509.Certificate;

@SnowflakeJdbcInternalApi
public class SFTrustManagerFactory {
  private static final Object ROOT_CA_LOCK = new Object();
  private static final Map<Integer, Certificate> ROOT_CA = new java.util.HashMap<>();
  private final TrustManagerFactory trustManagerFactory;

  public SFTrustManagerFactory(TrustManagerFactory trustManagerFactory) {
    this.trustManagerFactory = trustManagerFactory;
  }

  public X509TrustManager getTrustManager(HttpClientSettingsKey key, File ocspCacheFile)
      throws IOException {
    X509TrustManager x509TrustManager = initX509TrustManager(trustManagerFactory);
    SFTrustManager sfTrustManager =
        new SFTrustManager(key, ocspCacheFile, ROOT_CA, x509TrustManager);
    if (x509TrustManager instanceof X509ExtendedTrustManager) {
      return new SFExtendedTrustManager(
          sfTrustManager, (X509ExtendedTrustManager) x509TrustManager);
    } else {
      return sfTrustManager;
    }
  }

  public Map<Integer, Certificate> getRootCACertificates() {
    synchronized (ROOT_CA_LOCK) {
      return SFTrustManagerFactory.ROOT_CA;
    }
  }

  private static X509TrustManager initX509TrustManager(TrustManagerFactory trustManagerFactory) {
    try {
      trustManagerFactory.init((KeyStore) null);
      X509TrustManager ret = null;
      for (TrustManager tm : trustManagerFactory.getTrustManagers()) {
        // Multiple TrustManager may be attached. We just need X509 Trust
        // Manager here.
        if (tm instanceof X509TrustManager) {
          ret = (X509TrustManager) tm;
          break;
        }
      }
      if (ret == null) {
        return null;
      }
      synchronized (ROOT_CA_LOCK) {
        // cache root CA certificates for later use.
        if (ROOT_CA.isEmpty()) {
          for (X509Certificate cert : ret.getAcceptedIssuers()) {
            Certificate bcCert = Certificate.getInstance(cert.getEncoded());
            ROOT_CA.put(bcCert.getSubject().hashCode(), bcCert);
          }
        }
      }
      return ret;
    } catch (KeyStoreException | CertificateEncodingException ex) {
      throw new SSLInitializationException(ex.getMessage(), ex);
    }
  }
}
