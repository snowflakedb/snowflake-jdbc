package net.snowflake.client.jdbc;

import java.security.InvalidAlgorithmParameterException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import javax.net.ssl.ManagerFactoryParameters;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactorySpi;
import javax.net.ssl.X509TrustManager;

public class TestTrustManagerSpi extends TrustManagerFactorySpi {

  @Override
  protected void engineInit(KeyStore ks) throws KeyStoreException {}

  @Override
  protected void engineInit(ManagerFactoryParameters spec)
      throws InvalidAlgorithmParameterException {}

  @Override
  protected TrustManager[] engineGetTrustManagers() {
    return new TrustManager[] {
      new X509TrustManager() {
        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType)
            throws CertificateException {}

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType)
            throws CertificateException {}

        @Override
        public X509Certificate[] getAcceptedIssuers() {
          return new X509Certificate[0];
        }
      }
    };
  }
}
