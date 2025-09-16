package net.snowflake.client.core;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import javax.net.ssl.X509TrustManager;
import net.snowflake.client.core.crl.CrlRevocationManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SFCrlTrustManagerDelegationTest {
  private X509TrustManager mockTrustManager;
  private CrlRevocationManager mockRevocationManager;
  private X509Certificate[] certChain;
  private String authType;

  @BeforeEach
  void setUp() {
    mockTrustManager = mock(X509TrustManager.class);
    mockRevocationManager = mock(CrlRevocationManager.class);
    certChain = new X509Certificate[] {mock(X509Certificate.class)};
    authType = "RSA";

    when(mockTrustManager.getAcceptedIssuers()).thenReturn(new X509Certificate[0]);
  }

  @Test
  void testCheckClientTrustedDelegatesToTrustManager() throws CertificateException {
    SFBasicCrlTrustManager trustManager =
        new SFBasicCrlTrustManager(mockRevocationManager, mockTrustManager);

    trustManager.checkClientTrusted(certChain, authType);

    verify(mockTrustManager).checkClientTrusted(certChain, authType);
  }

  @Test
  void testCheckServerTrustedDelegatesToTrustManager() throws CertificateException {
    SFBasicCrlTrustManager trustManager =
        new SFBasicCrlTrustManager(mockRevocationManager, mockTrustManager);

    trustManager.checkServerTrusted(certChain, authType);

    verify(mockTrustManager).checkServerTrusted(certChain, authType);
    verify(mockRevocationManager).validateRevocationStatus(certChain, authType);
  }

  @Test
  void testGetAcceptedIssuersDelegatesToTrustManager() {
    SFBasicCrlTrustManager trustManager =
        new SFBasicCrlTrustManager(mockRevocationManager, mockTrustManager);

    trustManager.getAcceptedIssuers();

    verify(mockTrustManager).getAcceptedIssuers();
  }
}
