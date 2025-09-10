package net.snowflake.client.core;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.Socket;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedTrustManager;
import net.snowflake.client.core.crl.CrlRevocationManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SFExtendedCrlTrustManagerDelegationTest {
  private X509ExtendedTrustManager mockTrustManager;
  private CrlRevocationManager mockRevocationManager;
  private X509Certificate[] certChain;
  private String authType;
  private Socket socket;
  private SSLEngine sslEngine;

  @BeforeEach
  void setUp() {
    mockTrustManager = mock(X509ExtendedTrustManager.class);
    mockRevocationManager = mock(CrlRevocationManager.class);
    certChain = new X509Certificate[] {mock(X509Certificate.class)};
    authType = "RSA";
    socket = mock(Socket.class);
    sslEngine = mock(SSLEngine.class);

    when(mockTrustManager.getAcceptedIssuers()).thenReturn(new X509Certificate[0]);
  }

  @Test
  void testCheckClientTrustedDelegatesToTrustManager() throws CertificateException {
    SFExtendedCrlTrustManager trustManager =
        new SFExtendedCrlTrustManager(mockRevocationManager, mockTrustManager);

    trustManager.checkClientTrusted(certChain, authType);

    verify(mockTrustManager).checkClientTrusted(certChain, authType);
  }

  @Test
  void testCheckClientTrustedWithSocketDelegatesToTrustManager() throws CertificateException {
    SFExtendedCrlTrustManager trustManager =
        new SFExtendedCrlTrustManager(mockRevocationManager, mockTrustManager);

    trustManager.checkClientTrusted(certChain, authType, socket);

    verify(mockTrustManager).checkClientTrusted(certChain, authType, socket);
  }

  @Test
  void testCheckClientTrustedWithSSLEngineDelegatesToTrustManager() throws CertificateException {
    SFExtendedCrlTrustManager trustManager =
        new SFExtendedCrlTrustManager(mockRevocationManager, mockTrustManager);

    trustManager.checkClientTrusted(certChain, authType, sslEngine);

    verify(mockTrustManager).checkClientTrusted(certChain, authType, sslEngine);
  }

  @Test
  void testCheckServerTrustedDelegatesToTrustManager() throws CertificateException {
    SFExtendedCrlTrustManager trustManager =
        new SFExtendedCrlTrustManager(mockRevocationManager, mockTrustManager);

    trustManager.checkServerTrusted(certChain, authType);

    verify(mockTrustManager).checkServerTrusted(certChain, authType);
    verify(mockRevocationManager).validateRevocationStatus(certChain, authType);
  }

  @Test
  void testCheckServerTrustedWithSocketDelegatesToTrustManager() throws CertificateException {
    SFExtendedCrlTrustManager trustManager =
        new SFExtendedCrlTrustManager(mockRevocationManager, mockTrustManager);

    trustManager.checkServerTrusted(certChain, authType, socket);

    verify(mockTrustManager).checkServerTrusted(certChain, authType, socket);
    verify(mockRevocationManager).validateRevocationStatus(certChain, authType);
  }

  @Test
  void testCheckServerTrustedWithSSLEngineDelegatesToTrustManager() throws CertificateException {
    SFExtendedCrlTrustManager trustManager =
        new SFExtendedCrlTrustManager(mockRevocationManager, mockTrustManager);

    trustManager.checkServerTrusted(certChain, authType, sslEngine);

    verify(mockTrustManager).checkServerTrusted(certChain, authType, sslEngine);
    verify(mockRevocationManager).validateRevocationStatus(certChain, authType);
  }

  @Test
  void testGetAcceptedIssuersDelegatesToTrustManager() {
    SFExtendedCrlTrustManager trustManager =
        new SFExtendedCrlTrustManager(mockRevocationManager, mockTrustManager);

    trustManager.getAcceptedIssuers();

    verify(mockTrustManager).getAcceptedIssuers();
  }
}
