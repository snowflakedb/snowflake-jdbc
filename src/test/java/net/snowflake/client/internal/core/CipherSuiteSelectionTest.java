package net.snowflake.client.internal.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.net.Socket;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Cipher suite selection observed on a real handshake, since what matters is the suite actually
 * negotiated rather than the list the driver computed.
 */
public class CipherSuiteSelectionTest {

  private String original;

  @BeforeEach
  public void remember() {
    original = System.getProperty(SFSSLConnectionSocketFactory.HTTPS_CIPHER_SUITES);
  }

  @AfterEach
  public void restore() {
    if (original == null) {
      System.clearProperty(SFSSLConnectionSocketFactory.HTTPS_CIPHER_SUITES);
    } else {
      System.setProperty(SFSSLConnectionSocketFactory.HTTPS_CIPHER_SUITES, original);
    }
  }

  /**
   * Discriminating: the JDK would otherwise pick TLS_AES_128_GCM_SHA256, so negotiating AES_256 can
   * only be the configured list taking effect.
   */
  @Test
  public void shouldPinNegotiatedSuiteFromHttpsCipherSuites() throws Exception {
    assumeTrue(isTls13Available());
    System.setProperty(SFSSLConnectionSocketFactory.HTTPS_CIPHER_SUITES, "TLS_AES_256_GCM_SHA384");

    try (LocalTlsServer server = new LocalTlsServer("TLSv1.3")) {
      handshake(server);

      assertEquals("TLSv1.3", server.awaitNegotiatedProtocol());
      assertEquals("TLS_AES_256_GCM_SHA384", server.awaitNegotiatedCipher());
    }
  }

  /** With nothing configured, selection is left to JSSE and a TLS 1.3 suite is still negotiated. */
  @Test
  public void shouldNegotiateJsseDefaultWhenUnset() throws Exception {
    assumeTrue(isTls13Available());
    System.clearProperty(SFSSLConnectionSocketFactory.HTTPS_CIPHER_SUITES);

    try (LocalTlsServer server = new LocalTlsServer("TLSv1.3")) {
      handshake(server);

      assertEquals("TLSv1.3", server.awaitNegotiatedProtocol());
      String cipher = server.awaitNegotiatedCipher();
      assertNotNull(cipher);
      assertTrue(cipher.startsWith("TLS_AES") || cipher.startsWith("TLS_CHACHA"), cipher);
    }
  }

  private static void handshake(LocalTlsServer server) throws Exception {
    SFSSLConnectionSocketFactory factory =
        new SFSSLConnectionSocketFactory(trustAll(), false, TlsVersion.TLS_1_3, TlsVersion.TLS_1_3);
    try (Socket plain = new Socket(server.getHost(), server.getPort());
        Socket layered = factory.createLayeredSocket(plain, "localhost", server.getPort(), null)) {
      ((SSLSocket) layered).startHandshake();
    }
  }

  private static TrustManager[] trustAll() {
    return new TrustManager[] {
      new X509TrustManager() {
        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType) {}

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType) {}

        @Override
        public X509Certificate[] getAcceptedIssuers() {
          return new X509Certificate[0];
        }
      }
    };
  }

  private static boolean isTls13Available() {
    try {
      javax.net.ssl.SSLContext.getInstance("TLSv1.3");
      return true;
    } catch (Exception e) {
      return false;
    }
  }
}
