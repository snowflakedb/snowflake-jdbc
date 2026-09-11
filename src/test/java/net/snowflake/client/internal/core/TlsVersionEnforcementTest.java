package net.snowflake.client.internal.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.net.Socket;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import org.junit.jupiter.api.Test;

/**
 * End-to-end TLS version enforcement against a loopback server pinned to TLS 1.2, exercising the
 * deprecated per-connection properties. The equivalent assertions for the {@code
 * jdk.tls.client.protocols} system property live in {@code ArcReactorTlsEnforcementTest}, which
 * needs that flag at JVM launch.
 */
public class TlsVersionEnforcementTest {

  @Test
  public void shouldFailHandshakeWhenServerOnlyOffersOlderProtocol() throws Exception {
    assumeTrue(isTls13Available());

    try (LocalTlsServer server = new LocalTlsServer("TLSv1.2")) {
      SSLException thrown =
          assertThrows(
              SSLException.class, () -> handshake(server, TlsVersion.TLS_1_3, TlsVersion.TLS_1_3));

      // must be a protocol/handshake failure, not a timeout or a generic connection error
      assertTrue(
          isProtocolFailure(thrown),
          "expected a TLS protocol failure but got: " + describe(thrown));
      assertEquals(LocalTlsServer.HANDSHAKE_FAILED, server.awaitNegotiatedProtocol());
    }
  }

  /**
   * Control: the same server and the same code path succeed at TLS 1.2, proving the failure above
   * is caused by the configured floor rather than by the server or the harness.
   */
  @Test
  public void shouldNegotiateTls12WhenAllowed() throws Exception {
    try (LocalTlsServer server = new LocalTlsServer("TLSv1.2")) {
      handshake(server, TlsVersion.TLS_1_2, TlsVersion.TLS_1_3);

      assertEquals("TLSv1.2", server.awaitNegotiatedProtocol());
    }
  }

  /** Drives the real socket factory so the assertion covers production wiring. */
  private static void handshake(LocalTlsServer server, TlsVersion min, TlsVersion max)
      throws Exception {
    SFSSLConnectionSocketFactory factory =
        new SFSSLConnectionSocketFactory(trustAll(), false, min, max);
    try (Socket plain = new Socket(server.getHost(), server.getPort());
        Socket layered = factory.createLayeredSocket(plain, "localhost", server.getPort(), null)) {
      ((SSLSocket) layered).startHandshake();
    }
  }

  private static boolean isProtocolFailure(Throwable thrown) {
    for (Throwable t = thrown; t != null; t = t.getCause()) {
      String message = t.getMessage();
      if (message != null) {
        String lower = message.toLowerCase();
        if (lower.contains("protocol")
            || lower.contains("handshake_failure")
            || lower.contains("no appropriate")
            || lower.contains("received fatal alert")) {
          return true;
        }
      }
    }
    return false;
  }

  private static String describe(Throwable thrown) {
    StringBuilder sb = new StringBuilder();
    for (Throwable t = thrown; t != null; t = t.getCause()) {
      sb.append(t.getClass().getSimpleName()).append(": ").append(t.getMessage()).append(" | ");
    }
    return sb.toString();
  }

  /** Isolates the assertion to protocol negotiation rather than trust configuration. */
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
      SSLContext.getInstance("TLSv1.3");
      return true;
    } catch (Exception e) {
      return false;
    }
  }
}
