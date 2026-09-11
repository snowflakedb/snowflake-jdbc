package net.snowflake.client.internal.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.Socket;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Verifies that {@code jdk.tls.client.protocols} governs the driver's own connections, with no
 * connection property set. This is the mechanism the Top Secret deployment relies on, and the only
 * one that also reaches PUT/GET stage transfers.
 *
 * <p>Requires {@code -Djdk.tls.client.protocols=TLSv1.3} at JVM launch, supplied by the {@code
 * arcReactorTests} Maven profile. It cannot be set from within the test: the value is read at the
 * first JSSE use and cached thereafter.
 */
@Tag(TestTags.ARC_REACTOR)
public class ArcReactorTlsEnforcementTest {

  @BeforeEach
  public void requireJvmFlag() {
    String configured = System.getProperty(SFSSLConnectionSocketFactory.JDK_TLS_CLIENT_PROTOCOLS);
    assertNotNull(
        configured,
        "This test requires -Djdk.tls.client.protocols=TLSv1.3 at JVM launch. Run it via"
            + " 'mvn test -DarcReactorTests', not as part of the default unit run.");
    assertEquals("TLSv1.3", configured.trim());
  }

  /**
   * No connection property is set, so before this change the driver would have offered its default
   * TLS 1.2-1.3 range and happily negotiated 1.2 -- ignoring the JVM flag entirely.
   */
  @Test
  public void shouldRefuseOlderProtocolFromJvmFlagAloneWithNoConnectionProperty() throws Exception {
    try (LocalTlsServer server = new LocalTlsServer("TLSv1.2")) {
      SSLException thrown =
          assertThrows(SSLException.class, () -> handshakeWithDriverDefaults(server));

      assertTrue(
          isProtocolFailure(thrown),
          "expected a TLS protocol failure but got: " + describe(thrown));
      assertEquals(LocalTlsServer.HANDSHAKE_FAILED, server.awaitNegotiatedProtocol());
    }
  }

  /** A TLS 1.3 server is still reachable, so the flag restricts rather than breaks connectivity. */
  @Test
  public void shouldStillNegotiateTls13() throws Exception {
    try (LocalTlsServer server = new LocalTlsServer("TLSv1.3")) {
      handshakeWithDriverDefaults(server);

      assertEquals("TLSv1.3", server.awaitNegotiatedProtocol());
    }
  }

  /** Uses the driver's default constructor: no per-connection TLS configuration whatsoever. */
  private static void handshakeWithDriverDefaults(LocalTlsServer server) throws Exception {
    SFSSLConnectionSocketFactory factory = new SFSSLConnectionSocketFactory(trustAll(), false);
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
}
