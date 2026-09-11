package net.snowflake.client.internal.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The load-bearing assertion for stage-transfer TLS: {@code jdk.tls.client.protocols} reaches the
 * cloud storage SDK, which exposes no TLS version API of its own.
 *
 * <p>Paired with {@code StageTransferTlsTest}, which shows the same stack happily negotiating TLS
 * 1.2 without the flag. Together they establish that stage transfers are governed by the JVM
 * property and never by the deprecated per-connection TLS properties -- the reason those were
 * deprecated.
 *
 * <p>Requires {@code -Djdk.tls.client.protocols=TLSv1.3} at JVM launch (the {@code arcReactorTests}
 * profile); the value is read at the first JSSE use and cached, so a test cannot set it.
 */
@Tag(TestTags.ARC_REACTOR)
public class ArcReactorStageTransferTlsTest {

  @BeforeEach
  public void requireJvmFlag() {
    String configured = System.getProperty(SFSSLConnectionSocketFactory.JDK_TLS_CLIENT_PROTOCOLS);
    assertNotNull(
        configured,
        "This test requires -Djdk.tls.client.protocols=TLSv1.3 at JVM launch. Run it via"
            + " 'mvn test -DarcReactorTests', not as part of the default unit run.");
    assertEquals("TLSv1.3", configured.trim());
  }

  @Test
  public void shouldRefuseTls12StageEndpoint() throws Exception {
    try (LocalTlsServer stage = new LocalTlsServer("TLSv1.2")) {
      StageSdkTlsProbe.attemptRequest(stage.getHost(), stage.getPort());

      assertEquals(
          LocalTlsServer.HANDSHAKE_FAILED,
          stage.awaitNegotiatedProtocol(),
          "the JVM flag should have prevented the storage SDK from negotiating TLS 1.2");
    }
  }

  /**
   * A TLS 1.3 stage endpoint stays reachable, so the flag restricts rather than breaks transfers.
   */
  @Test
  public void shouldStillReachTls13StageEndpoint() throws Exception {
    try (LocalTlsServer stage = new LocalTlsServer("TLSv1.3")) {
      StageSdkTlsProbe.attemptRequest(stage.getHost(), stage.getPort());

      assertEquals("TLSv1.3", stage.awaitNegotiatedProtocol());
    }
  }
}
