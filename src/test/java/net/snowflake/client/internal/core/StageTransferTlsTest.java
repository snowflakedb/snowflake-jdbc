package net.snowflake.client.internal.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.Test;

/**
 * Baseline for stage-transfer TLS: with no {@code jdk.tls.client.protocols} flag, the cloud storage
 * stack negotiates whatever the endpoint offers, including TLS 1.2.
 *
 * <p>{@code ArcReactorStageTransferTlsTest} asserts the other half -- that the flag constrains it.
 * Together they show that stage transfers are governed by the JVM property and not by the
 * deprecated per-connection TLS properties.
 */
public class StageTransferTlsTest {

  @Test
  public void shouldNegotiateTls12ForStageTransferWithoutJvmFlag() throws Exception {
    // the premise of this test is that the flag is absent; a hardened environment may supply it
    // externally (JAVA_TOOL_OPTIONS, _JAVA_OPTIONS, a site java.security), in which case the
    // enforcement assertion in ArcReactorStageTransferTlsTest is the relevant one
    assumeTrue(SFSSLConnectionSocketFactory.configuredJdkClientProtocols() == null);

    try (LocalTlsServer stage = new LocalTlsServer("TLSv1.2")) {
      StageSdkTlsProbe.attemptRequest(stage.getHost(), stage.getPort());

      assertEquals(
          "TLSv1.2",
          stage.awaitNegotiatedProtocol(),
          "without the JVM flag the storage SDK should have negotiated TLS 1.2");
    }
  }
}
