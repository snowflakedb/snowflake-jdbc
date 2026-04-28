package net.snowflake.client.internal.core;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import net.snowflake.client.internal.jdbc.OCSPErrorCode;
import net.snowflake.client.internal.util.SFPair;
import org.apache.commons.codec.binary.Base64;
import org.junit.jupiter.api.Test;

/** Regression: a non-SUCCESSFUL OCSP cache entry must surface as SFOCSPException, not NPE. */
public class SFTrustManagerOcspCachePoisoningTest {

  /** RFC 6960 unauthorized(6) OCSP response: SEQUENCE { ENUMERATED 6 }. */
  private static final String UNAUTHORIZED_OCSP_B64 =
      Base64.encodeBase64String(new byte[] {0x30, 0x03, 0x0A, 0x01, 0x06});

  @Test
  public void validateRevocationStatusMain_throwsSfOcspExceptionForUnauthorizedResponse() {
    SFTrustManager tm = new SFTrustManager(new HttpClientSettingsKey(OCSPMode.FAIL_OPEN), null);

    SFOCSPException ex =
        assertThrows(
            SFOCSPException.class,
            () -> tm.validateRevocationStatusMain(SFPair.of(null, null), UNAUTHORIZED_OCSP_B64),
            "Expected SFOCSPException for an unauthorized(6) OCSP payload (was NPE before fix)");

    assertSame(
        OCSPErrorCode.INVALID_OCSP_RESPONSE,
        ex.getErrorCode(),
        "Unauthorized(6) payloads must surface as INVALID_OCSP_RESPONSE so isCached evicts them"
            + " and the fail-open gate engages");
  }
}
