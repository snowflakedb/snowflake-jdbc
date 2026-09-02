package net.snowflake.client.internal.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import net.snowflake.client.internal.jdbc.OCSPErrorCode;
import net.snowflake.client.internal.util.SFPair;
import org.apache.commons.codec.binary.Base64;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Regression: a non-SUCCESSFUL OCSP cache entry must surface as SFOCSPException, not NPE. */
public class SFTrustManagerOcspCachePoisoningTest {

  /** RFC 6960 unauthorized(6) OCSP response: SEQUENCE { ENUMERATED 6 }. */
  private static final String UNAUTHORIZED_OCSP_B64 =
      Base64.encodeBase64String(new byte[] {0x30, 0x03, 0x0A, 0x01, 0x06});

  @AfterEach
  public void resetStaticState() {
    SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_URL_VALUE = null;
    SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_RETRY_URL_PATTERN = null;
  }

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

  @Test
  public void resetOCSPResponseCacherServerURL_rejectsNonSnowflakeHost() throws IOException {
    SFTrustManager.resetOCSPResponseCacherServerURL(
        "http://ocsp.evil.privatelink.snowflakecomputing.attacker.com/ocsp_response_cache.json");

    assertNull(
        SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_URL_VALUE,
        "URL with attacker-controlled host must not be accepted");
  }

  @Test
  public void resetOCSPResponseCacherServerURL_acceptsLegitimatePrivateLinkHost()
      throws IOException {
    String legitimateUrl =
        "http://ocsp.account.privatelink.snowflakecomputing.com/ocsp_response_cache.json";
    SFTrustManager.resetOCSPResponseCacherServerURL(legitimateUrl);

    assertSame(
        legitimateUrl,
        SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_URL_VALUE,
        "Legitimate PrivateLink OCSP cache URL must be accepted");
  }

  @Test
  public void resetOCSPResponseCacherServerURL_rejectsSnowflakeDomainAsSubdomain()
      throws IOException {
    SFTrustManager.resetOCSPResponseCacherServerURL(
        "http://ocsp.account.snowflakecomputing.com.evil.com/ocsp_response_cache.json");

    assertNull(
        SFTrustManager.SF_OCSP_RESPONSE_CACHE_SERVER_URL_VALUE,
        "Snowflake domain embedded in attacker domain must not be accepted");
  }

  @Test
  public void ocspCacheServer_nonSnowflakeHostFallsBackToSafeDefault() {
    SFTrustManager.OCSPCacheServer cacheServer = new SFTrustManager.OCSPCacheServer();
    cacheServer.resetOCSPResponseCacheServer("evil.privatelink.snowflakecomputing.attacker.com");

    assertEquals(
        "https://ocspssd.snowflakecomputing.com/ocsp/fetch",
        cacheServer.SF_OCSP_RESPONSE_CACHE_SERVER,
        "Host with .snowflakecomputing. as substring but invalid domain must fall back to safe default");
  }

  @Test
  public void ocspCacheServer_acceptsLegitimateSnowflakeHost() {
    SFTrustManager.OCSPCacheServer cacheServer = new SFTrustManager.OCSPCacheServer();
    cacheServer.resetOCSPResponseCacheServer("account.us-west-2.aws.snowflakecomputing.com");

    assertTrue(
        cacheServer.SF_OCSP_RESPONSE_CACHE_SERVER.contains("ocspssd"),
        "Legitimate Snowflake host must produce a valid OCSP cache server URL");
  }

  @Test
  public void ocspCacheServer_acceptsGlobalSnowflakeHost() {
    SFTrustManager.OCSPCacheServer cacheServer = new SFTrustManager.OCSPCacheServer();
    cacheServer.resetOCSPResponseCacheServer("account-abc123.global.snowflakecomputing.com");

    assertTrue(
        cacheServer.SF_OCSP_RESPONSE_CACHE_SERVER.contains("ocspssd"),
        "Global Snowflake host must produce a valid OCSP cache server URL");
  }

  @Test
  public void isAllowedOcspHost_acceptsOcspPrefixedSnowflakeHost() {
    assertTrue(SFTrustManager.isAllowedOcspHost("ocsp.account.privatelink.snowflakecomputing.com"));
  }

  @Test
  public void isAllowedOcspHost_rejectsOcspPrefixedAttackerHost() {
    assertFalse(
        SFTrustManager.isAllowedOcspHost("ocsp.evil.privatelink.snowflakecomputing.attacker.com"));
  }

  @Test
  public void isAllowedOcspHost_rejectsNullHost() {
    assertFalse(SFTrustManager.isAllowedOcspHost(null));
  }

  @Test
  public void isAllowedOcspHost_acceptsPlainSnowflakeHost() {
    assertTrue(SFTrustManager.isAllowedOcspHost("account.snowflakecomputing.com"));
  }
}
