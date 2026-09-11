package net.snowflake.client.internal.core;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.lang.reflect.Field;
import java.security.NoSuchAlgorithmException;
import javax.net.ssl.SSLContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class SFSSLConnectionSocketFactoryTest {

  private String originalJdkClientProtocols;

  @BeforeEach
  public void rememberJdkProperty() {
    originalJdkClientProtocols =
        System.getProperty(SFSSLConnectionSocketFactory.JDK_TLS_CLIENT_PROTOCOLS);
  }

  /**
   * Restores rather than clears: these tests share a JVM with the rest of the unit suite, and the
   * property may have been supplied externally for the whole run.
   */
  @AfterEach
  public void restoreJdkProperty() {
    if (originalJdkClientProtocols == null) {
      System.clearProperty(SFSSLConnectionSocketFactory.JDK_TLS_CLIENT_PROTOCOLS);
    } else {
      System.setProperty(
          SFSSLConnectionSocketFactory.JDK_TLS_CLIENT_PROTOCOLS, originalJdkClientProtocols);
    }
  }

  // ── Precedence: jdk.tls.client.protocols wins ────────────────────────────

  @Test
  public void shouldTakeVersionsFromJdkClientProtocols() {
    assumeTrue(isTls13Available());
    System.setProperty(SFSSLConnectionSocketFactory.JDK_TLS_CLIENT_PROTOCOLS, "TLSv1.3");

    assertArrayEquals(
        new String[] {"TLSv1.3"},
        SFSSLConnectionSocketFactory.resolveEnabledProtocols(
            TlsVersion.TLS_1_2, TlsVersion.TLS_1_3));
  }

  @Test
  public void shouldPreferJdkClientProtocolsOverConnectionProperty() {
    System.setProperty(SFSSLConnectionSocketFactory.JDK_TLS_CLIENT_PROTOCOLS, "TLSv1.2");

    // the deprecated connection properties must not override the system property
    assertArrayEquals(
        new String[] {"TLSv1.2"},
        SFSSLConnectionSocketFactory.resolveEnabledProtocols(
            TlsVersion.TLS_1_3, TlsVersion.TLS_1_3));
  }

  /**
   * Regression guard: deferring to the JSSE defaults would let an externally configured TLS 1.0/1.1
   * be offered to Snowflake, because Apache HttpClient only strips {@code SSL*} names by itself.
   */
  @Test
  public void shouldNotLowerTheDriverFloorFromTheJvmProperty() {
    System.setProperty(
        SFSSLConnectionSocketFactory.JDK_TLS_CLIENT_PROTOCOLS, "TLSv1,TLSv1.1,TLSv1.2");

    assertArrayEquals(
        new String[] {"TLSv1.2"},
        SFSSLConnectionSocketFactory.resolveEnabledProtocols(
            TlsVersion.TLS_1_2, TlsVersion.TLS_1_3));
  }

  @Test
  public void shouldRejectJdkClientProtocolsNamingNoSupportedVersion() {
    System.setProperty(SFSSLConnectionSocketFactory.JDK_TLS_CLIENT_PROTOCOLS, "TLSv1,TLSv1.1");

    assertThrows(
        IllegalStateException.class,
        () ->
            SFSSLConnectionSocketFactory.resolveEnabledProtocols(
                TlsVersion.TLS_1_2, TlsVersion.TLS_1_3));
  }

  @Test
  public void shouldIgnoreBlankJdkClientProtocols() {
    System.setProperty(SFSSLConnectionSocketFactory.JDK_TLS_CLIENT_PROTOCOLS, "   ");

    assertArrayEquals(
        new String[] {"TLSv1.2"},
        SFSSLConnectionSocketFactory.resolveEnabledProtocols(
            TlsVersion.TLS_1_2, TlsVersion.TLS_1_2));
  }

  // ── Fallback: deprecated connection properties ───────────────────────────

  @Test
  public void shouldUseDefaultRangeWhenNothingConfigured() {
    String[] protocols =
        SFSSLConnectionSocketFactory.resolveEnabledProtocols(
            TlsVersion.DEFAULT_MIN, TlsVersion.DEFAULT_MAX);

    if (isTls13Available()) {
      assertArrayEquals(new String[] {"TLSv1.2", "TLSv1.3"}, protocols);
    } else {
      assertArrayEquals(new String[] {"TLSv1.2"}, protocols);
    }
  }

  @ParameterizedTest
  @CsvSource({
    "TLSv1.2,TLSv1.3,TLSv1.2 TLSv1.3",
    "TLSv1.2,TLSv1.2,TLSv1.2",
    "TLSv1.3,TLSv1.3,TLSv1.3"
  })
  public void shouldHonorRangeWhenJdkPropertyAbsent(String min, String max, String expected) {
    assumeTrue(isTls13Available());

    String[] protocols =
        SFSSLConnectionSocketFactory.resolveEnabledProtocols(
            TlsVersion.fromString(min), TlsVersion.fromString(max));

    assertEquals(expected, String.join(" ", protocols));
  }

  @Test
  public void shouldRejectMinGreaterThanMax() {
    // defence in depth: SFBaseSession rejects this at property-set time with a SQLException
    assertThrows(
        IllegalArgumentException.class,
        () ->
            SFSSLConnectionSocketFactory.resolveEnabledProtocols(
                TlsVersion.TLS_1_3, TlsVersion.TLS_1_2));
  }

  // ── Wiring: what actually reaches Apache HttpClient ──────────────────────

  /**
   * Apache's {@code SSLConnectionSocketFactory} calls {@code setEnabledProtocols} only when {@code
   * supportedProtocols} is non-null; otherwise it leaves the JSSE defaults in place. These two
   * tests assert the constructed factory really carries what {@code resolveEnabledProtocols}
   * decided, since that field is what governs the handshake.
   */
  @Test
  public void shouldPassJvmPropertyVersionsToApacheWhenSet() throws Exception {
    assumeTrue(isTls13Available());
    System.setProperty(SFSSLConnectionSocketFactory.JDK_TLS_CLIENT_PROTOCOLS, "TLSv1.3");

    assertArrayEquals(
        new String[] {"TLSv1.3"},
        supportedProtocolsOf(newFactory(TlsVersion.TLS_1_2, TlsVersion.TLS_1_3)));
  }

  @Test
  public void shouldPassExplicitSupportedProtocolsWhenJdkPropertyAbsent() throws Exception {
    assumeTrue(isTls13Available());

    assertArrayEquals(
        new String[] {"TLSv1.3"},
        supportedProtocolsOf(newFactory(TlsVersion.TLS_1_3, TlsVersion.TLS_1_3)));
  }

  private static SFSSLConnectionSocketFactory newFactory(TlsVersion min, TlsVersion max)
      throws Exception {
    return new SFSSLConnectionSocketFactory(null, false, min, max);
  }

  private static String[] supportedProtocolsOf(SFSSLConnectionSocketFactory factory)
      throws Exception {
    Field field =
        org.apache.http.conn.ssl.SSLConnectionSocketFactory.class.getDeclaredField(
            "supportedProtocols");
    field.setAccessible(true);
    return (String[]) field.get(factory);
  }

  private boolean isTls13Available() {
    try {
      SSLContext.getInstance("TLSv1.3");
      return true;
    } catch (NoSuchAlgorithmException e) {
      return false;
    }
  }
}
