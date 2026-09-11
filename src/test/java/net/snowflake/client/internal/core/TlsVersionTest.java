package net.snowflake.client.internal.core;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import javax.net.ssl.SSLContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TlsVersionTest {

  @ParameterizedTest
  @ValueSource(strings = {"TLSv1.2", "tlsv1.2", "  TLSv1.2  "})
  public void shouldParseCaseInsensitivelyAndTrim(String text) {
    assertEquals(TlsVersion.TLS_1_2, TlsVersion.fromString(text));
  }

  @Test
  public void shouldRejectNullRatherThanReturningIt() {
    // returning null here used to defer the failure to a later NullPointerException
    assertThrows(IllegalArgumentException.class, () -> TlsVersion.fromString(null));
  }

  @ParameterizedTest
  @ValueSource(strings = {"TLSv1.1", "SSLv3", "", "1.3", "TLSv1.4"})
  public void shouldRejectUnknownVersions(String text) {
    assertThrows(IllegalArgumentException.class, () -> TlsVersion.fromString(text));
  }

  @Test
  public void shouldOrderVersionsOldestFirst() {
    // protocolsInRange relies on declaration order via compareTo
    assertTrue(TlsVersion.TLS_1_2.compareTo(TlsVersion.TLS_1_3) < 0);
  }

  @Test
  public void shouldListSupportedValuesForErrorMessages() {
    assertEquals("TLSv1.2, TLSv1.3", TlsVersion.supportedValues());
  }

  @Test
  public void shouldRejectInvertedRange() {
    assertThrows(
        IllegalArgumentException.class,
        () -> TlsVersion.protocolsInRange(TlsVersion.TLS_1_3, TlsVersion.TLS_1_2));
  }

  @Test
  public void shouldReturnOnlyVersionsInRange() {
    assumeTrue(isTls13Available());

    assertArrayEquals(
        new String[] {"TLSv1.3"},
        TlsVersion.protocolsInRange(TlsVersion.TLS_1_3, TlsVersion.TLS_1_3));
  }

  /**
   * Everything offered must be something this JVM will actually enable, otherwise the driver
   * advertises a protocol that can never be negotiated. Holds on any JVM.
   */
  @Test
  public void shouldOnlyOfferProtocolsTheJvmCanEnable() throws Exception {
    SSLContext context = SSLContext.getInstance("TLS");
    context.init(null, null, null);
    Set<String> enabled =
        new HashSet<>(Arrays.asList(context.getDefaultSSLParameters().getProtocols()));

    for (String offered :
        TlsVersion.protocolsInRange(TlsVersion.DEFAULT_MIN, TlsVersion.DEFAULT_MAX)) {
      assertTrue(
          enabled.contains(offered), offered + " is offered but this JVM will not enable it");
    }
  }

  /**
   * A JVM banning TLSv1.2 through {@code jdk.tls.disabledAlgorithms} still reports it as
   * <em>supported</em>, and {@code setEnabledProtocols} still accepts it -- it simply never gets
   * negotiated. Filtering on the supported set would therefore offer a dead protocol, so the
   * allowed set is what the JVM will enable. Simulated by injection here: a real ban needs {@code
   * -Djava.security.properties} at JVM launch, which conflicts with the TLS 1.2 servers the Arc
   * Reactor suite relies on and so cannot share that profile's forked JVM.
   */
  @Test
  public void shouldSkipBannedVersionWithinTheRange() {
    Set<String> tls13Only = Collections.singleton("TLSv1.3");

    assertArrayEquals(
        new String[] {"TLSv1.3"},
        TlsVersion.protocolsInRange(TlsVersion.DEFAULT_MIN, TlsVersion.DEFAULT_MAX, tls13Only));
  }

  /** With every version in the range banned there is nothing to offer, so this must fail loudly. */
  @Test
  public void shouldFailWhenEveryVersionInRangeIsBanned() {
    Set<String> tls13Only = Collections.singleton("TLSv1.3");

    assertThrows(
        IllegalStateException.class,
        () -> TlsVersion.protocolsInRange(TlsVersion.TLS_1_2, TlsVersion.TLS_1_2, tls13Only));
  }

  private boolean isTls13Available() {
    try {
      SSLContext.getInstance("TLSv1.3");
      return true;
    } catch (Exception e) {
      return false;
    }
  }
}
