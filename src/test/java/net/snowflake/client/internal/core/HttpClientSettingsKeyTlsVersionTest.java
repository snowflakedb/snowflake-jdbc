package net.snowflake.client.internal.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class HttpClientSettingsKeyTlsVersionTest {

  @Test
  public void shouldDefaultToTheDriverDefaults() {
    HttpClientSettingsKey key = new HttpClientSettingsKey(OCSPMode.FAIL_OPEN);

    assertEquals(TlsVersion.DEFAULT_MIN, key.getMinTlsVersion());
    assertEquals(TlsVersion.DEFAULT_MAX, key.getMaxTlsVersion());
  }

  @Test
  public void shouldNotEqualWhenMinimumDiffers() {
    HttpClientSettingsKey relaxed = new HttpClientSettingsKey(OCSPMode.FAIL_OPEN);
    HttpClientSettingsKey strict = new HttpClientSettingsKey(OCSPMode.FAIL_OPEN);
    strict.setTlsVersions(TlsVersion.TLS_1_3, TlsVersion.TLS_1_3);

    assertNotEquals(relaxed, strict);
    assertNotEquals(strict, relaxed);
    assertNotEquals(relaxed.hashCode(), strict.hashCode());
  }

  @Test
  public void shouldNotEqualWhenMaximumDiffers() {
    HttpClientSettingsKey wide = new HttpClientSettingsKey(OCSPMode.FAIL_OPEN);
    HttpClientSettingsKey capped = new HttpClientSettingsKey(OCSPMode.FAIL_OPEN);
    capped.setTlsVersions(TlsVersion.TLS_1_2, TlsVersion.TLS_1_2);

    assertNotEquals(wide, capped);
    assertNotEquals(wide.hashCode(), capped.hashCode());
  }

  @Test
  public void shouldStillEqualWhenTlsVersionsMatch() {
    HttpClientSettingsKey first = new HttpClientSettingsKey(OCSPMode.FAIL_OPEN);
    first.setTlsVersions(TlsVersion.TLS_1_3, TlsVersion.TLS_1_3);
    HttpClientSettingsKey second = new HttpClientSettingsKey(OCSPMode.FAIL_OPEN);
    second.setTlsVersions(TlsVersion.TLS_1_3, TlsVersion.TLS_1_3);

    assertEquals(first, second);
    assertEquals(first.hashCode(), second.hashCode());
  }

  @Test
  public void shouldIncludeTlsVersionsInToStringForDiagnostics() {
    HttpClientSettingsKey key = new HttpClientSettingsKey(OCSPMode.FAIL_OPEN);
    key.setTlsVersions(TlsVersion.TLS_1_3, TlsVersion.TLS_1_3);

    String rendered = key.toString();
    assertTrue(rendered.contains("minTlsVersion=TLS_1_3"), rendered);
    assertTrue(rendered.contains("maxTlsVersion=TLS_1_3"), rendered);
  }
}
