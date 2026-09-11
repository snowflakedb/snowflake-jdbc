package net.snowflake.client.internal.util;

import static net.snowflake.client.internal.util.PlatformDetectionConfig.DEFAULT_DETECTION_TIMEOUT_MS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import net.snowflake.client.internal.core.auth.wif.AwsAttestationService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Uses the visible-for-testing injection overload so the assertions can be about whether detection
 * ran at all, not just about the value returned.
 */
public class PlatformDetectorCacheTest {

  private static final List<String> DETECTED = Arrays.asList("is_aws_lambda");

  private PlatformDetector mockDetector;
  private AwsAttestationService mockAttestationService;

  @BeforeEach
  public void setUp() {
    mockDetector = mock(PlatformDetector.class);
    mockAttestationService = mock(AwsAttestationService.class);
    when(mockDetector.detectPlatforms(anyInt(), any())).thenReturn(DETECTED);
    PlatformDetector.resetCacheForTesting();
  }

  @AfterEach
  public void tearDown() {
    PlatformDetector.resetCacheForTesting();
  }

  private List<String> detect(PlatformDetectionConfig config) {
    return PlatformDetector.getCachedPlatformDetection(
        config, mockDetector, mockAttestationService);
  }

  private static PlatformDetectionConfig enabledWithTimeout(int timeoutMs) {
    return PlatformDetectionConfig.resolve(false, timeoutMs);
  }

  private static PlatformDetectionConfig disabled() {
    return PlatformDetectionConfig.resolve(true, null);
  }

  @Test
  @DisplayName("Disabled detection should return the sentinel without running any detection")
  public void shouldReturnDisabledSentinelWithoutDetecting() {
    List<String> platforms = detect(disabled());

    assertEquals(Collections.singletonList("disabled"), platforms);
    verify(mockDetector, never()).detectPlatforms(anyInt(), any());
  }

  @Test
  @DisplayName("Disabled detection should not populate the cache")
  public void shouldNotCacheWhenDisabled() {
    detect(disabled());

    // A subsequent enabled lookup must still perform detection; the disabled call must not have
    // seeded the cache with the sentinel.
    List<String> platforms = detect(enabledWithTimeout(0));

    assertEquals(DETECTED, platforms);
    verify(mockDetector, times(1)).detectPlatforms(eq(0), any());
  }

  @Test
  @DisplayName("Should pass the configured timeout to the detector rather than the default")
  public void shouldUseConfiguredTimeout() {
    detect(enabledWithTimeout(0));

    // The regression this guards: the timeout used to be hardcoded to the default, so a configured
    // 0 (no network probes) silently ran a full 200ms detection round.
    verify(mockDetector, times(1)).detectPlatforms(eq(0), any());
    verify(mockDetector, never()).detectPlatforms(eq(DEFAULT_DETECTION_TIMEOUT_MS), any());
  }

  @Test
  @DisplayName("Repeated lookups with the same timeout should detect once and return the same list")
  public void shouldCacheForRepeatedSameTimeout() {
    List<String> first = detect(enabledWithTimeout(50));
    List<String> second = detect(enabledWithTimeout(50));

    assertSame(first, second, "Second lookup should return the cached instance");
    verify(mockDetector, times(1)).detectPlatforms(eq(50), any());
  }

  @Test
  @DisplayName("Different timeouts should be cached separately, not share the first result")
  public void shouldCacheSeparatelyPerTimeout() {
    detect(enabledWithTimeout(0));
    detect(enabledWithTimeout(50));

    // Keying by timeout is what stops a later connection from silently inheriting the timeout the
    // first connection in the JVM happened to use.
    verify(mockDetector, times(1)).detectPlatforms(eq(0), any());
    verify(mockDetector, times(1)).detectPlatforms(eq(50), any());
  }

  @Test
  @DisplayName("Cache should stop growing past its bound rather than retaining every timeout")
  public void shouldBoundCacheSize() {
    // Fill the cache to its limit with distinct timeouts.
    for (int timeoutMs = 1; timeoutMs <= 8; timeoutMs++) {
      detect(enabledWithTimeout(timeoutMs));
    }
    // An early key is still cached, so it does not re-detect.
    detect(enabledWithTimeout(1));
    verify(mockDetector, times(1)).detectPlatforms(eq(1), any());

    // A key past the bound is served but not retained, so it re-detects each time. That trades
    // repeated work for a map that cannot grow without limit.
    detect(enabledWithTimeout(99));
    detect(enabledWithTimeout(99));
    verify(mockDetector, times(2)).detectPlatforms(eq(99), any());
  }

  @Test
  @DisplayName("Negative timeout should skip the network detectors, not fabricate timeout entries")
  public void shouldSkipNetworkDetectorsOnNegativeTimeout() {
    EnvironmentProvider noEnv = mock(EnvironmentProvider.class);
    when(noEnv.getEnv(anyString())).thenReturn(null);
    // Unroutable metadata URLs: if the guard let a negative timeout through, every probe would be
    // started and then immediately abandoned, yielding a list of fabricated "_timeout" entries.
    PlatformDetector realDetector =
        new PlatformDetector(
            "http://127.0.0.1:1",
            "http://127.0.0.1:1",
            "http://127.0.0.1:1",
            "http://127.0.0.1:1",
            noEnv);

    List<String> platforms = realDetector.detectPlatforms(-1, mockAttestationService);

    assertEquals(Collections.emptyList(), platforms);
  }

  @Test
  @DisplayName("Cache reset should force re-detection")
  public void shouldRedetectAfterCacheReset() {
    detect(enabledWithTimeout(0));
    PlatformDetector.resetCacheForTesting();
    detect(enabledWithTimeout(0));

    verify(mockDetector, times(2)).detectPlatforms(eq(0), any());
  }
}
