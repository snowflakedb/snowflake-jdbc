package net.snowflake.client.internal.util;

import static net.snowflake.client.internal.util.PlatformDetectionConfig.DEFAULT_DETECTION_TIMEOUT_MS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import net.snowflake.client.SystemPropertyOverrider;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Precedence and parsing rules for the platform-detection settings. Most cases go through the
 * visible-for-testing {@code resolve} overload, because Java's environment map cannot be mutated at
 * runtime; two tests additionally exercise the real system-property lookup.
 */
public class PlatformDetectionConfigTest {

  private static PlatformDetectionConfig resolve(
      Boolean connDisabled,
      Integer connTimeoutMs,
      String disableSysProp,
      String disableEnv,
      String timeoutSysProp,
      String timeoutEnv) {
    return PlatformDetectionConfig.resolve(
        connDisabled, connTimeoutMs, disableSysProp, disableEnv, timeoutSysProp, timeoutEnv);
  }

  @Test
  @DisplayName("Should default to enabled with the built-in timeout when nothing is configured")
  public void shouldDefaultToEnabledWithDefaultTimeout() {
    PlatformDetectionConfig config = resolve(null, null, null, null, null, null);

    assertFalse(config.isDisabled());
    assertEquals(DEFAULT_DETECTION_TIMEOUT_MS, config.getTimeoutMs());
  }

  @Test
  @DisplayName("Connection property should win over both global sources for the disable flag")
  public void shouldPreferConnectionPropertyForDisableFlag() {
    assertTrue(resolve(true, null, "false", "false", null, null).isDisabled());
    // The connection property explicitly saying "false" must not fall through to the globals.
    assertFalse(resolve(false, null, "true", "true", null, null).isDisabled());
  }

  @Test
  @DisplayName("System property should win over the environment variable for the disable flag")
  public void shouldPreferSystemPropertyOverEnvVarForDisableFlag() {
    assertTrue(resolve(null, null, "true", "false", null, null).isDisabled());
    assertFalse(resolve(null, null, "false", "true", null, null).isDisabled());
  }

  @Test
  @DisplayName("Environment variable should apply when nothing higher is set")
  public void shouldFallBackToEnvVarForDisableFlag() {
    assertTrue(resolve(null, null, null, "true", null, null).isDisabled());
  }

  @ParameterizedTest
  @ValueSource(strings = {"TRUE", "True", "  true  "})
  @DisplayName("Disable flag should accept case-insensitive and padded \"true\"")
  public void shouldAcceptCaseInsensitiveTrue(String value) {
    assertTrue(resolve(null, null, value, null, null, null).isDisabled());
    assertTrue(resolve(null, null, null, value, null, null).isDisabled());
  }

  @ParameterizedTest
  @ValueSource(strings = {"1", "yes", "on", "", "false", "garbage"})
  @DisplayName("Disable flag should reject every value other than \"true\"")
  public void shouldRejectNonTrueValues(String value) {
    assertFalse(resolve(null, null, value, null, null, null).isDisabled());
    assertFalse(resolve(null, null, null, value, null, null).isDisabled());
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "   "})
  @DisplayName("Blank global value should be treated as absent, not mask the next source")
  public void shouldNotLetBlankGlobalValueMaskNextSource(String blank) {
    // A bare -Dproperty, or an unset shell variable expanded into one, yields the empty string.
    // Treating that as "set to not-true" would silently defeat an environment variable that is set.
    assertTrue(resolve(null, null, blank, "true", null, null).isDisabled());
    assertFalse(resolve(null, null, null, blank, null, null).isDisabled());
  }

  @Test
  @DisplayName("Blank timeout global should be treated as absent, not mask the next source")
  public void shouldNotLetBlankTimeoutMaskNextSource() {
    assertEquals(33, resolve(null, null, null, null, "", "33").getTimeoutMs());
    assertEquals(33, resolve(null, null, null, null, "   ", "33").getTimeoutMs());
  }

  @Test
  @DisplayName("Explicit false in a global should be authoritative, not fall through")
  public void shouldTreatExplicitFalseAsAuthoritative() {
    assertFalse(resolve(null, null, "false", "true", null, null).isDisabled());
  }

  @Test
  @DisplayName("Unrecognized global value should be authoritative and leave detection enabled")
  public void shouldTreatUnrecognizedGlobalValueAsNotDisabled() {
    assertFalse(resolve(null, null, "yes", "true", null, null).isDisabled());
  }

  @ParameterizedTest
  @CsvSource({"5001, 5000", "300000, 5000", "2147483647, 5000"})
  @DisplayName("Timeout above the maximum should clamp, to bound the global lock hold time")
  public void shouldClampExcessiveTimeout(int configured, int expected) {
    // Detection runs under PlatformDetector's class monitor, so an unclamped value would block
    // every other connecting thread for its full duration.
    assertEquals(expected, resolve(null, configured, null, null, null, null).getTimeoutMs());
    assertEquals(
        expected, resolve(null, null, null, null, String.valueOf(configured), null).getTimeoutMs());
    assertEquals(
        expected, resolve(null, null, null, null, null, String.valueOf(configured)).getTimeoutMs());
  }

  @Test
  @DisplayName("Timeout at the maximum should be preserved")
  public void shouldPreserveTimeoutAtMaximum() {
    assertEquals(
        PlatformDetectionConfig.MAX_DETECTION_TIMEOUT_MS,
        resolve(null, PlatformDetectionConfig.MAX_DETECTION_TIMEOUT_MS, null, null, null, null)
            .getTimeoutMs());
  }

  @Test
  @DisplayName("Timeout should resolve in connection, system property, environment, default order")
  public void shouldResolveTimeoutInPrecedenceOrder() {
    assertEquals(11, resolve(null, 11, "22", "33", null, null).getTimeoutMs());
    assertEquals(22, resolve(null, null, null, null, "22", "33").getTimeoutMs());
    assertEquals(33, resolve(null, null, null, null, null, "33").getTimeoutMs());
    assertEquals(
        DEFAULT_DETECTION_TIMEOUT_MS, resolve(null, null, null, null, null, null).getTimeoutMs());
  }

  @Test
  @DisplayName("Zero timeout should be preserved, since it is the no-network mode")
  public void shouldPreserveZeroTimeout() {
    assertEquals(0, resolve(null, 0, null, null, null, null).getTimeoutMs());
    assertEquals(0, resolve(null, null, null, null, "0", null).getTimeoutMs());
    assertEquals(0, resolve(null, null, null, null, null, "0").getTimeoutMs());
  }

  @ParameterizedTest
  @CsvSource({"-1, 0", "-200, 0"})
  @DisplayName("Negative timeout should clamp to zero rather than be honored")
  public void shouldClampNegativeTimeoutToZero(int configured, int expected) {
    assertEquals(expected, resolve(null, configured, null, null, null, null).getTimeoutMs());
    assertEquals(
        expected, resolve(null, null, null, null, String.valueOf(configured), null).getTimeoutMs());
  }

  @ParameterizedTest
  @ValueSource(strings = {"abc", "1.5"})
  @DisplayName("Unparseable timeout should fall through to the next source instead of failing")
  public void shouldFallThroughOnUnparseableTimeout(String value) {
    // System property unparseable -> environment variable applies.
    assertEquals(33, resolve(null, null, null, null, value, "33").getTimeoutMs());
    // Both unparseable -> built-in default applies.
    assertEquals(
        DEFAULT_DETECTION_TIMEOUT_MS, resolve(null, null, null, null, value, value).getTimeoutMs());
  }

  @Test
  @DisplayName("Should read the disable flag from the real system property")
  public void shouldReadDisableFlagFromRealSystemProperty() {
    SystemPropertyOverrider overrider =
        new SystemPropertyOverrider(PlatformDetectionConfig.DISABLE_SYSTEM_PROPERTY, "true");
    try {
      assertTrue(PlatformDetectionConfig.resolve(null, null).isDisabled());
      assertTrue(PlatformDetectionConfig.fromGlobalConfig().isDisabled());
      // A connection property still overrides the system property.
      assertFalse(PlatformDetectionConfig.resolve(false, null).isDisabled());
    } finally {
      overrider.rollback();
    }
  }

  @Test
  @DisplayName("Should read the timeout from the real system property")
  public void shouldReadTimeoutFromRealSystemProperty() {
    SystemPropertyOverrider overrider =
        new SystemPropertyOverrider(PlatformDetectionConfig.TIMEOUT_MS_SYSTEM_PROPERTY, "0");
    try {
      assertEquals(0, PlatformDetectionConfig.resolve(null, null).getTimeoutMs());
      assertEquals(77, PlatformDetectionConfig.resolve(null, 77).getTimeoutMs());
    } finally {
      overrider.rollback();
    }
  }
}
