package net.snowflake.client.internal.util;

import static net.snowflake.client.internal.jdbc.SnowflakeUtil.systemGetEnv;
import static net.snowflake.client.internal.jdbc.SnowflakeUtil.systemGetProperty;

import net.snowflake.client.internal.log.SFLogger;
import net.snowflake.client.internal.log.SFLoggerFactory;

/**
 * Effective platform-detection settings for a single login.
 *
 * <p>The JVM-wide fallbacks exist because the static detection cache in {@link PlatformDetector}
 * makes the metadata probes a process-level side effect — a per-connection switch alone cannot
 * guarantee that no probe is ever issued, since an earlier connection that left detection enabled
 * has already emitted them.
 *
 * <p>The asymmetry in boolean parsing is deliberate: {@link #DISABLE_SYSTEM_PROPERTY} and {@link
 * #DISABLE_ENV_VAR} accept only the case-insensitive literal {@code "true"}, while the pre-existing
 * {@code disablePlatformDetection} connection property keeps its looser {@code
 * SFLoginInput.getBooleanValue} semantics (which also accept {@code "on"}) for backward
 * compatibility.
 */
public final class PlatformDetectionConfig {

  private static final SFLogger logger = SFLoggerFactory.getLogger(PlatformDetectionConfig.class);

  /** Applied per network-dependent detector, not as a total budget across all of them. */
  public static final int DEFAULT_DETECTION_TIMEOUT_MS = 200;

  /**
   * Detection runs while {@code PlatformDetector} holds its class monitor, so an arbitrarily large
   * timeout would block every other thread trying to open a connection for that whole duration.
   * Platform detection is only telemetry enrichment, so a configured value above this is clamped
   * rather than honored.
   */
  static final int MAX_DETECTION_TIMEOUT_MS = 5_000;

  public static final String DISABLE_SYSTEM_PROPERTY =
      "net.snowflake.jdbc.disablePlatformDetection";

  public static final String DISABLE_ENV_VAR = "SNOWFLAKE_DISABLE_PLATFORM_DETECTION";

  public static final String TIMEOUT_MS_SYSTEM_PROPERTY =
      "net.snowflake.jdbc.platformDetectionTimeoutMs";

  public static final String TIMEOUT_MS_ENV_VAR = "SNOWFLAKE_PLATFORM_DETECTION_TIMEOUT_MS";

  private final boolean disabled;
  private final int timeoutMs;

  private PlatformDetectionConfig(boolean disabled, int timeoutMs) {
    this.disabled = disabled;
    this.timeoutMs = timeoutMs;
  }

  /** Null arguments mean the corresponding connection property was not supplied. */
  public static PlatformDetectionConfig resolve(
      Boolean connectionDisabled, Integer connectionTimeoutMs) {
    return resolve(
        connectionDisabled,
        connectionTimeoutMs,
        systemGetProperty(DISABLE_SYSTEM_PROPERTY),
        systemGetEnv(DISABLE_ENV_VAR),
        systemGetProperty(TIMEOUT_MS_SYSTEM_PROPERTY),
        systemGetEnv(TIMEOUT_MS_ENV_VAR));
  }

  public static PlatformDetectionConfig fromGlobalConfig() {
    return resolve(null, null);
  }

  /**
   * Visible-for-testing overload that takes the raw system-property and environment values as
   * parameters, so unit tests can exercise the precedence chain without mutating the JVM
   * environment (Java's {@code System.getenv} map is immutable at runtime).
   */
  static PlatformDetectionConfig resolve(
      Boolean connectionDisabled,
      Integer connectionTimeoutMs,
      String disableSystemProperty,
      String disableEnvVar,
      String timeoutSystemProperty,
      String timeoutEnvVar) {
    return new PlatformDetectionConfig(
        resolveDisabled(connectionDisabled, disableSystemProperty, disableEnvVar),
        resolveTimeoutMs(connectionTimeoutMs, timeoutSystemProperty, timeoutEnvVar));
  }

  private static boolean resolveDisabled(
      Boolean connectionDisabled, String disableSystemProperty, String disableEnvVar) {
    if (connectionDisabled != null) {
      return connectionDisabled;
    }
    Boolean fromSystemProperty = parseDisabled(disableSystemProperty, DISABLE_SYSTEM_PROPERTY);
    if (fromSystemProperty != null) {
      return fromSystemProperty;
    }
    Boolean fromEnvVar = parseDisabled(disableEnvVar, DISABLE_ENV_VAR);
    if (fromEnvVar != null) {
      return fromEnvVar;
    }
    return false;
  }

  /**
   * Returns {@code null} for a blank value so it is treated as absent: a bare {@code -Dproperty}
   * with no value, or an unset shell variable expanded into one, yields the empty string, and that
   * must not silently override a lower-precedence source that is genuinely set. Any other
   * unrecognized value is authoritative at its own precedence level but warned about, since a
   * reader who wrote {@code yes} expects suppression and would otherwise get none, silently.
   */
  private static Boolean parseDisabled(String value, String source) {
    if (value == null || value.trim().isEmpty()) {
      return null;
    }
    String trimmed = value.trim();
    if ("true".equalsIgnoreCase(trimmed)) {
      return Boolean.TRUE;
    }
    if (!"false".equalsIgnoreCase(trimmed)) {
      logger.warn(
          "Ignoring unrecognized value {} for {}; only \"true\" disables platform detection",
          value,
          source);
    }
    return Boolean.FALSE;
  }

  private static int resolveTimeoutMs(
      Integer connectionTimeoutMs, String timeoutSystemProperty, String timeoutEnvVar) {
    if (connectionTimeoutMs != null) {
      return clampTimeoutMs(connectionTimeoutMs, "connection property");
    }
    Integer fromSystemProperty = parseTimeoutMs(timeoutSystemProperty, TIMEOUT_MS_SYSTEM_PROPERTY);
    if (fromSystemProperty != null) {
      return clampTimeoutMs(fromSystemProperty, TIMEOUT_MS_SYSTEM_PROPERTY);
    }
    Integer fromEnvVar = parseTimeoutMs(timeoutEnvVar, TIMEOUT_MS_ENV_VAR);
    if (fromEnvVar != null) {
      return clampTimeoutMs(fromEnvVar, TIMEOUT_MS_ENV_VAR);
    }
    return DEFAULT_DETECTION_TIMEOUT_MS;
  }

  /**
   * Returns {@code null} when the value is absent or unparseable, so resolution falls through to
   * the next source instead of failing the connection.
   */
  private static Integer parseTimeoutMs(String value, String source) {
    if (value == null || value.trim().isEmpty()) {
      return null;
    }
    try {
      return Integer.valueOf(value.trim());
    } catch (NumberFormatException e) {
      logger.warn("Failed to parse the platform detection timeout {} with value {}", source, value);
      return null;
    }
  }

  /**
   * A negative timeout would still start the metadata requests before the wait gives up, so it is
   * clamped to 0 — which skips the network-dependent detectors outright — rather than being honored
   * or rejected.
   */
  private static int clampTimeoutMs(int timeoutMs, String source) {
    if (timeoutMs < 0) {
      logger.warn(
          "Negative platform detection timeout {}ms from {}; treating it as 0, which skips the"
              + " network-dependent checks",
          timeoutMs,
          source);
      return 0;
    }
    if (timeoutMs > MAX_DETECTION_TIMEOUT_MS) {
      logger.warn(
          "Platform detection timeout {}ms from {} exceeds the maximum {}ms and would stall other"
              + " connections; clamping it",
          timeoutMs,
          source,
          MAX_DETECTION_TIMEOUT_MS);
      return MAX_DETECTION_TIMEOUT_MS;
    }
    return timeoutMs;
  }

  public boolean isDisabled() {
    return disabled;
  }

  /**
   * {@code 0} skips the network-dependent detectors, leaving only the environment-variable ones.
   */
  public int getTimeoutMs() {
    return timeoutMs;
  }

  @Override
  public String toString() {
    return "PlatformDetectionConfig{disabled=" + disabled + ", timeoutMs=" + timeoutMs + "}";
  }
}
