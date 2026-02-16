package net.snowflake.client.internal.driver;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import net.snowflake.client.internal.core.SecurityUtil;
import net.snowflake.client.internal.core.minicore.Minicore;
import net.snowflake.client.internal.jdbc.SnowflakeUtil;
import net.snowflake.client.internal.jdbc.telemetryOOB.TelemetryService;
import net.snowflake.client.internal.log.SFLogger;
import net.snowflake.client.internal.log.SFLoggerFactory;

/**
 * Handles all one-time initialization for the Snowflake JDBC driver.
 *
 * <p>This includes:
 *
 * <ul>
 *   <li>Arrow result format support and Netty configuration
 *   <li>BouncyCastle security provider registration
 *   <li>Out-of-band telemetry configuration
 *   <li>Suppression of illegal reflective access warnings
 * </ul>
 *
 * <p>All initialization is performed once in the static block of {@link
 * net.snowflake.client.api.driver.SnowflakeDriver}. This class is thread-safe and ensures
 * initialization happens exactly once.
 */
public final class DriverInitializer {
  private static final SFLogger logger = SFLoggerFactory.getLogger(DriverInitializer.class);

  private static volatile boolean initialized = false;
  private static volatile boolean arrowEnabled = true;
  private static volatile String arrowDisableReason = null;

  private DriverInitializer() {
    // Utility class - prevent instantiation
  }

  /**
   * Perform all driver initialization. This method is idempotent and thread-safe.
   *
   * <p>If called multiple times, subsequent calls are no-ops.
   */
  public static synchronized void initialize() {
    if (initialized) {
      logger.debug("Driver already initialized, skipping");
      return;
    }

    logger.info("Initializing Snowflake JDBC Driver...");

    initializeArrowSupport();
    initializeSecurityProvider();
    initializeTelemetry();
    initializeMinicore();

    initialized = true;
    logger.info("Snowflake JDBC Driver initialization complete");
  }

  /**
   * Initialize Apache Arrow support for high-performance result sets.
   *
   * <p>This method configures Netty for Arrow buffer memory management and attempts to suppress
   * illegal reflective access warnings. If initialization fails, Arrow is disabled and the driver
   * will fall back to JSON result format.
   */
  private static void initializeArrowSupport() {
    try {
      // Configure Netty for Arrow buffer memory management
      configureNettyForArrow();

      // Suppress reflective access warnings from Netty/Arrow
      suppressIllegalReflectiveAccessWarnings();

      arrowEnabled = true;
      logger.info("Arrow result format enabled successfully");
    } catch (Throwable t) {
      arrowEnabled = false;
      arrowDisableReason = t.getLocalizedMessage();
      logger.warn("Failed to enable Arrow result format: {}", arrowDisableReason);
    }
  }

  /**
   * Configure Netty for Arrow buffer memory management.
   *
   * <p>This sets a system property required to enable direct memory usage for Arrow buffers in
   * Java.
   */
  private static void configureNettyForArrow() {
    // Required to enable direct memory usage for Arrow buffers in Java
    System.setProperty("io.netty.tryReflectionSetAccessible", "true");
  }

  /**
   * Suppress illegal reflective access warnings caused by Netty/Arrow dependencies.
   *
   * <p>Only suppresses warnings if not explicitly disabled via the {@code
   * snowflake.jdbc.enable.illegalAccessWarning} system property.
   *
   * <p>This uses sun.misc.Unsafe to set jdk.internal.module.IllegalAccessLogger's logger to null,
   * effectively disabling the warnings. This is necessary because the Netty dependency of Apache
   * Arrow causes warnings on Java 9+. Failures are non-fatal and the driver will continue to
   * function normally.
   */
  private static void suppressIllegalReflectiveAccessWarnings() {
    if ("true"
        .equals(SnowflakeUtil.systemGetProperty("snowflake.jdbc.enable.illegalAccessWarning"))) {
      logger.debug("Keeping illegal access warnings enabled (user requested)");
      return;
    }

    try {
      // Get sun.misc.Unsafe class and instance
      Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
      Field field = unsafeClass.getDeclaredField("theUnsafe");
      field.setAccessible(true);
      Object unsafe = field.get(null);

      // Get Unsafe methods for manipulating static fields
      Method putObjectVolatile =
          unsafeClass.getDeclaredMethod(
              "putObjectVolatile", Object.class, long.class, Object.class);
      Method staticFieldOffset = unsafeClass.getDeclaredMethod("staticFieldOffset", Field.class);
      Method staticFieldBase = unsafeClass.getDeclaredMethod("staticFieldBase", Field.class);

      // Get the IllegalAccessLogger class and its logger field
      Class<?> loggerClass = Class.forName("jdk.internal.module.IllegalAccessLogger");
      Field loggerField = loggerClass.getDeclaredField("logger");

      // Use Unsafe to set the logger to null, effectively disabling warnings
      Long loggerOffset = (Long) staticFieldOffset.invoke(unsafe, loggerField);
      Object loggerBase = staticFieldBase.invoke(unsafe, loggerField);
      putObjectVolatile.invoke(unsafe, loggerBase, loggerOffset, null);

      logger.debug("Illegal reflective access warnings suppressed");
    } catch (Throwable ex) {
      // Non-fatal - just log and continue
      logger.debug("Failed to suppress reflective access warnings: {}", ex.getMessage());
    }
  }

  /**
   * Register BouncyCastle security provider for cryptographic operations.
   *
   * <p>BouncyCastle is used for various security operations in the JDBC driver.
   */
  private static void initializeSecurityProvider() {
    try {
      SecurityUtil.addBouncyCastleProvider();
      logger.debug("BouncyCastle security provider registered");
    } catch (Throwable t) {
      logger.warn("Failed to register BouncyCastle provider: {}", t.getMessage());
    }
  }

  /**
   * Configure telemetry settings for the driver.
   *
   * <p>By default, out-of-band telemetry is disabled.
   */
  private static void initializeTelemetry() {
    try {
      TelemetryService.disableOOBTelemetry();
      logger.debug("Out-of-band telemetry disabled");
    } catch (Throwable t) {
      logger.warn("Failed to configure telemetry: {}", t.getMessage());
    }
  }

  /**
   * Start asynchronous minicore native library loading.
   *
   * <p>Minicore is loaded in the background so it can overlap with connection setup. The loading
   * result is reported via telemetry during session establishment.
   */
  private static void initializeMinicore() {
    try {
      Minicore.initializeAsync();
      logger.debug("Minicore async initialization started");
    } catch (Throwable t) {
      logger.trace("Failed to start minicore initialization", t);
    }
  }

  // Public accessors for Arrow status

  /**
   * Check if Arrow result format is enabled.
   *
   * @return true if Arrow is enabled, false otherwise
   */
  public static boolean isArrowEnabled() {
    return arrowEnabled;
  }

  /**
   * Get the reason why Arrow was disabled (if applicable).
   *
   * @return error message if Arrow is disabled, null otherwise
   */
  public static String getArrowDisableReason() {
    return arrowDisableReason;
  }

  /**
   * Check if driver has been initialized.
   *
   * @return true if initialized, false otherwise
   */
  public static boolean isInitialized() {
    return initialized;
  }

  static synchronized void resetForTesting() {
    initialized = false;
    arrowEnabled = true;
    arrowDisableReason = null;
  }
}
