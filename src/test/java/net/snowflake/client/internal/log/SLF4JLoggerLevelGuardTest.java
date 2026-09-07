package net.snowflake.client.internal.log;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

/**
 * Tests that SLF4JLogger level guards prevent expensive SecretDetector.maskSecrets() calls when
 * logging is disabled. Regression test for SNOW-3863592.
 */
@Tag(TestTags.CORE)
public class SLF4JLoggerLevelGuardTest {

  private static final String SECRET_MSG =
      "credentials=(aws_key_id='abc123' aws_secret_key='rtyuiop')";

  /**
   * Simulates the SNOW-3863592 scenario: trace(msg, boolean) called in a hot loop with trace
   * disabled (INFO level). The level guard must prevent any call to the underlying logger.
   */
  @Test
  public void testBooleanOverloadsSkippedAtInfoLevel() throws Exception {
    // INFO level: trace and debug disabled, info/warn/error enabled
    SLF4JLogger logger = createLoggerWithMock(false, false, true, true, true);
    Logger mockLogger = getMockLogger(logger);

    logger.trace(SECRET_MSG, true);
    logger.debug(SECRET_MSG, true);

    verify(mockLogger, never()).trace(anyString());
    verify(mockLogger, never()).debug(anyString());
  }

  /** Simulates all levels disabled (OFF). No method should reach the underlying logger. */
  @Test
  public void testAllOverloadsSkippedWhenOff() throws Exception {
    SLF4JLogger logger = createLoggerWithMock(false, false, false, false, false);
    Logger mockLogger = getMockLogger(logger);

    logger.trace(SECRET_MSG, true);
    logger.debug(SECRET_MSG, true);
    logger.info(SECRET_MSG, true);
    logger.warn(SECRET_MSG, true);
    logger.error(SECRET_MSG, true);
    logger.trace(SECRET_MSG, new Exception("x"));
    logger.debug(SECRET_MSG, new Exception("x"));
    logger.info(SECRET_MSG, new Exception("x"));
    logger.warn(SECRET_MSG, new Exception("x"));
    logger.error(SECRET_MSG, new Exception("x"));

    verify(mockLogger, never()).trace(anyString());
    verify(mockLogger, never()).debug(anyString());
    verify(mockLogger, never()).info(anyString());
    verify(mockLogger, never()).error(anyString());
    verify(mockLogger, never()).trace(anyString(), any(Throwable.class));
    verify(mockLogger, never()).debug(anyString(), any(Throwable.class));
    verify(mockLogger, never()).error(anyString(), any(Throwable.class));
  }

  /** When the level IS enabled, masking must still work (no regression). */
  @Test
  public void testMaskingStillWorksWhenEnabled() throws Exception {
    SLF4JLogger logger = createLoggerWithMock(true, true, true, true, true);
    Logger mockLogger = getMockLogger(logger);

    logger.trace(SECRET_MSG, true);

    verify(mockLogger).trace(anyString());
  }

  private SLF4JLogger createLoggerWithMock(
      boolean traceEnabled,
      boolean debugEnabled,
      boolean infoEnabled,
      boolean warnEnabled,
      boolean errorEnabled)
      throws Exception {
    SLF4JLogger logger = new SLF4JLogger(SLF4JLoggerLevelGuardTest.class);
    Logger mockLogger = mock(Logger.class);

    when(mockLogger.isTraceEnabled()).thenReturn(traceEnabled);
    when(mockLogger.isDebugEnabled()).thenReturn(debugEnabled);
    when(mockLogger.isInfoEnabled()).thenReturn(infoEnabled);
    when(mockLogger.isWarnEnabled()).thenReturn(warnEnabled);
    when(mockLogger.isErrorEnabled()).thenReturn(errorEnabled);

    injectSlf4jLogger(logger, mockLogger);
    return logger;
  }

  private Logger getMockLogger(SLF4JLogger logger) throws Exception {
    Field loggerField = SLF4JLogger.class.getDeclaredField("slf4jLogger");
    loggerField.setAccessible(true);
    return (Logger) loggerField.get(logger);
  }

  private static void injectSlf4jLogger(SLF4JLogger target, Logger newLogger) throws Exception {
    Field loggerField = SLF4JLogger.class.getDeclaredField("slf4jLogger");
    loggerField.setAccessible(true);
    loggerField.set(target, newLogger);

    Field locationAwareField = SLF4JLogger.class.getDeclaredField("isLocationAwareLogger");
    locationAwareField.setAccessible(true);
    locationAwareField.setBoolean(target, false);
  }
}
