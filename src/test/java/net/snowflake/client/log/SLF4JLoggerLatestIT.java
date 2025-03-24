package net.snowflake.client.log;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.AppenderBase;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.slf4j.LoggerFactory;

/** A class for testing {@link SLF4JLogger} */
@Tag(TestTags.CORE)
public class SLF4JLoggerLatestIT extends AbstractLoggerIT {
  /** {@link SLF4JLogger} instance that will be tested in this class */
  private static final SLF4JLogger LOGGER = new SLF4JLogger(SLF4JLoggerLatestIT.class);

  /**
   * Used for storing the log level set on the logger instance before starting the tests. Once the
   * tests are run, this log level will be restored.
   */
  private static Level logLevelToRestore;

  /**
   * This is the logger instance internally used by the SLF4JLogger instance. LoggerFactory returns
   * the cached instance if an instance by the same name was created before. The name used for
   * creating SLF4JLogger instance is also used here to get the same internal logger instance.
   *
   * <p>SLF4JLogger doesn't expose methods to set logging level and other configurations, hence
   * direct access to the internal logger is required.
   */
  private static final Logger internalLogger =
      (Logger) LoggerFactory.getLogger(SLF4JLoggerLatestIT.class);

  /**
   * Used for storing whether additive property was set on the logger instance before starting the
   * tests. Once the tests are run, additivity will be restored if it was previously enabled.
   *
   * <p>If additivity is enabled, {@link Appender}s in the parent loggers are also invoked.
   */
  private static boolean additivityToRestore = true;

  /**
   * Used for storing any appenders present in the internal logger before starting the tests. They
   * will be restored after running the tests.
   */
  private static final List<Appender<ILoggingEvent>> appendersToRestore = new ArrayList<>();

  /** This appender will be added to the internal logger to get logged messages. */
  private final Appender<ILoggingEvent> testAppender = new TestAppender();

  /** Message last logged using SLF4JLogger. */
  private String lastLogMessage = null;

  /** Level at which last message was logged using SLF4JLogger. */
  private Level lastLogMessageLevel = null;

  @BeforeAll
  public static void oneTimeSetUp() {
    logLevelToRestore = internalLogger.getLevel();
    additivityToRestore = internalLogger.isAdditive();

    appendersToRestore.clear();

    // Get all existing appenders and restore them once testing is done.
    Iterator<Appender<ILoggingEvent>> itr = internalLogger.iteratorForAppenders();
    while (itr.hasNext()) {
      appendersToRestore.add(itr.next());
    }

    // Remove existing appenders to avoid unnecessary logging
    appendersToRestore.forEach(internalLogger::detachAppender);

    // Disable running appenders in parent loggers to avoid unnecessary logging
    internalLogger.setAdditive(false);
  }

  @AfterAll
  public static void oneTimeTearDown() {
    // Restore original configuration
    internalLogger.setLevel(logLevelToRestore);
    internalLogger.setAdditive(additivityToRestore);

    internalLogger.detachAndStopAllAppenders();

    appendersToRestore.forEach(internalLogger::addAppender);
  }

  @BeforeEach
  public void setUp() {
    super.setUp();
    if (!testAppender.isStarted()) {
      testAppender.start();
    }

    internalLogger.addAppender(testAppender);
  }

  @AfterEach
  public void tearDown() {
    internalLogger.detachAppender(testAppender);
  }

  @Override
  void logMessage(LogLevel level, String message, Object... args) {
    switch (level) {
      case ERROR:
        LOGGER.error(message, args);
        break;
      case WARNING:
        LOGGER.warn(message, args);
        break;
      case INFO:
        LOGGER.info(message, args);
        break;
      case DEBUG:
        LOGGER.debug(message, args);
        break;
      case TRACE:
        LOGGER.trace(message, args);
        break;
    }
  }

  @Override
  void logMessage(LogLevel level, String message, Throwable throwable) {
    switch (level) {
      case ERROR:
        LOGGER.error(message, throwable);
        break;
      case WARNING:
        LOGGER.warn(message, throwable);
        break;
      case INFO:
        LOGGER.info(message, throwable);
        break;
      case DEBUG:
        LOGGER.debug(message, throwable);
        break;
      case TRACE:
        LOGGER.trace(message, throwable);
        break;
    }
  }

  @Override
  void logMessage(LogLevel level, String message, boolean isMasked) {
    switch (level) {
      case ERROR:
        LOGGER.error(message, isMasked);
        break;
      case WARNING:
        LOGGER.warn(message, isMasked);
        break;
      case INFO:
        LOGGER.info(message, isMasked);
        break;
      case DEBUG:
        LOGGER.debug(message, isMasked);
        break;
      case TRACE:
        LOGGER.trace(message, isMasked);
        break;
    }
  }

  @Override
  void setLogLevel(LogLevel level) {
    internalLogger.setLevel(toLogBackLevel(level));
  }

  @Override
  String getLoggedMessage() {
    return this.lastLogMessage;
  }

  @Override
  LogLevel getLoggedMessageLevel() {
    return fromLogBackLevel(this.lastLogMessageLevel);
  }

  @Override
  void clearLastLoggedMessageAndLevel() {
    this.lastLogMessage = null;
    this.lastLogMessageLevel = null;
  }

  /** Converts log levels in {@link LogLevel} to appropriate levels in {@link Level}. */
  private Level toLogBackLevel(LogLevel level) {
    switch (level) {
      case ERROR:
        return Level.ERROR;
      case WARNING:
        return Level.WARN;
      case INFO:
        return Level.INFO;
      case DEBUG:
        return Level.DEBUG;
      case TRACE:
        return Level.TRACE;
    }

    return Level.TRACE;
  }

  /** Converts log levels in {@link Level} to appropriate levels in {@link LogLevel}. */
  private LogLevel fromLogBackLevel(Level level) {
    if (Level.ERROR.equals(level)) {
      return LogLevel.ERROR;
    }
    if (Level.WARN.equals(level)) {
      return LogLevel.WARNING;
    }
    if (Level.INFO.equals(level)) {
      return LogLevel.INFO;
    }
    if (Level.DEBUG.equals(level)) {
      return LogLevel.DEBUG;
    }
    if (Level.TRACE.equals(level) || Level.ALL.equals(level)) {
      return LogLevel.TRACE;
    }

    throw new IllegalArgumentException(
        String.format("Specified log level '%s' not supported", level.toString()));
  }

  /** An appender that will be used for getting messages logged by a {@link Logger} instance */
  private class TestAppender extends AppenderBase<ILoggingEvent> {
    @Override
    public void append(ILoggingEvent event) {
      // Assign the log message and it's level to the outer class instance
      // variables so that it can see the messages logged
      lastLogMessage = event.getFormattedMessage();
      lastLogMessageLevel = event.getLevel();
    }
  }
}
