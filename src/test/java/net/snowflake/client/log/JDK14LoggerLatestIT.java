package net.snowflake.client.log;

import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;

/** A class for testing {@link JDK14Logger} */
@Tag(TestTags.CORE)
public class JDK14LoggerLatestIT extends AbstractLoggerIT {
  /** {@link JDK14Logger} instance that will be tested in this class */
  private static final JDK14Logger LOGGER = new JDK14Logger(JDK14LoggerLatestIT.class.getName());

  /**
   * Used for storing the log level set on the logger instance before starting the tests. Once the
   * tests are run, this log level will be restored.
   */
  private static Level logLevelToRestore;

  /**
   * This is the logger instance internally used by the JDK14Logger instance. Logger.getLogger()
   * returns the cached instance if an instance by the same name was created before. The name used
   * for creating JDK14Logger instance is also used here to get the same internal logger instance.
   *
   * <p>JDK14Logger doesn't expose methods to add handlers and set other configurations, hence
   * direct access to the internal logger is required.
   */
  private static final Logger internalLogger =
      Logger.getLogger(JDK14LoggerLatestIT.class.getName());

  /**
   * Used for storing whether the parent logger handlers were run in the logger instance before
   * starting the tests. Once the tests are run, the behavior will be restored.
   */
  private static boolean useParentHandlersToRestore = true;

  /** This handler will be added to the internal logger to get logged messages. */
  private TestJDK14LogHandler handler = new TestJDK14LogHandler(new SFFormatter());

  /** Message last logged using JDK14Logger. */
  private String lastLogMessage = null;

  /** Level at which last message was logged using JDK14Logger. */
  private Level lastLogMessageLevel = null;

  @BeforeAll
  public static void oneTimeSetUp() {
    logLevelToRestore = internalLogger.getLevel();
    useParentHandlersToRestore = internalLogger.getUseParentHandlers();

    internalLogger.setUseParentHandlers(false);
  }

  @AfterAll
  public static void oneTimeTearDown() {
    internalLogger.setLevel(logLevelToRestore);
    internalLogger.setUseParentHandlers(useParentHandlersToRestore);
  }

  @BeforeEach
  public void setUp() {
    super.setUp();
    internalLogger.addHandler(this.handler);
  }

  @AfterEach
  public void tearDown() {
    internalLogger.removeHandler(this.handler);
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
  void setLogLevel(LogLevel level) {
    internalLogger.setLevel(toJavaCoreLoggerLevel(level));
  }

  @Override
  String getLoggedMessage() {
    return this.lastLogMessage;
  }

  @Override
  LogLevel getLoggedMessageLevel() {
    return fromJavaCoreLoggerLevel(this.lastLogMessageLevel);
  }

  @Override
  void clearLastLoggedMessageAndLevel() {
    this.lastLogMessage = null;
    this.lastLogMessageLevel = null;
  }

  /** Converts log levels in {@link LogLevel} to appropriate levels in {@link Level}. */
  private Level toJavaCoreLoggerLevel(LogLevel level) {
    switch (level) {
      case ERROR:
        return Level.SEVERE;
      case WARNING:
        return Level.WARNING;
      case INFO:
        return Level.INFO;
      case DEBUG:
        return Level.FINE;
      case TRACE:
        return Level.FINEST;
    }

    return Level.FINEST;
  }

  /** Converts log levels in {@link Level} to appropriate levels in {@link LogLevel}. */
  private LogLevel fromJavaCoreLoggerLevel(Level level) {
    if (Level.SEVERE.equals(level)) {
      return LogLevel.ERROR;
    }
    if (Level.WARNING.equals(level)) {
      return LogLevel.WARNING;
    }
    if (Level.INFO.equals(level)) {
      return LogLevel.INFO;
    }
    if (Level.FINE.equals(level) || Level.FINER.equals(level)) {
      return LogLevel.DEBUG;
    }
    if (Level.FINEST.equals(level) || Level.ALL.equals(level)) {
      return LogLevel.TRACE;
    }

    throw new IllegalArgumentException(
        String.format("Specified log level '%s' not supported", level.toString()));
  }

  /** An handler that will be used for getting messages logged by a {@link Logger} instance */
  private class TestJDK14LogHandler extends Handler {
    /**
     * Creates an instance of {@link TestJDK14LogHandler}
     *
     * @param formatter Formatter that will be used for formatting log records in this handler
     *     instance
     */
    TestJDK14LogHandler(Formatter formatter) {
      super();
      super.setFormatter(formatter);
    }

    @Override
    public void publish(LogRecord record) {
      // Assign the log message and it's level to the outer class instance
      // variables so that it can see the messages logged
      lastLogMessage = getFormatter().formatMessage(record);
      lastLogMessageLevel = record.getLevel();
    }

    @Override
    public void flush() {}

    @Override
    public void close() {}
  }
}
