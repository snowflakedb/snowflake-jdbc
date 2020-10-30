package net.snowflake.client.log;

import static org.junit.Assert.*;

import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class JDK14JCLWrapperLatestIT {
  JDK14JCLWrapper wrapper = new JDK14JCLWrapper(JDK14JCLWrapperLatestIT.class.getName());
  JDK14Logger logger = (JDK14Logger) wrapper.getLogger();

  /** Message last logged using JDK14Logger. */
  private String lastLogMessage = null;

  /** Level at which last message was logged using JDK14Logger. */
  private Level logLevelToRestore = null;

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
      // Assign the log message and its level to the outer class instance
      // variables so that it can see the messages logged
      lastLogMessage = getFormatter().formatMessage(record);
    }

    @Override
    public void flush() {}

    @Override
    public void close() throws SecurityException {}
  }

  /** Logging levels */
  private enum LogLevel {
    ERROR,
    WARN,
    INFO,
    DEBUG,
    TRACE
  }

  private TestJDK14LogHandler handler = new TestJDK14LogHandler(new SFFormatter());

  @Before
  public void setUp() {
    logLevelToRestore = logger.getLevel();
    // Set debug level to lowest so that all possible messages can be sent.
    logger.setLevel(Level.FINEST);
    logger.addHandler(this.handler);
  }

  @After
  public void tearDown() {
    logger.setLevel(logLevelToRestore);
    logger.removeHandler(this.handler);
  }

  String getLoggedMessage() {
    return this.lastLogMessage;
  }

  private void testLogMessagesWithThrowable(LogLevel level, String message, Throwable t) {
    switch (level) {
      case ERROR:
        wrapper.error(message, t);
        break;
      case WARN:
        wrapper.warn(message, t);
        break;
      case INFO:
        wrapper.info(message, t);
        break;
    }
    assertEquals(message, getLoggedMessage());
  }

  private void testLogMessagesNoThrowable(LogLevel level, String message) {
    switch (level) {
      case ERROR:
        wrapper.error(message);
        break;
      case WARN:
        wrapper.warn(message);
        break;
      case INFO:
        wrapper.info(message);
        break;
    }
    assertEquals(message, getLoggedMessage());
  }

  private void testNullLogMessagesWithThrowable(LogLevel level, String message, Throwable t) {
    switch (level) {
      case TRACE:
        wrapper.trace(message, t);
        break;
      case DEBUG:
        wrapper.debug(message, t);
        break;
    }
    assertEquals(null, getLoggedMessage());
  }

  private void testNullLogMessagesNoThrowable(LogLevel level, String message) {
    switch (level) {
      case TRACE:
        wrapper.trace(message);
        break;
      case DEBUG:
        wrapper.debug(message);
        break;
    }
    assertEquals(null, getLoggedMessage());
  }

  /**
   * Test that trace and debug levels never display messages in wrapper (disabled for apache
   * overall). Other 3 levels of error, warn, and info can display logs if debug level is set
   * at/below them.
   */
  @Test
  public void testNullLogMessages() {
    LogLevel[] levelsDisplayingOutput = {LogLevel.ERROR, LogLevel.WARN, LogLevel.INFO};
    LogLevel[] levelsWithNoOutput = {LogLevel.TRACE, LogLevel.DEBUG};
    for (LogLevel level : levelsWithNoOutput) {
      testNullLogMessagesWithThrowable(level, "sample message", null);
      testNullLogMessagesNoThrowable(level, "sample message");
    }
    for (LogLevel level : levelsDisplayingOutput) {
      testLogMessagesWithThrowable(level, "sample message", null);
      testLogMessagesNoThrowable(level, "sample message");
    }
  }

  /**
   * Test that trace and debug are always disabled, while other 3 levels are enabled when debug
   * level is set at/below that level.
   */
  @Test
  public void testEnabledMessaging() {
    assertFalse(wrapper.isTraceEnabled());
    assertFalse(wrapper.isDebugEnabled());
    assertTrue(wrapper.isInfoEnabled());
    assertTrue(wrapper.isErrorEnabled());
    assertTrue(wrapper.isFatalEnabled());
  }
}
