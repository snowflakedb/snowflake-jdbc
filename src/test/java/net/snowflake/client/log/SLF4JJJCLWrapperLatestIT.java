package net.snowflake.client.log;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.AppenderBase;
import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.CORE)
public class SLF4JJJCLWrapperLatestIT {

  /** Message last logged using SLF4JLogger. */
  private String lastLogMessage = null;

  /** Level of logging to restore at the end of testing. Most likely null. */
  private Level levelToRestore = null;

  /** An appender that will be used for getting messages logged by a {@link Logger} instance */
  private class TestAppender extends AppenderBase<ILoggingEvent> {
    @Override
    public void append(ILoggingEvent event) {
      // Assign the log message and it's level to the outer class instance
      // variables so that it can see the messages logged
      lastLogMessage = event.getFormattedMessage();
    }
  }

  String getLoggedMessage() {
    return this.lastLogMessage;
  }

  /** Logging levels */
  private enum LogLevel {
    FATAL,
    ERROR,
    WARN,
    INFO,
    DEBUG,
    TRACE
  }

  SLF4JJCLWrapper wrapper = new SLF4JJCLWrapper(SLF4JJJCLWrapperLatestIT.class.getName());
  Logger logger = (Logger) wrapper.getLogger();
  private final Appender<ILoggingEvent> testAppender = new TestAppender();

  @BeforeEach
  public void setUp() {
    levelToRestore = logger.getLevel();
    if (!testAppender.isStarted()) {
      testAppender.start();
    }
    // Set debug level to lowest possible level so all messages can be recorded.
    logger.setLevel(Level.TRACE);
    logger.addAppender(testAppender);
  }

  @AfterEach
  public void tearDown() {
    logger.setLevel(levelToRestore);
    logger.detachAppender(testAppender);
  }

  // helper function, throwables allowed
  private void testNullLogMessagesWithThrowable(LogLevel level, String message, Throwable t) {
    switch (level) {
      case FATAL:
        wrapper.fatal(message, t);
        break;
      case ERROR:
        wrapper.error(message, t);
        break;
      case WARN:
        wrapper.warn(message, t);
        break;
      case INFO:
        wrapper.info(message, t);
        break;
      case DEBUG:
        wrapper.debug(message, t);
        break;
      case TRACE:
        wrapper.trace(message, t);
        break;
    }
    assertEquals(null, getLoggedMessage());
  }

  // helper function, no throwables
  private void testNullLogMessagesNoThrowable(LogLevel level, String message) {
    switch (level) {
      case FATAL:
        wrapper.fatal(message);
        break;
      case ERROR:
        wrapper.error(message);
        break;
      case WARN:
        wrapper.warn(message);
        break;
      case INFO:
        wrapper.info(message);
        break;
      case DEBUG:
        wrapper.debug(message);
        break;
      case TRACE:
        wrapper.trace(message);
        break;
    }
    assertEquals(null, getLoggedMessage());
  }

  /** Test that all levels are disabled for wrapper class. No messages returned at any level. */
  @Test
  public void testNullLogMessages() {
    for (LogLevel level : LogLevel.values()) {
      testNullLogMessagesWithThrowable(level, "sample message", null);
      testNullLogMessagesNoThrowable(level, "sample message");
    }
  }

  /**
   * Test that tracing and debugging are disabled for apache at all times. With other levels, pass
   * in results from the actual logger to see if messages can be enabled.
   */
  @Test
  public void testEnabledMessaging() {
    assertFalse(wrapper.isTraceEnabled());
    assertFalse(wrapper.isDebugEnabled());
    assertTrue(wrapper.isInfoEnabled());
    assertTrue(wrapper.isWarnEnabled());
    assertTrue(wrapper.isErrorEnabled());
    assertTrue(wrapper.isFatalEnabled());
  }
}
