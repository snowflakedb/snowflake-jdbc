package net.snowflake.client.log;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.AppenderBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class SLF4JJJCLWrapperIT {

    /** Message last logged using SLF4JLogger. */
    private String lastLogMessage = null;

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
        ERROR,
        WARN,
        INFO,
        DEBUG,
        TRACE
    }

    SLF4JJCLWrapper wrapper = new SLF4JJCLWrapper(SLF4JJJCLWrapperIT.class.getName());
    Logger logger = (Logger) wrapper.getLogger();
    private final Appender<ILoggingEvent> testAppender = new TestAppender();

    @Before
    public void setUp() {
        if (!testAppender.isStarted()) {
            testAppender.start();
        }
        logger.setLevel(Level.TRACE);
        logger.addAppender(testAppender);
    }

    @After
    public void tearDown() {
        logger.detachAppender(testAppender);
    }

    @Test
    public void testEnabledMessaging()
    {
        assertFalse(wrapper.isTraceEnabled());
        assertFalse(wrapper.isDebugEnabled());
        assertTrue(wrapper.isInfoEnabled());
        assertTrue(wrapper.isErrorEnabled());
        assertTrue(wrapper.isFatalEnabled());
    }

    private void testNullLogMessagesWithThrowable(LogLevel level, String message, Throwable t)
    {
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
            case DEBUG:
                wrapper.debug(message, t);
                break;
            case TRACE:
                wrapper.trace(message, t);
                break;
        }
        assertEquals(null, getLoggedMessage());
    }

    private void testNullLogMessagesNoThrowable(LogLevel level, String message)
    {
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
            case DEBUG:
                wrapper.debug(message);
                break;
            case TRACE:
                wrapper.trace(message);
                break;
        }
        assertEquals(null, getLoggedMessage());
    }

    @Test
    public void testNullLogMessages()
    {
        for (LogLevel level: LogLevel.values())
        {
            testNullLogMessagesWithThrowable(level, "sample message", null);
            testNullLogMessagesNoThrowable(level, "sample message");
        }
    }
}
