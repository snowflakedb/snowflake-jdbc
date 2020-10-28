package net.snowflake.client.log;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

public class JDK14JCLWrapperIT {
    JDK14JCLWrapper wrapper = new JDK14JCLWrapper(JDK14JCLWrapperIT.class.getName());
    JDK14Logger logger = (JDK14Logger) wrapper.getLogger();

    /** Message last logged using JDK14Logger. */
    private String lastLogMessage = null;

    /** Level at which last message was logged using JDK14Logger. */
    private Level lastLogMessageLevel = null;

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
        logger.setLevel(Level.FINEST);
        logger.addHandler(this.handler);
    }

    @After
    public void tearDown() {
        logger.removeHandler(this.handler);
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

    String getLoggedMessage() {
        return this.lastLogMessage;
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
