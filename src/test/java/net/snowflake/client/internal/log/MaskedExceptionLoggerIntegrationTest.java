package net.snowflake.client.log;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import net.snowflake.client.util.MaskedException;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class MaskedExceptionLoggerIntegrationTest {
  private static final String SECRET = "abcdef";
  private static final String SECRET_MSG = "password=" + SECRET;

  @Test
  public void testJdk14LoggerMasksException() {
    String loggerName = "net.snowflake.client.log.MaskedExceptionLoggerIntegrationTest";
    Logger jul = Logger.getLogger(loggerName);

    // Ensure our handler sees the record.
    Level priorLevel = jul.getLevel();
    boolean priorUseParentHandlers = jul.getUseParentHandlers();
    jul.setLevel(Level.ALL);
    jul.setUseParentHandlers(false);

    AtomicReference<LogRecord> last = new AtomicReference<>();
    Handler handler =
        new Handler() {
          @Override
          public void publish(LogRecord record) {
            last.set(record);
          }

          @Override
          public void flush() {}

          @Override
          public void close() {}
        };

    jul.addHandler(handler);
    try {
      JDK14Logger logger = new JDK14Logger(loggerName);
      logger.error(SECRET_MSG, new Exception(SECRET_MSG));

      LogRecord record = last.get();
      assertNotNull(record);
      assertNotNull(record.getThrown());
      assertTrue(record.getThrown() instanceof MaskedException);

      String rendered = record.getThrown().toString();
      assertNotNull(rendered);
      assertTrue(rendered.contains("****"));
      assertFalse(rendered.contains(SECRET));

      String msg = record.getThrown().getMessage();
      assertNotNull(msg);
      assertTrue(msg.contains("****"));
      assertFalse(msg.contains(SECRET));
    } finally {
      jul.removeHandler(handler);
      jul.setUseParentHandlers(priorUseParentHandlers);
      jul.setLevel(priorLevel);
    }
  }

  @Test
  public void testSlf4jLoggerMasksException() throws Exception {
    SLF4JLogger logger = new SLF4JLogger(MaskedExceptionLoggerIntegrationTest.class.getName());

    org.slf4j.Logger mockLogger = mock(org.slf4j.Logger.class);
    injectSlf4jLogger(logger, mockLogger);

    logger.error(SECRET_MSG, new Exception(SECRET_MSG));

    ArgumentCaptor<Throwable> throwableCaptor = ArgumentCaptor.forClass(Throwable.class);
    verify(mockLogger).error(anyString(), throwableCaptor.capture());
    Throwable thrown = throwableCaptor.getValue();
    assertNotNull(thrown);
    assertTrue(thrown instanceof MaskedException);

    String rendered = thrown.toString();
    assertNotNull(rendered);
    assertTrue(rendered.contains("****"));
    assertFalse(rendered.contains(SECRET));

    String msg = thrown.getMessage();
    assertNotNull(msg);
    assertTrue(msg.contains("****"));
    assertFalse(msg.contains(SECRET));
  }

  private static void injectSlf4jLogger(SLF4JLogger target, org.slf4j.Logger newLogger)
      throws Exception {
    Field loggerField = SLF4JLogger.class.getDeclaredField("slf4jLogger");
    loggerField.setAccessible(true);
    loggerField.set(target, newLogger);

    Field locationAwareField = SLF4JLogger.class.getDeclaredField("isLocationAwareLogger");
    locationAwareField.setAccessible(true);
    locationAwareField.setBoolean(target, false);
  }
}
