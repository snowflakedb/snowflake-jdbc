package net.snowflake.client.log;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.logging.Level;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class BufferingLoggerTest {

  private ListHandler listHandler;
  private java.util.logging.Logger julLogger;

  @BeforeEach
  public void reset() throws NoSuchFieldException, IllegalAccessException {
    // Reset the state of the Snowflake log factory to its pre-initialized, buffering state
    Field f = SFLoggerFactory.class.getDeclaredField("MODE");
    f.setAccessible(true);
    f.set(null, SFLoggerFactory.Mode.BUFFERING);

    Field f2 = SFLoggerFactory.class.getDeclaredField("registeredLoggers");
    f2.setAccessible(true);
    ((java.util.List<?>) f2.get(null)).clear();

    // Reset the JDK logger to its pre-initialized state
    Field f3 = JDK14Logger.class.getDeclaredField("isLoggerInit");
    f3.setAccessible(true);
    f3.set(null, false);

    // Set up the java.util.logging.Logger for the test
    julLogger = java.util.logging.Logger.getLogger("net.snowflake.client");
    listHandler = new ListHandler();
    julLogger.addHandler(listHandler);
    julLogger.setLevel(Level.INFO);
    julLogger.setUseParentHandlers(false);
  }

  @Test
  public void testBuffering() throws IOException {
    SFLogger logger = SFLoggerFactory.getLogger(BufferingLoggerTest.class);

    logger.info("info message");
    logger.debug("debug message");

    SFLoggerFactory.reconfigure();

    assertEquals(1, listHandler.records.size());
    assertEquals("info message", listHandler.records.get(0).getMessage());
  }

  @Test
  public void testNoBuffering() throws IOException {
    SFLoggerFactory.reconfigure();
    SFLogger logger = SFLoggerFactory.getLogger(BufferingLoggerTest.class);

    logger.info("info message");
    logger.debug("debug message");

    assertEquals(1, listHandler.records.size());
    assertEquals("info message", listHandler.records.get(0).getMessage());
  }

  @Test
  public void testMultipleLoggers() throws IOException {
    SFLogger logger1 = SFLoggerFactory.getLogger("net.snowflake.client.logger1");
    SFLogger logger2 = SFLoggerFactory.getLogger("net.snowflake.client.logger2");

    logger1.info("info message 1");
    logger1.warn("warn message 1");
    logger2.info("info message 2");

    SFLoggerFactory.reconfigure();

    assertEquals(3, listHandler.records.size());
    assertEquals("info message 1", listHandler.records.get(0).getMessage());
    assertEquals("warn message 1", listHandler.records.get(1).getMessage());
    assertEquals("info message 2", listHandler.records.get(2).getMessage());
  }
}
