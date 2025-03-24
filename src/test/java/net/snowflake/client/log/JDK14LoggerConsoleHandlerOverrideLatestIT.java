package net.snowflake.client.log;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Properties;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.jdbc.BaseJDBCTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.CORE)
public class JDK14LoggerConsoleHandlerOverrideLatestIT extends BaseJDBCTest {
  private static final PrintStream standardOut = System.out;
  private static final PrintStream standardErr = System.err;
  private static final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
  private static final ByteArrayOutputStream errStream = new ByteArrayOutputStream();
  private static final String errorMessage = "error message 1";
  private static final String warningMessage = "warning message 1";
  private static final String infoMessage = "info message 1";
  private static final String debugMessage = "debug message 1";

  @BeforeAll
  public static void replaceStreams() {
    System.setOut(new PrintStream(outputStream));
    System.setErr(new PrintStream(errStream));
  }

  @AfterAll
  public static void resetStreams() {
    System.setOut(standardOut);
    System.setErr(standardErr);
  }

  /** Added in > 3.20.0 */
  @Test
  public void shouldLogAllToStdErr() throws Exception {
    Properties paramProperties = new Properties();

    connectAndLog(paramProperties);

    Handler[] handlers = Logger.getLogger("").getHandlers();
    assertTrue(handlers.length > 0);
    assertTrue(Arrays.stream(handlers).anyMatch(h -> h instanceof ConsoleHandler));

    System.out.flush();
    System.err.flush();
    assertEquals("", outputStream.toString());
    // overriding stderr does not work correctly with maven
    // String errString = errStream.toString();
    // assertTrue(errString.contains(errorMessage), () -> "STDERR: " + errString);
    // assertTrue(errString.contains(warningMessage), () -> "STDERR: " + errString);
    // assertTrue(errString.contains(infoMessage), () -> "STDERR: " + errString);
    // assertFalse(errString.contains(debugMessage), () -> "STDERR: " + errString);
  }

  /** Added in > 3.20.0 */
  @Test
  public void shouldOverrideConsoleLoggerToStdOut() throws Exception {
    Properties paramProperties = new Properties();
    paramProperties.put("JAVA_LOGGING_CONSOLE_STD_OUT", true);

    connectAndLog(paramProperties);

    Handler[] handlers = Logger.getLogger("").getHandlers();
    assertTrue(handlers.length > 0);
    assertFalse(Arrays.stream(handlers).anyMatch(h -> h instanceof ConsoleHandler));
    assertTrue(Arrays.stream(handlers).anyMatch(h -> h instanceof StdOutConsoleHandler));

    System.out.flush();
    System.err.flush();
    String outString = outputStream.toString();
    assertTrue(outString.contains(errorMessage), () -> "STDOUT: " + outString);
    assertTrue(outString.contains(warningMessage), () -> "STDOUT: " + outString);
    assertTrue(outString.contains(infoMessage), () -> "STDOUT: " + outString);
    assertFalse(outString.contains(debugMessage), () -> "STDOUT: " + outString);
    // overriding stderr does not work correctly with maven
    // assertEquals("", errStream.toString());
  }

  /** Added in > 3.20.0 */
  @Test
  public void shouldOverrideConsoleLoggerWithSpecificThreshold() throws Exception {
    Properties paramProperties = new Properties();
    paramProperties.put("JAVA_LOGGING_CONSOLE_STD_OUT", true);
    paramProperties.put("JAVA_LOGGING_CONSOLE_STD_OUT_THRESHOLD", "WARNING");

    connectAndLog(paramProperties);

    Handler[] handlers = Logger.getLogger("").getHandlers();
    assertTrue(handlers.length > 0);
    assertFalse(Arrays.stream(handlers).anyMatch(h -> h instanceof ConsoleHandler));
    assertTrue(
        Arrays.stream(handlers)
            .anyMatch(
                h ->
                    h instanceof StdErrOutThresholdAwareConsoleHandler
                        && ((StdErrOutThresholdAwareConsoleHandler) h)
                            .getThreshold()
                            .equals(Level.WARNING)));

    System.out.flush();
    System.err.flush();
    String outString = outputStream.toString();
    assertFalse(outString.contains(errorMessage), () -> "STDOUT: " + outString);
    assertTrue(outString.contains(warningMessage), () -> "STDOUT: " + outString);
    assertTrue(outString.contains(infoMessage), () -> "STDOUT: " + outString);
    assertFalse(outString.contains(debugMessage), () -> "STDOUT: " + outString);
    // overriding stderr does not work correctly with maven
    // String errString = errStream.toString();
    // assertTrue(errString.contains(errorMessage), () -> "STDERR: " + errString);
    // assertFalse(errString.contains(warningMessage), () -> "STDERR: " + errString);
    // assertFalse(errString.contains(infoMessage), () -> "STDERR: " + errString);
    // assertFalse(errString.contains(debugMessage), () -> "STDERR: " + errString);
  }

  private static void connectAndLog(Properties paramProperties) throws SQLException {
    try (Connection con = getConnection(paramProperties)) {
      SFLogger logger = SFLoggerFactory.getLogger(JDK14LoggerConsoleHandlerOverrideLatestIT.class);
      logger.error(errorMessage);
      logger.warn(warningMessage);
      logger.info(infoMessage);
      logger.debug(debugMessage);
    }
  }

  /** Added in > 3.20.0 */
  @Test
  public void shouldThrowExceptionOnUnknownLevel() throws Exception {
    Properties paramProperties = new Properties();
    paramProperties.put("JAVA_LOGGING_CONSOLE_STD_OUT", true);
    paramProperties.put("JAVA_LOGGING_CONSOLE_STD_OUT_THRESHOLD", "UNKNOWN");
    assertThrows(UnknownJavaUtilLoggingLevelException.class, () -> getConnection(paramProperties));
  }

  @BeforeEach
  public void reset() {
    JDK14Logger.resetToDefaultConsoleHandler();
    outputStream.reset();
    errStream.reset();
  }
}
