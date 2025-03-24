package net.snowflake.client.log;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** A base class for testing implementations of {@link SFLogger} */
@Tag(TestTags.CORE)
public abstract class AbstractLoggerIT {
  public static final String fakeCreds =
      "credentials=(aws_key_id='abc123' aws_secret_key='rtyuiop')";

  @BeforeEach
  void setUp() {
    setLogLevel(LogLevel.TRACE);
  }

  /**
   * Message logging will be skipped if the message level is below the level set for the logger.
   * This method tests that the lambda arguments provided along with the message format are not
   * evaluated if logging is skipped.
   */
  @Test
  public void TestLambdaIsNotEvaluatedIfMsgIsNotLogged() {
    setLogLevel(LogLevel.ERROR);

    logMessage(
        LogLevel.TRACE,
        "Value: {}",
        (ArgSupplier)
            () -> {
              fail("Lambda expression evaluated even though message is not logged");
              return 0;
            });
  }

  @Test
  public void TestWithSingleLambdaArg() {
    logAndVerifyAtEachLogLevel("Value: 5", "Value: {}", (ArgSupplier) () -> 2 + 3);
  }

  @Test
  public void TestWithMultipleLambdaArgs() {
    int a = 2, b = 3, c = 5;

    logAndVerifyAtEachLogLevel(
        String.format("Sum of %s and %s is %s", a, b + c, a + b + c),
        "Sum of {} and {} is {}",
        a,
        (ArgSupplier) () -> b + c,
        (ArgSupplier) () -> a + b + c);
  }

  @Test
  public void TestWithNullArgs() {
    logAndVerifyAtEachLogLevel(
        "Values are null and null", "Values are {} and {}", null, (ArgSupplier) () -> null);
  }

  @Test
  public void TestWithIsMaskedTrue() {
    for (LogLevel level : LogLevel.values()) {
      logMessage(level, fakeCreds, true);
      String loggedMsg = getLoggedMessage();
      String expectedMessage = "credentials=(aws_key_id='****' aws_secret_key='****')";
      assertEquals(expectedMessage, loggedMsg);
    }
  }

  @Test
  public void TestWithIsMaskedFalse() {
    for (LogLevel level : LogLevel.values()) {
      logMessage(level, fakeCreds, false);
      String loggedMsg = getLoggedMessage();
      // message doesn't change since it's not masked
      assertEquals(fakeCreds, loggedMsg);
    }
  }

  @Test
  public void testWithThrowable() {
    for (LogLevel level : LogLevel.values()) {
      logMessage(level, "sample message", (Throwable) null);
    }
  }

  /**
   * Logs the given message format and its arguments at each of the logging levels and verifies that
   * the logger logged the message correctly.
   */
  private void logAndVerifyAtEachLogLevel(String expectedLogMsg, String msg, Object... args) {
    for (LogLevel level : LogLevel.values()) {
      clearLastLoggedMessageAndLevel();

      logMessage(level, msg, args);

      String loggedMsg = getLoggedMessage();
      assertEquals(
          expectedLogMsg,
          loggedMsg,
          String.format(
              "Message logged did not match expected value. " + "expected=%s actual=%s",
              expectedLogMsg, loggedMsg));

      LogLevel loggedMsgLevel = getLoggedMessageLevel();
      assertEquals(
          level,
          loggedMsgLevel,
          String.format(
              "Message was not logged at expected log level. " + "expected=%s actual=%s",
              level.toString(), loggedMsgLevel.toString()));
    }
  }

  /**
   * Log message at the given level.
   *
   * @param level level at which the message is to be logged
   * @param message message or message format
   * @param args values for placeholders in the message format
   */
  abstract void logMessage(LogLevel level, String message, Object... args);

  /**
   * Log message at the given level.
   *
   * @param level level at which the message is to be logged
   * @param message message or message format
   * @param isMasked for masking secrets
   */
  abstract void logMessage(LogLevel level, String message, boolean isMasked);

  /**
   * Log message at the given level.
   *
   * @param level level at which the message is to be logged
   * @param message message or message format
   * @param throwable for exception thrown
   */
  abstract void logMessage(LogLevel level, String message, Throwable throwable);

  /**
   * Set minimum log level on the logger instance at which a message will be accepted.
   *
   * @param level Minimum log level
   */
  abstract void setLogLevel(LogLevel level);

  /** Gets message last logged by the logger instance */
  abstract String getLoggedMessage();

  /** Gets level at which the last message was logged. */
  abstract LogLevel getLoggedMessageLevel();

  /** Clears cached last logged message and its level. */
  abstract void clearLastLoggedMessageAndLevel();

  /** Logging levels */
  protected enum LogLevel {
    ERROR,
    WARNING,
    INFO,
    DEBUG,
    TRACE
  }
}
