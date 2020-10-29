/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.log;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * A base class for testing implementations of {@link SFLogger}
 */
public abstract class AbstractLoggerIT
{
  @Before
  void setUp()
  {
    setLogLevel(LogLevel.TRACE);
  }

  /**
   * Message logging will be skipped if the message level is below the level
   * set for the logger. This method tests that the lambda arguments provided
   * along with the message format are not evaluated if logging is skipped.
   */
  @Test
  public void TestLambdaIsNotEvaluatedIfMsgIsNotLogged()
  {
    setLogLevel(LogLevel.ERROR);

    logMessage(
        LogLevel.TRACE,
        "Value: {}",
        (ArgSupplier) () ->
        {
          Assert.fail("Lambda expression evaluated even though message " +
                      "is not logged");
          return 0;
        });
  }

  @Test
  public void TestWithSingleLambdaArg()
  {
    logAndVerifyAtEachLogLevel(
        "Value: 5",
        "Value: {}",
        (ArgSupplier) () -> 2 + 3);
  }

  @Test
  public void TestWithMultipleLambdaArgs()
  {
    int a = 2, b = 3, c = 5;

    logAndVerifyAtEachLogLevel(
        String.format("Sum of %s and %s is %s", a, b + c, a + b + c),
        "Sum of {} and {} is {}",
        a,
        (ArgSupplier) () -> b + c,
        (ArgSupplier) () -> a + b + c);
  }

  @Test
  public void TestWithNullArgs()
  {
    logAndVerifyAtEachLogLevel(
        "Values are null and null",
        "Values are {} and {}",
        null,
        (ArgSupplier) () -> null);
  }

  /**
   * Logs the given message format and its arguments at each of the logging
   * levels and verifies that the logger logged the message correctly.
   */
  private void logAndVerifyAtEachLogLevel(
      String expectedLogMsg,
      String msg,
      Object... args)
  {
    for (LogLevel level : LogLevel.values())
    {
      clearLastLoggedMessageAndLevel();

      logMessage(level, msg, args);

      String loggedMsg = getLoggedMessage();
      Assert.assertEquals(
          String.format("Message logged did not match expected value. " +
                        "expected=%s actual=%s", expectedLogMsg, loggedMsg),
          expectedLogMsg,
          loggedMsg);

      LogLevel loggedMsgLevel = getLoggedMessageLevel();
      Assert.assertEquals(
          String.format("Message was not logged at expected log level. " +
                        "expected=%s actual=%s", level.toString(), loggedMsgLevel.toString()),
          level,
          loggedMsgLevel);
    }
  }

  /**
   * Log message at the given level.
   *
   * @param level   level at which the message is to be logged
   * @param message message or message format
   * @param args    values for placeholders in the message format
   */
  abstract void logMessage(LogLevel level, String message, Object... args);

  /**
   * Set minimum log level on the logger instance at which a message will be
   * accepted.
   *
   * @param level Minimum log level
   */
  abstract void setLogLevel(LogLevel level);

  /**
   * Gets message last logged by the logger instance
   */
  abstract String getLoggedMessage();

  /**
   * Gets level at which the last message was logged.
   */
  abstract LogLevel getLoggedMessageLevel();

  /**
   * Clears cached last logged message and its level.
   */
  abstract void clearLastLoggedMessageAndLevel();

  /**
   * Logging levels
   */
  protected enum LogLevel
  {
    ERROR,
    WARNING,
    INFO,
    DEBUG,
    TRACE
  }
}
