/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.log;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.jdbc.BaseJDBCTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.CORE)
public class JDK14LoggerConsoleHandlerOverrideLatestIT extends BaseJDBCTest {
  /** Added in > 3.20.0 */
  @Test
  public void shouldOverrideConsoleLoggerToStdOut() throws Exception {
    Properties paramProperties = new Properties();
    paramProperties.put("JAVA_LOGGING_CONSOLE_STD_OUT", true);
    try (Connection connection = getConnection(paramProperties);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("select 1")) {
      assertTrue(resultSet.next());
      Handler[] handlers = Logger.getLogger("").getHandlers();
      assertTrue(handlers.length > 0);
      assertFalse(Arrays.stream(handlers).anyMatch(h -> h instanceof ConsoleHandler));
      assertTrue(Arrays.stream(handlers).anyMatch(h -> h instanceof StdOutConsoleHandler));
    }
  }

  /** Added in > 3.20.0 */
  @Test
  public void shouldOverrideConsoleLoggerWithSpecificThreshold() throws Exception {
    Properties paramProperties = new Properties();
    paramProperties.put("JAVA_LOGGING_CONSOLE_STD_OUT", true);
    paramProperties.put("JAVA_LOGGING_CONSOLE_STD_OUT_THRESHOLD", "FINE");
    try (Connection connection = getConnection(paramProperties);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("select 1")) {
      assertTrue(resultSet.next());
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
                              .equals(Level.FINE)));
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
  }
}
