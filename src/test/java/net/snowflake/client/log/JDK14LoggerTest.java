/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.log;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.logging.Level;
import org.junit.Test;

public class JDK14LoggerTest {

  @Test
  public void testLegacyLoggerInit() {
    System.setProperty("snowflake.jdbc.log.size", "100000");
    System.setProperty("snowflake.jdbc.log.count", "3");
    System.setProperty("net.snowflake.jdbc.loggerImpl", "net.snowflake.client.log.JDK14Logger");

    JDK14Logger logger = new JDK14Logger(JDK14LoggerTest.class.getName());
    assertFalse(logger.isDebugEnabled());
    assertTrue(logger.isInfoEnabled());

    String level = "all";
    Level tracingLevel = Level.parse(level.toUpperCase());
    JDK14Logger.honorTracingParameter(tracingLevel);
    assertTrue(logger.isDebugEnabled());
  }
}
