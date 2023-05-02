/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.log;

import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;

import org.apache.commons.logging.LogFactory;

public class SFLoggerUtil {
  public static void initializeSnowflakeLogger() {
    String logger = systemGetProperty("net.snowflake.jdbc.loggerImpl");
    SFLoggerFactory.LoggerImpl loggerImplementation = SFLoggerFactory.LoggerImpl.fromString(logger);
    if (loggerImplementation == null) {
      loggerImplementation = SFLoggerFactory.LoggerImpl.JDK14LOGGER;
    }

    System.setProperty(
        "org.apache.commons.logging.LogFactory", "org.apache.commons.logging.impl.LogFactoryImpl");
    LogFactory logFactory = LogFactory.getFactory();
    switch (loggerImplementation) {
      case SLF4JLOGGER:
        logFactory.setAttribute(
            "org.apache.commons.logging.Log", "net.snowflake.client.log.SLF4JJCLWrapper");
        break;
      case JDK14LOGGER:
      default:
        logFactory.setAttribute(
            "org.apache.commons.logging.Log", "net.snowflake.client.log.JDK14JCLWrapper");
    }
  }
}
