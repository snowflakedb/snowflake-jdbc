/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.log;

import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;

import com.google.common.base.Strings;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import org.apache.commons.logging.LogFactory;

public class SFLoggerUtil {
  private static final String NOT_PROVIDED_LOG = "not provided";
  private static final String PROVIDED_LOG = "provided";

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

  @SnowflakeJdbcInternalApi
  public static <T> String isVariableProvided(T variable) {
    if (variable instanceof String) {
      return (Strings.isNullOrEmpty((String) variable)) ? NOT_PROVIDED_LOG : PROVIDED_LOG;
    }
    return variable == null ? NOT_PROVIDED_LOG : PROVIDED_LOG;
  }
}
