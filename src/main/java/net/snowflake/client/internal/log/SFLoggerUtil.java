package net.snowflake.client.internal.log;

import static net.snowflake.client.internal.jdbc.SnowflakeUtil.isNullOrEmpty;
import static net.snowflake.client.internal.jdbc.SnowflakeUtil.systemGetProperty;

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

    CommonsLoggingWrapperMode commonsLoggingWrapperMode = CommonsLoggingWrapperMode.detect();
    if (commonsLoggingWrapperMode == CommonsLoggingWrapperMode.OFF) {
      return;
    }

    System.setProperty(
        "org.apache.commons.logging.LogFactory", "org.apache.commons.logging.impl.LogFactoryImpl");
    LogFactory logFactory = LogFactory.getFactory();
    if (commonsLoggingWrapperMode == CommonsLoggingWrapperMode.ALL) {
      logFactory.setAttribute(
          "org.apache.commons.logging.Log", CommonsLoggingWrapper.class.getName());
      return;
    }
    switch (loggerImplementation) {
      case SLF4JLOGGER:
        logFactory.setAttribute(
            "org.apache.commons.logging.Log", "net.snowflake.client.internal.log.SLF4JJCLWrapper");
        break;
      case JDK14LOGGER:
      default:
        logFactory.setAttribute(
            "org.apache.commons.logging.Log", "net.snowflake.client.internal.log.JDK14JCLWrapper");
    }
  }

  public static <T> String isVariableProvided(T variable) {
    if (variable instanceof String) {
      return (isNullOrEmpty((String) variable)) ? NOT_PROVIDED_LOG : PROVIDED_LOG;
    }
    return variable == null ? NOT_PROVIDED_LOG : PROVIDED_LOG;
  }
}
