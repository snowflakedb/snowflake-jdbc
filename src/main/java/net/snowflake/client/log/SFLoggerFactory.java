/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.log;

/**
 * Used to create SFLogger instance
 *
 * Created by hyu on 11/17/16.
 */
public class SFLoggerFactory
{
  private static LoggerImpl loggerImplementation;

  enum LoggerImpl
  {
    SLF4JLOGGER("net.snowflake.client.log.SLF4JLogger"),
    JDK14LOGGER("net.snowflake.client.log.JDK14Logger");

    private String loggerImplClassName;

    LoggerImpl(String loggerClass)
    {
      this.loggerImplClassName = loggerClass;
    }

    public String getLoggerImplClassName()
    {
      return this.loggerImplClassName;
    }

    public static LoggerImpl fromString(String loggerImplClassName)
    {
      if (loggerImplClassName != null)
      {
        for (LoggerImpl imp : LoggerImpl.values())
        {
          if (loggerImplClassName.equalsIgnoreCase(
              imp.getLoggerImplClassName()))
          {
            return imp;
          }
        }
      }
      return null;
    }
  }

  /**
   * @param clazz Class type that the logger is instantiated
   * @return An SFLogger instance given the name of the class
   */
  public static SFLogger getLogger(Class<?> clazz)
  {
    // only need to determine the logger implementation only once
    if (loggerImplementation == null)
    {
      String logger = System.getProperty("net.snowflake.jdbc.loggerImpl");

      loggerImplementation = LoggerImpl.fromString(logger);

      if (loggerImplementation == null) {
        // default to use java util logging
        loggerImplementation = LoggerImpl.JDK14LOGGER;
      }
    }

    switch (loggerImplementation)
    {
      case SLF4JLOGGER:
        return new SLF4JLogger(clazz);
      case JDK14LOGGER:
      default:
        return new JDK14Logger(clazz.getName());
    }
  }
}
