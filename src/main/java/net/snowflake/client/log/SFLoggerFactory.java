/*
 * Copyright (c) 2012-2018 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.log;

import java.io.IOException;
import java.net.URL;
import java.util.Enumeration;

/**
 * Used to create SFLogger instance
 *
 * Created by hyu on 11/17/16.
 */
public class SFLoggerFactory
{
  public static LoggerImpl loggerImplementation;

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

  public static SFLogger getLogger(Class<?> clazz)
  {
    // only need to determine the logger implementation only once
    if (loggerImplementation == null)
    {
      String logger = System.getProperty("net.snowflake.jdbc.loggerImpl");

      loggerImplementation = LoggerImpl.fromString(logger);

      if (loggerImplementation == null) {
        // try to load slf4j implementation class first
        loggerImplementation = slf4jImplExist() ? LoggerImpl.SLF4JLOGGER
            : LoggerImpl.JDK14LOGGER;
      }

      if (loggerImplementation == LoggerImpl.JDK14LOGGER)
      {
        JDK14Logger.defaultInit();
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

  private static final String STATIC_LOGGER_BINDER_PATH="org/slf4j/impl/StaticLoggerBinder.class";
  /**
   * Methods used to determine if the slf4j implementation exist
   * or not.
   *
   * The way to determine is to figure out if org.slf4j.StaticLoggerBinder
   * is in the classpath or not. If not, then there is no implementation,
   * switch to JDKLogger by default;
   * @return
   */
  private static boolean slf4jImplExist()
  {
    try
    {
      ClassLoader loggerFactoryClassLoader = SFLoggerFactory.class.getClassLoader();
      Enumeration<URL> paths;
      if (loggerFactoryClassLoader == null)
      {
        paths = ClassLoader.getSystemResources(STATIC_LOGGER_BINDER_PATH);
      }
      else
      {
        paths = loggerFactoryClassLoader.getResources(STATIC_LOGGER_BINDER_PATH);
      }
      return paths.hasMoreElements();
    }
    catch (IOException e)
    {
      System.err.println(e);
    }
    return false;
  }
}
