/*
 * Copyright (c) 2012-2018 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.log;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.logging.Handler;
import java.util.logging.StreamHandler;

/**
 * Use java.util.logging to implements SFLogger.
 *
 * Log Level mapping from SFLogger to java.util.logging:
 * ERROR -- SEVERE
 * WARN  -- WARNING
 * INFO  -- INFO
 * DEBUG -- FINE
 * TRACE -- FINEST
 *
 * Created by hyu on 11/17/16.
 */
public class JDK14Logger implements SFLogger
{
  private Logger jdkLogger;

  private Set<String> logMethods = new HashSet<>(Arrays.asList(
      "debug", "error", "info", "trace", "warn"));

  public JDK14Logger(String name)
  {
    this.jdkLogger = Logger.getLogger(name);
  }

  public boolean isDebugEnabled()
  {
    return this.jdkLogger.isLoggable(Level.FINE);
  }

  public boolean isErrorEnabled()
  {
    return this.jdkLogger.isLoggable(Level.SEVERE);
  }

  public boolean isInfoEnabled()
  {
    return this.jdkLogger.isLoggable(Level.INFO);
  }

  public boolean isTraceEnabled()
  {
    return this.jdkLogger.isLoggable(Level.FINEST);
  }

  public boolean isWarnEnabled()
  {
    return this.jdkLogger.isLoggable(Level.WARNING);
  }

  public void debug(String msg)
  {
    logInternal(Level.FINE, msg);
  }

  public void debug(String msg, Object... arguments)
  {
    logInternal(Level.FINE, msg, arguments);
  }

  public void debug(String msg, Throwable t)
  {
    logInternal(Level.FINE, msg, t);
  }

  public void error(String msg)
  {
    logInternal(Level.SEVERE, msg);
  }

  public void error(String msg, Object... arguments)
  {
    logInternal(Level.SEVERE, msg, arguments);
  }

  public void error(String msg, Throwable t)
  {
    logInternal(Level.SEVERE, msg, t);
  }

  public void info(String msg)
  {
    logInternal(Level.INFO, msg);
  }

  public void info(String msg, Object... arguments)
  {
    logInternal(Level.INFO, msg, arguments);
  }

  public void info(String msg, Throwable t)
  {
    logInternal(Level.INFO, msg, t);
  }

  public void trace(String msg)
  {
    logInternal(Level.FINEST, msg);
  }

  public void trace(String msg, Object... arguments)
  {
    logInternal(Level.FINEST, msg, arguments);
  }

  public void trace(String msg, Throwable t)
  {
    logInternal(Level.FINEST, msg, t);
  }

  public void warn(String msg)
  {
    logInternal(Level.WARNING, msg);
  }

  public void warn(String msg, Object... arguments)
  {
    logInternal(Level.WARNING, msg, arguments);
  }

  public void warn(String msg, Throwable t)
  {
    logInternal(Level.WARNING, msg, t);
  }

  private void logInternal(Level level, String msg)
  {
    if (jdkLogger.isLoggable(level))
    {
      String[] source = findSourceInStack();
      jdkLogger.logp(level, source[0], source[1], msg);
    }
  }

  private void logInternal(Level level, String msg, Object... arguments)
  {
    if (jdkLogger.isLoggable(level))
    {
      String[] source = findSourceInStack();
      jdkLogger.logp(level, source[0], source[1], refactorString(msg), arguments);
    }
  }

  private void logInternal(Level level, String msg, Throwable t)
  {
    if (jdkLogger.isLoggable(level))
    {
      String[] source = findSourceInStack();
      jdkLogger.logp(level, source[0], source[1], msg, t);
    }
  }

  public static void addHandler(Handler handler)
  {
    Logger snowflakeLogger = Logger.getLogger(SFFormatter.CLASS_NAME_PREFIX);
    snowflakeLogger.addHandler(handler);
  }

  public static void setLevel(Level level)
  {
    Logger snowflakeLogger = Logger.getLogger(SFFormatter.CLASS_NAME_PREFIX);
    snowflakeLogger.setLevel(level);
  }

  /**
   * Since we use SLF4J ways of formatting string we need to refactor message string
   * if we have arguments.
   * For example, in sl4j, this string can be formatted with 2 arguments
   *
   *   ex.1: Error happened in {} on {}
   *
   * And if two arguments are provided, error message can be formatted.
   *
   * However, in java.util.logging, to achieve formatted error message,
   * Same string should be converted to
   *
   *   ex.2: Error happened in {0} on {1}
   *
   * Which represented first arguments and second arguments will be replaced in the
   * corresponding places.
   *
   * This method will convert string in ex.1 to ex.2
   *
   */
  private String refactorString(String original)
  {
    StringBuilder sb = new StringBuilder();
    int argCount = 0;
    for (int i=0; i<original.length(); i++)
    {
      if (original.charAt(i) == '{' && i < original.length()-1 && original.charAt(i+1) == '}')
      {
        sb.append(String.format("{%d}", argCount));
        argCount ++;
        i ++;
      }
      else
      {
        sb.append(original.charAt(i));
      }
    }
    return sb.toString();
  }

  /**
   * Used to find the index of the source class/method in current stack
   * This method will locate the source as the first method after logMethods
   * @return an array of size two, first element is className and second is
   *          methodName
   */
  private String[] findSourceInStack()
  {
    StackTraceElement[] stackTraces = Thread.currentThread().getStackTrace();
    String[] results = new String[2];
    for (int i=0; i<stackTraces.length; i++)
    {
      if (logMethods.contains(stackTraces[i].getMethodName()))
      {
        // since already find the highest logMethods, find the first method after this one
        // and is not a logMethods. This is done to avoid multiple wrapper over log methods
        for (int j=i; j<stackTraces.length; j++)
        {
          if (!logMethods.contains(stackTraces[j].getMethodName()))
          {
            results[0] = stackTraces[j].getClassName();
            results[1] = stackTraces[j].getMethodName();
            return results;
          }
        }
      }
    }
    return results;
  }

  static void defaultInit()
  {
    if (!hasLoggingConfig())
    {
      java.util.logging.Logger sfRootLogger = java.util.logging.Logger
          .getLogger("net.snowflake");
      // disable parent's default console handler
      sfRootLogger.setUseParentHandlers(false);
      // change to use Stream handler print to stdout
      sfRootLogger.addHandler(new StreamHandler(System.out,
          new SFFormatter()));
    }
  }

  /**
   * Check whether a logging.properties exists or not.
   *
   * It will first check where $JRE/lib/logging.properties exist or not,
   * Then check if two system property
   *    - java.util.logging.config.class
   *    - java.util.logging.config.file
   * has been set or not.
   *
   * If yes, return true. If none of the above is set, return false
   */
  private static boolean hasLoggingConfig()
  {
    String JRE = System.getProperty("java.home");
    Path configFilePath = Paths.get(JRE, "lib", "logging.properties");
    if (Files.exists(configFilePath))
    {
      return true;
    }
    else
    {
      String configClass = System.getProperty("java.util.logging.config.class");
      String configFile = System.getProperty("java.util.logging.config.file");

      return (configClass != null) || (configFile != null);
    }
  }
}
