/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.log;

import org.apache.commons.logging.Log;

/* This is a wrapper class of snowflake JDK14Logger for apache Jakarta Commons Logging
 * (the logging framework used by apache httpclient4.5 package) to choose to give us
 * the ability to filter out sensitive data.
 */
public class JDK14JCLWrapper implements Log
{
  private final SFLogger logger;

  public JDK14JCLWrapper(String className)
  {
    this.logger = new JDK14Logger(className);
  }

  public void debug(Object msg)
  {
    logger.debug(String.valueOf(msg));
  }

  public void debug(Object msg, Throwable t)
  {
    logger.debug(String.valueOf(msg), t);
  }

  public void error(Object msg)
  {
    logger.error(String.valueOf(msg));
  }

  public void error(Object msg, Throwable t)
  {
    logger.error(String.valueOf(msg), t);
  }

  public void fatal(Object msg)
  {
    this.error(msg);
  }

  public void fatal(Object msg, Throwable t)
  {
    this.error(msg, t);
  }

  public void info(Object msg)
  {
    logger.info(String.valueOf(msg));
  }

  public void info(Object msg, Throwable t)
  {
    logger.info(String.valueOf(msg), t);
  }

  public boolean isDebugEnabled()
  {
    return logger.isDebugEnabled();
  }

  public boolean isErrorEnabled()
  {
    return logger.isErrorEnabled();
  }

  public boolean isFatalEnabled()
  {
    return this.isErrorEnabled();
  }

  public boolean isInfoEnabled()
  {
    return logger.isInfoEnabled();
  }

  public boolean isTraceEnabled()
  {
    return logger.isTraceEnabled();
  }

  public boolean isWarnEnabled()
  {
    return logger.isWarnEnabled();
  }

  public void trace(Object msg)
  {
    logger.trace(String.valueOf(msg));
  }

  public void trace(Object msg, Throwable t)
  {
    logger.trace(String.valueOf(msg), t);
  }

  public void warn(Object msg)
  {
    logger.warn(String.valueOf(msg));
  }

  public void warn(Object msg, Throwable t)
  {
    logger.warn(String.valueOf(msg), t);
  }
}
