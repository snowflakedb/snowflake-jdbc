/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.FormattingTuple;
import org.slf4j.helpers.MessageFormatter;
import org.slf4j.spi.LocationAwareLogger;

/**
 * Created by hyu on 11/17/16.
 */
public class SLF4JLogger implements SFLogger
{
  private Logger slf4jLogger;

  private boolean isLocationAwareLogger;

  private static final String FQCN = SLF4JLogger.class.getName();

  public SLF4JLogger(Class<?> clazz) {
    slf4jLogger = LoggerFactory.getLogger(clazz);
    isLocationAwareLogger = slf4jLogger instanceof LocationAwareLogger;
  }

  public boolean isDebugEnabled() {
    return this.slf4jLogger.isDebugEnabled();
  }

  public boolean isErrorEnabled() {
    return this.slf4jLogger.isErrorEnabled();
  }

  public boolean isInfoEnabled() {
    return this.slf4jLogger.isInfoEnabled();
  }

  public boolean isTraceEnabled() {
    return this.slf4jLogger.isTraceEnabled();
  }

  public boolean isWarnEnabled() {
    return this.slf4jLogger.isWarnEnabled();
  }

  public void debug(String msg)
  {
    if (isLocationAwareLogger)
    {
      ((LocationAwareLogger) slf4jLogger).log(null, FQCN, LocationAwareLogger.DEBUG_INT, msg, null, null);
    }
    else
    {
      slf4jLogger.debug(msg);
    }
  }

  public void debug(String msg, Object... arguments)
  {
    if (isDebugEnabled())
    {
      FormattingTuple ft = MessageFormatter.arrayFormat(msg, arguments);
      this.debug(ft.getMessage());
    }
  }

  public void debug(String msg, Throwable t)
  {
    if (isLocationAwareLogger)
    {
      ((LocationAwareLogger) slf4jLogger).log(null, FQCN, LocationAwareLogger.DEBUG_INT, msg, null, t);
    }
    else
    {
      slf4jLogger.debug(msg, t);
    }
  }

  public void error(String msg)
  {
    if (isLocationAwareLogger)
    {
      ((LocationAwareLogger) slf4jLogger).log(null, FQCN, LocationAwareLogger.ERROR_INT, msg, null, null);
    }
    else
    {
      slf4jLogger.error(msg);
    }
  }

  public void error(String msg, Object... arguments)
  {
    if (isErrorEnabled())
    {
      FormattingTuple ft = MessageFormatter.arrayFormat(msg, arguments);
      this.error(ft.getMessage());
    }
  }

  public void error(String msg, Throwable t)
  {
    if (isLocationAwareLogger)
    {
      ((LocationAwareLogger) slf4jLogger).log(null, FQCN, LocationAwareLogger.ERROR_INT, msg, null, t);
    }
    else
    {
      slf4jLogger.error(msg, t);
    }
  }

  public void info(String msg)
  {
    if (isLocationAwareLogger)
    {
      ((LocationAwareLogger) slf4jLogger).log(null, FQCN, LocationAwareLogger.INFO_INT, msg, null, null);
    }
    else
    {
      slf4jLogger.info(msg);
    }
  }

  public void info(String msg, Object... arguments)
  {
    if (isInfoEnabled())
    {
      FormattingTuple ft = MessageFormatter.arrayFormat(msg, arguments);
      this.info(ft.getMessage());
    }
  }

  public void info(String msg, Throwable t)
  {
    if (isLocationAwareLogger)
    {
      ((LocationAwareLogger) slf4jLogger).log(null, FQCN, LocationAwareLogger.INFO_INT, msg, null, t);
    }
    else
    {
      slf4jLogger.error(msg, t);
    }
  }

  public void trace(String msg)
  {
    if (isLocationAwareLogger)
    {
      ((LocationAwareLogger) slf4jLogger).log(null, FQCN, LocationAwareLogger.TRACE_INT, msg, null, null);
    }
    else
    {
      slf4jLogger.trace(msg);
    }
  }

  public void trace(String msg, Object... arguments)
  {
    if (isTraceEnabled())
    {
      FormattingTuple ft = MessageFormatter.arrayFormat(msg, arguments);
      this.trace(ft.getMessage());
    }
  }

  public void trace(String msg, Throwable t)
  {
    if (isLocationAwareLogger)
    {
      ((LocationAwareLogger) slf4jLogger).log(null, FQCN, LocationAwareLogger.TRACE_INT, msg, null, t);
    }
    else
    {
      slf4jLogger.trace(msg, t);
    }
  }

  public void warn(String msg)
  {
    if (isLocationAwareLogger)
    {
      ((LocationAwareLogger) slf4jLogger).log(null, FQCN, LocationAwareLogger.WARN_INT, msg, null, null);
    }
    else
    {
      slf4jLogger.error(msg);
    }
  }

  public void warn(String msg, Object... arguments)
  {
    if (isWarnEnabled())
    {
      FormattingTuple ft = MessageFormatter.arrayFormat(msg, arguments);
      this.warn(ft.getMessage());
    }
  }

  public void warn(String msg, Throwable t)
  {
    if (isLocationAwareLogger)
    {
      ((LocationAwareLogger) slf4jLogger).log(null, FQCN, LocationAwareLogger.WARN_INT, msg, null, t);
    }
    else
    {
      slf4jLogger.error(msg, t);
    }
  }
}
