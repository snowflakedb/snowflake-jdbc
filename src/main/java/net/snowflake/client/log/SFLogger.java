/*
 * Copyright (c) 2012-2017 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.log;

/**
 * Interface used by JDBC driver to log information
 *
 * Five levels are included in this interface, from high to low:
 *   ERROR
 *   WARN
 *   INFO
 *   DEBUG
 *   TRACE
 *
 * Created by hyu on 11/17/16.
 */
public interface SFLogger
{
  /**
   * Is debug level enabled?
   * @return true if the trace level is DEBUG
   */
  boolean isDebugEnabled();

  /**
   * Is error level enabled?
   * @return true if the trace level is ERROR
   */
  boolean isErrorEnabled();

  /**
   * Is info level enabled?
   * @return true if the trace level is INFO
   */
  boolean isInfoEnabled();

  /**
   * Is trace level enabled?
   * @return true if the trace level is TRACE
   */
  boolean isTraceEnabled();

  /**
   * Is warn level enabled?
   * @return true if the trace level is WARN
   */
  boolean isWarnEnabled();

  void debug(String msg);

  void debug(String msg, Object... arguments);

  void debug(String msg, Throwable t);

  void error(String msg);

  void error(String msg, Object... arguments);

  void error(String msg, Throwable t);

  void info(String msg);

  void info(String msg, Object... arguments);

  void info(String msg, Throwable t);

  void trace(String msg);

  void trace(String msg, Object... arguments);

  void trace(String msg, Throwable t);

  void warn(String msg);

  void warn(String msg, Object... arguments);

  void warn(String msg, Throwable t);
}
