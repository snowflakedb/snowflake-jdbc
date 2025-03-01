package net.snowflake.client.log;

import org.apache.commons.logging.Log;

/* This is a wrapper class of snowflake JDK14Logger for apache Jakarta Commons Logging
 * (the logging framework used by apache httpclient4.5 package) to choose to give us
 * the ability to filter out sensitive data.
 */
public class JDK14JCLWrapper implements Log {
  private final SFLogger logger;

  public JDK14JCLWrapper(String className) {
    this.logger = new JDK14Logger(className);
  }

  SFLogger getLogger() {
    return logger;
  }

  public void debug(Object msg) {
    // do nothing
  }

  public void debug(Object msg, Throwable t) {
    // do nothing
  }

  public void error(Object msg) {
    logger.error(String.valueOf(msg), true);
  }

  public void error(Object msg, Throwable t) {
    logger.error(String.valueOf(msg), t);
  }

  public void fatal(Object msg) {
    this.error(msg);
  }

  public void fatal(Object msg, Throwable t) {
    this.error(msg, t);
  }

  public void info(Object msg) {
    logger.info(String.valueOf(msg), true);
  }

  public void info(Object msg, Throwable t) {
    logger.info(String.valueOf(msg), t);
  }

  public boolean isDebugEnabled() {
    return false;
  }

  public boolean isErrorEnabled() {
    return logger.isErrorEnabled();
  }

  public boolean isFatalEnabled() {
    return logger.isErrorEnabled();
  }

  public boolean isInfoEnabled() {
    return logger.isInfoEnabled();
  }

  public boolean isTraceEnabled() {
    return false;
  }

  public boolean isWarnEnabled() {
    return logger.isWarnEnabled();
  }

  public void trace(Object msg) {
    // do nothing
  }

  public void trace(Object msg, Throwable t) {
    // do nothing
  }

  public void warn(Object msg) {
    logger.warn(String.valueOf(msg));
  }

  public void warn(Object msg, Throwable t) {
    logger.warn(String.valueOf(msg), t);
  }
}
