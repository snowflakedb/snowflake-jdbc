package net.snowflake.client.log;

import org.apache.commons.logging.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* Although this class doesn't really include SLF4J, this class play the role of
 * a wrapper class of snowflake SLF4JLogger for apache Jakarta Commons
 * Logging (the logging framework used by apache httpclient4.5 package)
 * to choose to give us the ability to filter out sensitive data.
 *
 * The reason why we don't unify this class and JDK14JCLWrapper class is that the
 * way SLF4J gets the log functions caller prevents us to use an extra wrapper. If
 * we really wrap up SLF4J, the log will not catch correct log callers.
 */
public class SLF4JJCLWrapper implements Log {
  private Logger slf4jLogger;

  public SLF4JJCLWrapper(String name) {
    slf4jLogger = LoggerFactory.getLogger(name);
  }

  Logger getLogger() {
    return slf4jLogger;
  }

  public void debug(Object message) {
    // do nothing
  }

  public void debug(Object msg, Throwable t) {}

  public void error(Object msg) {}

  public void error(Object msg, Throwable t) {}

  public void fatal(Object msg) {}

  public void fatal(Object msg, Throwable t) {}

  public void info(Object msg) {}

  public void info(Object msg, Throwable t) {}

  public void trace(Object msg) {}

  public void trace(Object msg, Throwable t) {}

  public void warn(Object msg) {}

  public void warn(Object msg, Throwable t) {}

  public boolean isDebugEnabled() {
    return false;
  }

  public boolean isErrorEnabled() {
    return this.slf4jLogger.isErrorEnabled();
  }

  public boolean isFatalEnabled() {
    return this.slf4jLogger.isErrorEnabled();
  }

  public boolean isInfoEnabled() {
    return this.slf4jLogger.isInfoEnabled();
  }

  public boolean isTraceEnabled() {
    return false;
  }

  public boolean isWarnEnabled() {
    return this.slf4jLogger.isWarnEnabled();
  }
}
