package net.snowflake.client.log;

import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import org.apache.commons.logging.Log;

/**
 * This is a wrapper class of apache commons logging which uses SFLogger to use driver configuration
 * (via java.util.logging or SLF4J) and mask secrets. Wrapper does not hide trace and debug
 * messages.
 */
@SnowflakeJdbcInternalApi
public class CommonsLoggingWrapper implements Log {
  private final SFLogger logger;

  public CommonsLoggingWrapper(String className) {
    this.logger = SFLoggerFactory.getLogger(className);
  }

  public void debug(Object msg) {
    logger.debug(String.valueOf(msg), true);
  }

  public void debug(Object msg, Throwable t) {
    logger.debug(String.valueOf(msg), t);
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
    return logger.isDebugEnabled();
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
    return logger.isTraceEnabled();
  }

  public boolean isWarnEnabled() {
    return logger.isWarnEnabled();
  }

  public void trace(Object msg) {
    logger.debug(String.valueOf(msg), true);
  }

  public void trace(Object msg, Throwable t) {
    logger.trace(String.valueOf(msg), t);
  }

  public void warn(Object msg) {
    logger.warn(String.valueOf(msg));
  }

  public void warn(Object msg, Throwable t) {
    logger.warn(String.valueOf(msg), t);
  }
}
