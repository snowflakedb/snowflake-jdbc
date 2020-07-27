/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.log;

import net.snowflake.client.util.SecretDetector;
import org.apache.commons.logging.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.spi.LocationAwareLogger;

/* Although this class doesn't really include SLF4J, this class play the role of
 * a wrapper class of snowflake SLF4JLogger for apache Jakarta Commons
 * Logging (the logging framework used by apache httpclient4.5 package)
 * to choose to give us the ability to filter out sensitive data.
 *
 * The reason why we don't unify this class and JDK14JCLWrapper class is that the
 * way SLF4J gets the log functions caller prevents us to use an exntra wrapper. If
 * we really wrap up SLF4J, the log will not catch correct log callers.
 */
public class SLF4JJCLWrapper implements Log {
  private Logger slf4jLogger;

  private boolean isLocationAwareLogger;

  private static final String FQCN = SLF4JJCLWrapper.class.getName();

  public SLF4JJCLWrapper(String name) {
    slf4jLogger = LoggerFactory.getLogger(name);
    isLocationAwareLogger = slf4jLogger instanceof LocationAwareLogger;
  }

  public void debug(Object message) {
    // In this special use case, only this function is used by apache httpclient to output sensitive
    // data in our use case
    String msg = SecretDetector.maskSecrets(String.valueOf(message));
    if (isLocationAwareLogger) {
      ((LocationAwareLogger) slf4jLogger)
          .log(null, FQCN, LocationAwareLogger.DEBUG_INT, msg, null, null);
    } else {
      slf4jLogger.debug(msg);
    }
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
    return this.slf4jLogger.isDebugEnabled();
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
    return this.slf4jLogger.isTraceEnabled();
  }

  public boolean isWarnEnabled() {
    return this.slf4jLogger.isWarnEnabled();
  }
}
