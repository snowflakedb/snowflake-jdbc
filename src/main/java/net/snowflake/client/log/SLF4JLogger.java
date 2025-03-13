package net.snowflake.client.log;

import net.snowflake.client.util.SecretDetector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.FormattingTuple;
import org.slf4j.helpers.MessageFormatter;
import org.slf4j.spi.LocationAwareLogger;

public class SLF4JLogger implements SFLogger {
  private Logger slf4jLogger;

  private boolean isLocationAwareLogger;

  private static final String FQCN = SLF4JLogger.class.getName();

  public SLF4JLogger(Class<?> clazz) {
    slf4jLogger = LoggerFactory.getLogger(clazz);
    isLocationAwareLogger = slf4jLogger instanceof LocationAwareLogger;
  }

  public SLF4JLogger(String name) {
    slf4jLogger = LoggerFactory.getLogger(name);
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

  public void debug(String msg, boolean isMasked) {
    msg = isMasked == true ? SecretDetector.maskSecrets(msg) : msg;
    if (isLocationAwareLogger) {
      ((LocationAwareLogger) slf4jLogger)
          .log(null, FQCN, LocationAwareLogger.DEBUG_INT, msg, null, null);
    } else {
      slf4jLogger.debug(msg);
    }
  }

  // This function is used to display unmasked, potentially sensitive log information for internal
  // regression testing purposes. Do not use otherwise
  public void debugNoMask(String msg) {
    if (isLocationAwareLogger) {
      ((LocationAwareLogger) slf4jLogger)
          .log(null, FQCN, LocationAwareLogger.DEBUG_INT, msg, null, null);
    } else {
      slf4jLogger.debug(msg);
    }
  }

  public void debug(String msg, Object... arguments) {
    // use this as format example for JDK14Logger.
    if (isDebugEnabled()) {
      FormattingTuple ft = MessageFormatter.arrayFormat(msg, evaluateLambdaArgs(arguments));
      this.debug(SecretDetector.maskSecrets(ft.getMessage()), false);
    }
  }

  public void debug(String msg, Throwable t) {
    msg = SecretDetector.maskSecrets(msg);
    if (isLocationAwareLogger) {
      ((LocationAwareLogger) slf4jLogger)
          .log(null, FQCN, LocationAwareLogger.DEBUG_INT, msg, null, t);
    } else {
      slf4jLogger.debug(msg, t);
    }
  }

  public void error(String msg, boolean isMasked) {
    msg = isMasked == true ? SecretDetector.maskSecrets(msg) : msg;
    if (isLocationAwareLogger) {
      ((LocationAwareLogger) slf4jLogger)
          .log(null, FQCN, LocationAwareLogger.ERROR_INT, msg, null, null);
    } else {
      slf4jLogger.error(msg);
    }
  }

  public void error(String msg, Object... arguments) {
    if (isErrorEnabled()) {
      FormattingTuple ft = MessageFormatter.arrayFormat(msg, evaluateLambdaArgs(arguments));
      this.error(SecretDetector.maskSecrets(ft.getMessage()), false);
    }
  }

  public void error(String msg, Throwable t) {
    msg = SecretDetector.maskSecrets(msg);
    if (isLocationAwareLogger) {
      ((LocationAwareLogger) slf4jLogger)
          .log(null, FQCN, LocationAwareLogger.ERROR_INT, msg, null, t);
    } else {
      slf4jLogger.error(msg, t);
    }
  }

  public void info(String msg, boolean isMasked) {
    msg = isMasked == true ? SecretDetector.maskSecrets(msg) : msg;
    if (isLocationAwareLogger) {
      ((LocationAwareLogger) slf4jLogger)
          .log(null, FQCN, LocationAwareLogger.INFO_INT, msg, null, null);
    } else {
      slf4jLogger.info(msg);
    }
  }

  public void info(String msg, Object... arguments) {
    if (isInfoEnabled()) {
      FormattingTuple ft = MessageFormatter.arrayFormat(msg, evaluateLambdaArgs(arguments));
      this.info(SecretDetector.maskSecrets(ft.getMessage()), false);
    }
  }

  public void info(String msg, Throwable t) {
    msg = SecretDetector.maskSecrets(msg);
    if (isLocationAwareLogger) {
      ((LocationAwareLogger) slf4jLogger)
          .log(null, FQCN, LocationAwareLogger.INFO_INT, msg, null, t);
    } else {
      slf4jLogger.error(msg, t);
    }
  }

  public void trace(String msg, boolean isMasked) {
    msg = isMasked == true ? SecretDetector.maskSecrets(msg) : msg;
    if (isLocationAwareLogger) {
      ((LocationAwareLogger) slf4jLogger)
          .log(null, FQCN, LocationAwareLogger.TRACE_INT, msg, null, null);
    } else {
      slf4jLogger.trace(msg);
    }
  }

  public void trace(String msg, Object... arguments) {
    if (isTraceEnabled()) {
      FormattingTuple ft = MessageFormatter.arrayFormat(msg, evaluateLambdaArgs(arguments));
      this.trace(SecretDetector.maskSecrets(ft.getMessage()), false);
    }
  }

  public void trace(String msg, Throwable t) {
    msg = SecretDetector.maskSecrets(msg);
    if (isLocationAwareLogger) {
      ((LocationAwareLogger) slf4jLogger)
          .log(null, FQCN, LocationAwareLogger.TRACE_INT, msg, null, t);
    } else {
      slf4jLogger.trace(msg, t);
    }
  }

  public void warn(String msg, boolean isMasked) {
    msg = isMasked == true ? SecretDetector.maskSecrets(msg) : msg;
    if (isLocationAwareLogger) {
      ((LocationAwareLogger) slf4jLogger)
          .log(null, FQCN, LocationAwareLogger.WARN_INT, msg, null, null);
    } else {
      slf4jLogger.error(msg);
    }
  }

  public void warn(String msg, Object... arguments) {
    if (isWarnEnabled()) {
      FormattingTuple ft = MessageFormatter.arrayFormat(msg, evaluateLambdaArgs(arguments));
      this.warn(SecretDetector.maskSecrets(ft.getMessage()), false);
    }
  }

  public void warn(String msg, Throwable t) {
    msg = SecretDetector.maskSecrets(msg);
    if (isLocationAwareLogger) {
      ((LocationAwareLogger) slf4jLogger)
          .log(null, FQCN, LocationAwareLogger.WARN_INT, msg, null, t);
    } else {
      slf4jLogger.error(msg, t);
    }
  }

  private static Object[] evaluateLambdaArgs(Object... args) {
    final Object[] result = new Object[args.length];

    for (int i = 0; i < args.length; i++) {
      result[i] = args[i] instanceof ArgSupplier ? ((ArgSupplier) args[i]).get() : args[i];
    }

    return result;
  }
}
