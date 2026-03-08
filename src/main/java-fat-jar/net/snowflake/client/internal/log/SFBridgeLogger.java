package net.snowflake.client.internal.log;

import org.slf4j.helpers.MarkerIgnoringBase;

/**
 * SLF4J {@link org.slf4j.Logger} implementation that bridges to the Snowflake JDBC driver's {@link
 * SFLogger} abstraction.
 *
 * <p>This class is used by the shaded SLF4J inside the fat jar, so that internal dependency logging
 * (AWS SDK v2, Azure SDK, Google Cloud SDK) flows through {@link SFLoggerFactory} and ultimately to
 * JUL (default) or the user's SLF4J binding.
 *
 * <p>Extends {@link MarkerIgnoringBase} to handle all Marker-accepting overloads by delegating to
 * the non-Marker versions.
 */
public class SFBridgeLogger extends MarkerIgnoringBase {

  private static final long serialVersionUID = 1L;

  private final transient SFLogger delegate;

  SFBridgeLogger(String name) {
    this.name = name;
    this.delegate = SFLoggerFactory.getLogger(name);
  }

  // --- Trace ---

  @Override
  public boolean isTraceEnabled() {
    return delegate.isTraceEnabled();
  }

  @Override
  public void trace(String msg) {
    if (delegate.isTraceEnabled()) {
      delegate.trace(msg, false);
    }
  }

  @Override
  public void trace(String format, Object arg) {
    if (delegate.isTraceEnabled()) {
      delegate.trace(format, arg);
    }
  }

  @Override
  public void trace(String format, Object arg1, Object arg2) {
    if (delegate.isTraceEnabled()) {
      delegate.trace(format, arg1, arg2);
    }
  }

  @Override
  public void trace(String format, Object... arguments) {
    if (delegate.isTraceEnabled()) {
      delegate.trace(format, arguments);
    }
  }

  @Override
  public void trace(String msg, Throwable t) {
    if (delegate.isTraceEnabled()) {
      delegate.trace(msg, t);
    }
  }

  // --- Debug ---

  @Override
  public boolean isDebugEnabled() {
    return delegate.isDebugEnabled();
  }

  @Override
  public void debug(String msg) {
    if (delegate.isDebugEnabled()) {
      delegate.debug(msg, false);
    }
  }

  @Override
  public void debug(String format, Object arg) {
    if (delegate.isDebugEnabled()) {
      delegate.debug(format, arg);
    }
  }

  @Override
  public void debug(String format, Object arg1, Object arg2) {
    if (delegate.isDebugEnabled()) {
      delegate.debug(format, arg1, arg2);
    }
  }

  @Override
  public void debug(String format, Object... arguments) {
    if (delegate.isDebugEnabled()) {
      delegate.debug(format, arguments);
    }
  }

  @Override
  public void debug(String msg, Throwable t) {
    if (delegate.isDebugEnabled()) {
      delegate.debug(msg, t);
    }
  }

  // --- Info ---

  @Override
  public boolean isInfoEnabled() {
    return delegate.isInfoEnabled();
  }

  @Override
  public void info(String msg) {
    if (delegate.isInfoEnabled()) {
      delegate.info(msg, false);
    }
  }

  @Override
  public void info(String format, Object arg) {
    if (delegate.isInfoEnabled()) {
      delegate.info(format, arg);
    }
  }

  @Override
  public void info(String format, Object arg1, Object arg2) {
    if (delegate.isInfoEnabled()) {
      delegate.info(format, arg1, arg2);
    }
  }

  @Override
  public void info(String format, Object... arguments) {
    if (delegate.isInfoEnabled()) {
      delegate.info(format, arguments);
    }
  }

  @Override
  public void info(String msg, Throwable t) {
    if (delegate.isInfoEnabled()) {
      delegate.info(msg, t);
    }
  }

  // --- Warn ---

  @Override
  public boolean isWarnEnabled() {
    return delegate.isWarnEnabled();
  }

  @Override
  public void warn(String msg) {
    if (delegate.isWarnEnabled()) {
      delegate.warn(msg, false);
    }
  }

  @Override
  public void warn(String format, Object arg) {
    if (delegate.isWarnEnabled()) {
      delegate.warn(format, arg);
    }
  }

  @Override
  public void warn(String format, Object arg1, Object arg2) {
    if (delegate.isWarnEnabled()) {
      delegate.warn(format, arg1, arg2);
    }
  }

  @Override
  public void warn(String format, Object... arguments) {
    if (delegate.isWarnEnabled()) {
      delegate.warn(format, arguments);
    }
  }

  @Override
  public void warn(String msg, Throwable t) {
    if (delegate.isWarnEnabled()) {
      delegate.warn(msg, t);
    }
  }

  // --- Error ---

  @Override
  public boolean isErrorEnabled() {
    return delegate.isErrorEnabled();
  }

  @Override
  public void error(String msg) {
    if (delegate.isErrorEnabled()) {
      delegate.error(msg, false);
    }
  }

  @Override
  public void error(String format, Object arg) {
    if (delegate.isErrorEnabled()) {
      delegate.error(format, arg);
    }
  }

  @Override
  public void error(String format, Object arg1, Object arg2) {
    if (delegate.isErrorEnabled()) {
      delegate.error(format, arg1, arg2);
    }
  }

  @Override
  public void error(String format, Object... arguments) {
    if (delegate.isErrorEnabled()) {
      delegate.error(format, arguments);
    }
  }

  @Override
  public void error(String msg, Throwable t) {
    if (delegate.isErrorEnabled()) {
      delegate.error(msg, t);
    }
  }
}
