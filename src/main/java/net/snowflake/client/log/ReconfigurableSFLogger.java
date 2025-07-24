package net.snowflake.client.log;

import java.util.concurrent.atomic.AtomicReference;

public class ReconfigurableSFLogger implements SFLogger {
  private final AtomicReference<SFLogger> logger;
  private final String name;

  public ReconfigurableSFLogger(String name) {
    this.name = name;
    this.logger = new AtomicReference<>(new BufferingLogger(name));
  }

  public String getName() {
    return name;
  }

  public void reconfigure(SFLogger newLogger) {
    SFLogger oldLogger = this.logger.getAndSet(newLogger);
    if (oldLogger instanceof BufferingLogger) {
      ((BufferingLogger) oldLogger).flush(newLogger);
    }
  }

  @Override
  public boolean isDebugEnabled() {
    return logger.get().isDebugEnabled();
  }

  @Override
  public boolean isErrorEnabled() {
    return logger.get().isErrorEnabled();
  }

  @Override
  public boolean isInfoEnabled() {
    return logger.get().isInfoEnabled();
  }

  @Override
  public boolean isTraceEnabled() {
    return logger.get().isTraceEnabled();
  }

  @Override
  public boolean isWarnEnabled() {
    return logger.get().isWarnEnabled();
  }

  @Override
  public void debug(String msg, boolean isMasked) {
    logger.get().debug(msg, isMasked);
  }

  @Override
  public void debugNoMask(String msg) {
    logger.get().debugNoMask(msg);
  }

  @Override
  public void debug(String msg, Object... arguments) {
    logger.get().debug(msg, arguments);
  }

  @Override
  public void debug(String msg, Throwable t) {
    logger.get().debug(msg, t);
  }

  @Override
  public void error(String msg, boolean isMasked) {
    logger.get().error(msg, isMasked);
  }

  @Override
  public void error(String msg, Object... arguments) {
    logger.get().error(msg, arguments);
  }

  @Override
  public void error(String msg, Throwable t) {
    logger.get().error(msg, t);
  }

  @Override
  public void info(String msg, boolean isMasked) {
    logger.get().info(msg, isMasked);
  }

  @Override
  public void info(String msg, Object... arguments) {
    logger.get().info(msg, arguments);
  }

  @Override
  public void info(String msg, Throwable t) {
    logger.get().info(msg, t);
  }

  @Override
  public void trace(String msg, boolean isMasked) {
    logger.get().trace(msg, isMasked);
  }

  @Override
  public void trace(String msg, Object... arguments) {
    logger.get().trace(msg, arguments);
  }

  @Override
  public void trace(String msg, Throwable t) {
    logger.get().trace(msg, t);
  }

  @Override
  public void warn(String msg, boolean isMasked) {
    logger.get().warn(msg, isMasked);
  }

  @Override
  public void warn(String msg, Object... arguments) {
    logger.get().warn(msg, arguments);
  }

  @Override
  public void warn(String msg, Throwable t) {
    logger.get().warn(msg, t);
  }
}
