package net.snowflake.client.log;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.LogRecord;

public class BufferingLogger implements SFLogger {
  private final List<LogRecord> buffer = new ArrayList<>();
  private final String name;

  public BufferingLogger(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public synchronized void flush(SFLogger logger) {
    if (logger instanceof JDK14Logger) {
      JDK14Logger jdkLogger = (JDK14Logger) logger;
      for (LogRecord record : buffer) {
        jdkLogger.log(record);
      }
    } else {
      for (LogRecord record : buffer) {
        logger.info(record.getMessage());
      }
    }
    buffer.clear();
  }

  private void buffer(SFLogLevel level, String msg, Throwable t, Object... arguments) {
    synchronized (buffer) {
      buffer.add(createLogRecord(level, msg, t, arguments));
    }
  }

  private LogRecord createLogRecord(
      SFLogLevel level, String msg, Throwable t, Object... arguments) {
    String[] source = findSourceInStack();
    String message = MessageFormat.format(msg, arguments);
    LogRecord record = new LogRecord(SFToJavaLogMapper.toJavaUtilLoggingLevel(level), message);
    record.setSourceClassName(source[0]);
    record.setSourceMethodName(source[1]);
    record.setThrown(t);
    return record;
  }

  private static String[] findSourceInStack() {
    for (StackTraceElement ste : Thread.currentThread().getStackTrace()) {
      if (!ste.getClassName().equals(BufferingLogger.class.getName())
          && !ste.getClassName().equals(ReconfigurableSFLogger.class.getName())
          && !ste.getClassName().equals(SFLoggerFactory.class.getName())
          && !ste.getClassName().startsWith("java.lang")) {
        return new String[] {ste.getClassName(), ste.getMethodName()};
      }
    }
    return new String[] {"", ""};
  }

  @Override
  public boolean isDebugEnabled() {
    return true;
  }

  @Override
  public boolean isErrorEnabled() {
    return true;
  }

  @Override
  public boolean isInfoEnabled() {
    return true;
  }

  @Override
  public boolean isTraceEnabled() {
    return true;
  }

  @Override
  public boolean isWarnEnabled() {
    return true;
  }

  @Override
  public void debug(String msg, boolean isMasked) {
    buffer(SFLogLevel.DEBUG, msg, null);
  }

  @Override
  public void debugNoMask(String msg) {
    buffer(SFLogLevel.DEBUG, msg, null);
  }

  @Override
  public void debug(String msg, Object... arguments) {
    buffer(SFLogLevel.DEBUG, msg, null, arguments);
  }

  @Override
  public void debug(String msg, Throwable t) {
    buffer(SFLogLevel.DEBUG, msg, t);
  }

  @Override
  public void error(String msg, boolean isMasked) {
    buffer(SFLogLevel.ERROR, msg, null);
  }

  @Override
  public void error(String msg, Object... arguments) {
    buffer(SFLogLevel.ERROR, msg, null, arguments);
  }

  @Override
  public void error(String msg, Throwable t) {
    buffer(SFLogLevel.ERROR, msg, t);
  }

  @Override
  public void info(String msg, boolean isMasked) {
    buffer(SFLogLevel.INFO, msg, null);
  }

  @Override
  public void info(String msg, Object... arguments) {
    buffer(SFLogLevel.INFO, msg, null, arguments);
  }

  @Override
  public void info(String msg, Throwable t) {
    buffer(SFLogLevel.INFO, msg, t);
  }

  @Override
  public void trace(String msg, boolean isMasked) {
    buffer(SFLogLevel.TRACE, msg, null);
  }

  @Override
  public void trace(String msg, Object... arguments) {
    buffer(SFLogLevel.TRACE, msg, null, arguments);
  }

  @Override
  public void trace(String msg, Throwable t) {
    buffer(SFLogLevel.TRACE, msg, t);
  }

  @Override
  public void warn(String msg, boolean isMasked) {
    buffer(SFLogLevel.WARN, msg, null);
  }

  @Override
  public void warn(String msg, Object... arguments) {
    buffer(SFLogLevel.WARN, msg, null, arguments);
  }

  @Override
  public void warn(String msg, Throwable t) {
    buffer(SFLogLevel.WARN, msg, t);
  }
}
