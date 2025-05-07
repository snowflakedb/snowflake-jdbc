package net.snowflake.client.log;

import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.SimpleFormatter;
import java.util.logging.StreamHandler;

class StdErrOutThresholdAwareConsoleHandler extends StreamHandler {
  private final ConsoleHandler stdErrConsoleHandler = new ConsoleHandler();
  private final Level threshold;

  public StdErrOutThresholdAwareConsoleHandler(Level threshold) {
    super(System.out, new SimpleFormatter());
    this.threshold = threshold;
  }

  @Override
  public void publish(LogRecord record) {
    if (record.getLevel().intValue() > threshold.intValue()) {
      stdErrConsoleHandler.publish(record);
    } else {
      super.publish(record);
      flush();
    }
  }

  @Override
  public void close() {
    flush();
    stdErrConsoleHandler.close();
  }

  Level getThreshold() {
    return threshold;
  }
}
