package net.snowflake.client.log;

import java.util.logging.LogRecord;
import java.util.logging.SimpleFormatter;
import java.util.logging.StreamHandler;

class StdOutConsoleHandler extends StreamHandler {
  public StdOutConsoleHandler() {
    // configure with specific defaults for ConsoleHandler
    super(System.out, new SimpleFormatter());
  }

  @Override
  public void publish(LogRecord record) {
    super.publish(record);
    flush();
  }

  @Override
  public void close() {
    flush();
  }
}
