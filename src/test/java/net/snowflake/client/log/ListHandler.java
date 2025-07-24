package net.snowflake.client.log;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.LogRecord;

public class ListHandler extends Handler {
  public final List<LogRecord> records = new ArrayList<>();

  @Override
  public void publish(LogRecord record) {
    records.add(record);
  }

  @Override
  public void flush() {}

  @Override
  public void close() throws SecurityException {}
}
