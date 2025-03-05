package net.snowflake.client.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.zip.GZIPInputStream;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class EventHandlerTest {
  @TempDir private File tmpFolder;

  @BeforeEach
  public void setUp() throws IOException {
    new File(tmpFolder, "snowflake_dumps").mkdirs();
    System.setProperty("snowflake.dump_path", tmpFolder.getCanonicalPath());
  }

  @Test
  public void testPublishRecord() {
    LogRecord record = new LogRecord(Level.INFO, "test message");
    EventHandler handler = new EventHandler(10, 5000);
    assertEquals(0, handler.getLogBufferSize());
    handler.publish(record);
    assertEquals(1, handler.getLogBufferSize());
  }

  @Test
  public void testDumpLogBuffer() throws IOException {
    System.setProperty("snowflake.max_dumpfiles", "1");
    System.setProperty("snowflake.max_dumpdir_size_mb", "100");

    LogRecord record = new LogRecord(Level.INFO, "test message");
    EventHandler handler = new EventHandler(10, 5000);
    handler.publish(record);
    handler.flush();

    File logDumpFile = new File(EventUtil.getDumpPathPrefix() + "/sf_log_.dmp.gz");
    GZIPInputStream gzip = new GZIPInputStream(Files.newInputStream(logDumpFile.toPath()));
    StringWriter sWriter = new StringWriter();
    IOUtils.copy(gzip, sWriter, "UTF-8");

    assertTrue(sWriter.toString().contains("test message"));

    gzip.close();
    sWriter.close();
    logDumpFile.delete();
  }

  @Test
  public void testEventFlusher() {
    EventHandler handler = new EventHandler(2, 1000);
    assertEquals(0, handler.getBufferSize());
    handler.triggerBasicEvent(Event.EventType.STATE_TRANSITION, "test event");
    assertEquals(1, handler.getBufferSize());
    handler.triggerBasicEvent(Event.EventType.STATE_TRANSITION, "test event 2");
    // buffer should flush when max entries is reached
    assertEquals(0, handler.getBufferSize());
  }
}
