/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.zip.GZIPInputStream;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class EventHandlerTest {
  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Before
  public void setUp() throws IOException {
    tmpFolder.newFolder("snowflake_dumps");
    System.setProperty("snowflake.dump_path", tmpFolder.getRoot().getCanonicalPath());
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
    handler.startFlusher();
    handler.triggerBasicEvent(Event.EventType.STATE_TRANSITION, "test event");
    assertEquals(handler.getBufferSize(), 1);
    handler.triggerBasicEvent(Event.EventType.STATE_TRANSITION, "test event 2");
    // buffer should flush when max entries is reached
    assertEquals(handler.getBufferSize(), 0);
    handler.stopFlusher();
  }
}
