/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.*;
import java.nio.file.Files;
import java.util.zip.GZIPInputStream;
import net.snowflake.client.jdbc.BaseJDBCTest;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class EventTest extends BaseJDBCTest {
  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Before
  public void setUp() throws IOException {
    tmpFolder.newFolder("snowflake_dumps");
    System.setProperty("snowflake.dump_path", tmpFolder.getRoot().getCanonicalPath());
  }

  @Test
  public void testEvent() {
    Event event = new BasicEvent(Event.EventType.NONE, "basic event");
    event.setType(Event.EventType.NETWORK_ERROR);
    event.setMessage("network timeout");
    assertEquals(1, event.getType().getId());
    assertEquals("network timeout", event.getMessage());
  }

  @Test
  public void testWriteEventDumpLine() throws IOException {
    Event event = new BasicEvent(Event.EventType.NETWORK_ERROR, "network timeout");
    event.writeEventDumpLine("network timeout after 60 seconds");

    File dumpFile =
        new File(
            EventUtil.getDumpPathPrefix()
                + "/"
                + "sf_event_"
                + EventUtil.getDumpFileId()
                + ".dmp.gz");
    GZIPInputStream gzip = new GZIPInputStream(Files.newInputStream(dumpFile.toPath()));
    StringWriter sWriter = new StringWriter();
    IOUtils.copy(gzip, sWriter, "UTF-8");

    assertTrue(sWriter.toString().contains("network timeout after 60 seconds"));

    gzip.close();
    sWriter.close();
    dumpFile.delete();
  }
}
