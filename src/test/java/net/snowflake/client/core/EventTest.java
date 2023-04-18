/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import static net.snowflake.client.core.EventUtil.DUMP_PATH_PROP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;
import java.util.zip.GZIPInputStream;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class EventTest {
  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();
  private File homeDirectory;
  private File dmpDirectory;

  @Before
  public void setUp() throws IOException {
    homeDirectory = tmpFolder.newFolder("homedir");
    dmpDirectory = tmpFolder.newFolder("homedir", "snowflake_dumps");
  }

  @After
  public void tearDown() {
    dmpDirectory.delete();
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
    try {
      // Set dmp file path
      String dumpPath = homeDirectory.getCanonicalPath();
      System.setProperty(DUMP_PATH_PROP, dumpPath);
      EventUtil.setDumpPathPrefixForTesting(dumpPath);
      Event event = new BasicEvent(Event.EventType.NETWORK_ERROR, "network timeout");
      event.writeEventDumpLine("network timeout after 60 seconds");
      // Assert the dump path prefix function correctly leads to the temporary dump directory
      // created
      String dmpPath1 = EventUtil.getDumpPathPrefix();
      String dmpPath2 = dmpDirectory.getCanonicalPath();
      assertEquals("dump path is: " + EventUtil.getDumpPathPrefix(), dmpPath2, dmpPath1);
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
    } finally {
      System.clearProperty("snowflake.dump_path");
    }
  }
}
