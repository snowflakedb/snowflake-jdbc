package net.snowflake.client.core;

import static net.snowflake.client.core.EventUtil.DUMP_PATH_PROP;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;
import java.util.zip.GZIPInputStream;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class EventTest {
  @TempDir private File tmpFolder;
  private File homeDirectory;
  private File dmpDirectory;

  @BeforeEach
  public void setUp() throws IOException {
    homeDirectory = new File(tmpFolder, "homedir");
    homeDirectory.mkdirs();
    dmpDirectory = new File(homeDirectory, "snowflake_dumps");
    dmpDirectory.mkdirs();
  }

  @AfterEach
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
      assertEquals(dmpPath2, dmpPath1, "dump path is: " + EventUtil.getDumpPathPrefix());
      File dumpFile =
          new File(
              EventUtil.getDumpPathPrefix()
                  + File.separator
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
      EventUtil.setDumpPathPrefixForTesting(EventUtil.getDumpPathPrefix());
    }
  }
}
