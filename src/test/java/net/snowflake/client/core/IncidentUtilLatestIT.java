package net.snowflake.client.core;

import static net.snowflake.client.core.IncidentUtil.INC_DUMP_FILE_EXT;
import static net.snowflake.client.core.IncidentUtil.INC_DUMP_FILE_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.util.zip.GZIPInputStream;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.jdbc.BaseJDBCTest;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@Tag(TestTags.CORE)
public class IncidentUtilLatestIT extends BaseJDBCTest {
  @TempDir private File tmpFolder;
  private static final String FILE_NAME = "sf_incident_123456.dmp.gz";

  @Test
  public void testOneLinerDescription() {
    String desc = IncidentUtil.oneLiner("unexpected exception", new IOException("File not found"));
    assertEquals("unexpected exception java.io.IOException: File not found", desc.substring(0, 56));
  }

  /** Tests dumping JVM metrics for the current process */
  @Test
  public void testDumpVmMetrics() throws IOException {
    File dumpDir = new File(tmpFolder, "dump");
    dumpDir.mkdirs();
    String dumpPath = dumpDir.getCanonicalPath();
    System.setProperty("snowflake.dump_path", dumpPath);

    String incidentId = "123456";

    // write the VM metrics to the dump file
    IncidentUtil.dumpVmMetrics(incidentId);

    // Get location of where file will be written
    String targetVMFileLocation =
        EventUtil.getDumpPathPrefix() + "/" + INC_DUMP_FILE_NAME + incidentId + INC_DUMP_FILE_EXT;

    // Read back the file contents
    try (FileInputStream fis = new FileInputStream(targetVMFileLocation);
        GZIPInputStream gzip = new GZIPInputStream(fis)) {
      StringWriter sWriter = new StringWriter();
      IOUtils.copy(gzip, sWriter, "UTF-8");
      String output = sWriter.toString();
      assertEquals(
          "\n\n\n---------------------------  METRICS " + "---------------------------\n\n",
          output.substring(0, 69));
      sWriter.close();
    }
  }
}
