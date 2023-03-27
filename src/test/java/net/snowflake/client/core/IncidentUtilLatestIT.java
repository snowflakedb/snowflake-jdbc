/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.util.zip.GZIPInputStream;
import net.snowflake.client.category.TestCategoryCore;
import net.snowflake.client.jdbc.BaseJDBCTest;
import org.apache.commons.io.IOUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

@Category(TestCategoryCore.class)
public class IncidentUtilLatestIT extends BaseJDBCTest {
  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();
  private static final String FILE_NAME = "sf_incident_123456.dmp.gz";

  @Test
  public void testOneLinerDescription() {
    String desc = IncidentUtil.oneLiner("unexpected exception", new IOException("File not found"));
    assertEquals("unexpected exception java.io.IOException: File not found", desc.substring(0, 56));
  }

  /** Tests dumping JVM metrics for the current process */
  @Test
  public void testDumpVmMetrics() throws IOException {
    File targetVMFile = new File(getFullPathFileInResource("snowflake_dumps/" + FILE_NAME));
    int index = targetVMFile.getPath().indexOf("snowflake_dumps");
    String dumpPath = targetVMFile.getPath().substring(0, index - 1);
    System.setProperty("snowflake.dump_path", dumpPath);

    // write the VM metrics to the dump file
    IncidentUtil.dumpVmMetrics("123456");

    // Read back the file contents
    GZIPInputStream gzip = new GZIPInputStream(new FileInputStream(targetVMFile));
    StringWriter sWriter = new StringWriter();
    IOUtils.copy(gzip, sWriter, "UTF-8");
    String output = sWriter.toString();
    assertEquals(
        "\n\n\n---------------------------  METRICS " + "---------------------------\n\n",
        output.substring(0, 69));
    sWriter.close();
  }
}
