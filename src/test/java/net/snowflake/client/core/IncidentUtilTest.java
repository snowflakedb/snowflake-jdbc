/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.*;
import java.util.zip.GZIPInputStream;
import org.junit.Test;

public class IncidentUtilTest {

  private static final String FILE_NAME = "snowflake_dumps/sf_incident_123456.dmp.gz";
  private static final String TEMP_DIR_PATH = System.getProperty("java.io.tmpdir");

  @Test
  public void testOneLinerDescription() {
    IOException ex = new IOException("File not found");
    String desc = IncidentUtil.oneLiner("unexpected exception", ex);
    assertEquals("unexpected exception java.io.IOException: File not found", desc.substring(0, 56));
  }

  /** Tests dumping JVM metrics for the current process */
  @Test
  public void testDumpVmMetrics() throws IOException {
    // create the snowflake_dumps directory in the temp folder
    File targetDirectory = new File(TEMP_DIR_PATH + "/snowflake_dumps");
    targetDirectory.mkdir();
    // create the dmp file to write vm metrics to
    File targetVMFile = new File(TEMP_DIR_PATH + "/" + FILE_NAME);
    targetVMFile.createNewFile();
    System.setProperty(EventUtil.DUMP_PATH_PROP, TEMP_DIR_PATH);
    IncidentUtil.dumpVmMetrics("123456");

    // Read back the file
    GZIPInputStream gzip = new GZIPInputStream(new FileInputStream(targetVMFile));
    BufferedReader br = new BufferedReader(new InputStreamReader(gzip));

    if (!br.ready()) {
      fail();
    }

    // cleanup
    targetVMFile.delete();
    targetDirectory.delete();
  }
}
