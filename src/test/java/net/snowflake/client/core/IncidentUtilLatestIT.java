/*
 * Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.*;
import java.util.zip.GZIPInputStream;
import net.snowflake.client.ConditionalIgnoreRule;
import net.snowflake.client.RunningOnGithubAction;
import net.snowflake.client.category.TestCategoryCore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

@Category(TestCategoryCore.class)
public class IncidentUtilLatestIT {
  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();
  private static final String FILE_NAME = "sf_incident_123456.dmp.gz";

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testOneLinerDescription() {
    String desc = IncidentUtil.oneLiner("unexpected exception", new IOException("File not found"));
    assertEquals("unexpected exception java.io.IOException: File not found", desc.substring(0, 56));
  }

  /** Tests dumping JVM metrics for the current process */
  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testDumpVmMetrics() throws IOException {
    // create the snowflake_dumps directory in the temp folder
    File destFolder = tmpFolder.newFolder("snowflake_dumps");
    String destFolderCanonicalPath = destFolder.getCanonicalPath();
    String destFolderCanonicalPathWithSeparator = destFolderCanonicalPath + File.separator;

    // create the dmp file to write vm metrics to
    File targetVMFile = new File(destFolderCanonicalPathWithSeparator + FILE_NAME);
    targetVMFile.createNewFile();

    System.setProperty(EventUtil.DUMP_PATH_PROP, tmpFolder.getRoot().getCanonicalPath());
    IncidentUtil.dumpVmMetrics("123456");

    // Read back the file
    GZIPInputStream gzip = new GZIPInputStream(new FileInputStream(targetVMFile));
    BufferedReader br = new BufferedReader(new InputStreamReader(gzip));

    if (!br.ready()) {
      fail();
    }
  }
}
